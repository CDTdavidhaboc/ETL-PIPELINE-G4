"""
Paintelligent ETL - Stage 3: Data Load Stage

This script connects to the Transformation Stage handover outputs in
`transformation/output/` and the transformation run metadata in
`logs/transformation.log`, performs Pre-Load Validation Checks, loads the
validated datasets into the target SQLite data warehouse (`load/paintelligent.db`)
inside atomic transactions, runs Post-Load Validation and Reconciliation
checks, and writes structured operational/audit logs to `logs/load.log` and
`load_log` in SQLite.

Requirement Compliance (Stage 3 Spec):
    - Uses ONLY the data handed over from the Transformation Stage.
    - Does NOT re-extract or re-transform data in this stage.
    - Batch-only execution with full transaction rollback and idempotency.
"""

from __future__ import annotations

import sys
sys.dont_write_bytecode = True

import csv
import hashlib
import json
import logging
import re
import shutil
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# PATHS & CONSTANTS
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent

if SCRIPT_DIR.name.lower() == "csv" and SCRIPT_DIR.parent.name.lower() == "pipeline":
    PROJECT_ROOT = SCRIPT_DIR.parent.parent
else:
    PROJECT_ROOT = SCRIPT_DIR.parent

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

TRANSFORMATION_OUTPUT_DIR = PROJECT_ROOT / "transformation" / "output"
TRANSFORMATION_LOG_FILE = LOG_DIR / "transformation.log"
HANDOVER_METADATA_FILE = TRANSFORMATION_OUTPUT_DIR / "handover_metadata.json"

LOAD_DIR = PROJECT_ROOT / "load"
LOAD_DIR.mkdir(parents=True, exist_ok=True)

BACKUP_DIR = LOAD_DIR / "backups"
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

REJECTED_DIR = LOAD_DIR / "rejected"
REJECTED_DIR.mkdir(parents=True, exist_ok=True)

TARGET_DB_PATH = LOAD_DIR / "paintelligent.db"
TARGET_SQL_DUMP_PATH = LOAD_DIR / "paintelligent_readable.sql"
LOAD_LOG_FILE = LOG_DIR / "load.log"
LOAD_REJECTED_FILE = REJECTED_DIR / "load_rejected.csv"
LOAD_SUMMARY_JSON = LOAD_DIR / "load_metadata.json"

LOAD_CODE_VERSION = "v1.4.0"
OPERATIONAL_LOG_RETENTION_DAYS = 30
AUDIT_LOG_RETENTION_DAYS = 365
MIN_FREE_DISK_BYTES = 50 * 1024 * 1024  # 50 MB minimum required storage

EXPECTED_SCHEMAS = {
    "product_table": [
        "product_id",
        "product_name",
        "unit_purchase_price",
        "estimated_price_php",
        "stocks",
        "category",
        "brand",
    ],
    "sales_table": [
        "sales_id",
        "brand",
        "product",
        "category",
        "sales_date",
        "quantity",
        "unit_sold",
        "season",
    ],
    "paint_analysis_table": [
        "id",
        "color",
        "rgb",
        "paint_components",
        "analysis_date",
    ],
}

INPUT_FILES = {
    "product_table": TRANSFORMATION_OUTPUT_DIR / "products_transformed.csv",
    "sales_table": TRANSFORMATION_OUTPUT_DIR / "sales_transformed.csv",
    "paint_analysis_table": TRANSFORMATION_OUTPUT_DIR / "paint_analysis_transformed.csv",
}

REJECTED_COLUMNS = [
    "run_id",
    "batch_id",
    "table_name",
    "source_row_number",
    "original_record",
    "error_code",
    "rejection_reason",
    "rejected_timestamp",
]


# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------

logger = logging.getLogger("paintelligent.load")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.propagate = False

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

file_handler = logging.FileHandler(LOAD_LOG_FILE, mode="a", encoding="utf-8")
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.__stdout__)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


def now_str() -> str:
    """Return local timestamp formatted as YYYY-MM-DD HH:MM:SS."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def utc_iso() -> str:
    """Return ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def make_load_run_id() -> str:
    """Generate a unique pipeline load run ID."""
    return f"LOAD-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"


# ---------------------------------------------------------------------------
# HANDOVER METADATA PARSER (CONNECTS TO TRANSFORMATION STAGE)
# ---------------------------------------------------------------------------

def read_transformation_handover() -> dict[str, Any]:
    """Read the latest transformation run handover metadata from logs or JSON."""
    if HANDOVER_METADATA_FILE.exists():
        try:
            with open(HANDOVER_METADATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data.get("run_id") and data.get("batch_id"):
                    if "expected_counts" not in data and "dataset_metrics" in data:
                        dm = data["dataset_metrics"]
                        data["expected_counts"] = {
                            "product_table": dm.get("products", {}).get("records_output", 0),
                            "sales_table": dm.get("sales", {}).get("records_output", 0),
                            "paint_analysis_table": dm.get("paint_analysis", {}).get("records_output", 0),
                        }
                    return data
        except Exception as exc:
            logger.warning("Could not read handover_metadata.json: %s", exc)

    handover: dict[str, Any] = {
        "run_id": "UNKNOWN",
        "batch_id": "UNKNOWN",
        "start_timestamp": "UNKNOWN",
        "end_timestamp": "UNKNOWN",
        "status": "UNKNOWN",
        "batch_window": "Full available source data",
        "expected_counts": {},
        "validation_results": {},
        "rejection_rate": 0.0,
    }

    if not TRANSFORMATION_LOG_FILE.exists():
        return handover

    text = TRANSFORMATION_LOG_FILE.read_text(encoding="utf-8", errors="replace")
    blocks = text.split("PAINTELLIGENT TRANSFORMATION REPORT")
    if len(blocks) < 2:
        return handover

    latest_block = blocks[-1]

    run_match = re.search(r"Run ID:\s*(\S+)", latest_block)
    batch_match = re.search(r"Batch ID:\s*(\S+)", latest_block)
    start_match = re.search(r"Start Timestamp:\s*(\S+)", latest_block)
    status_match = re.search(r"Transformation Status:\s*(\S+)", latest_block)
    window_match = re.search(r"Batch window:\s*(.+)", latest_block)
    finish_match = re.search(r"Finished:\s*(\S+)", latest_block)
    val_match = re.search(r"Validation Results:\s*(\{.*\})", latest_block)
    rate_match = re.search(r"Rejection Rate:\s*([\d.]+)%", latest_block)

    if run_match:
        handover["run_id"] = run_match.group(1).strip()
    if batch_match:
        handover["batch_id"] = batch_match.group(1).strip()
    if start_match:
        handover["start_timestamp"] = start_match.group(1).strip()
    if status_match:
        handover["status"] = status_match.group(1).strip()
    if window_match:
        handover["batch_window"] = window_match.group(1).strip()
    if finish_match:
        handover["end_timestamp"] = finish_match.group(1).strip()
    if val_match:
        try:
            handover["validation_results"] = json.loads(val_match.group(1))
        except json.JSONDecodeError:
            pass
    if rate_match:
        handover["rejection_rate"] = float(rate_match.group(1))

    dataset_Mapping = {
        "products": "product_table",
        "sales": "sales_table",
        "paint_analysis": "paint_analysis_table",
    }
    for src_name, target_table in dataset_Mapping.items():
        m = re.search(
            rf"{src_name}\s*\|\s*read=(\d+)\s*\|\s*output=(\d+)\s*\|\s*modified=(\d+)\s*\|\s*rejected=(\d+)",
            latest_block,
        )
        if m:
            handover["expected_counts"][target_table] = int(m.group(2))

    # Derive explicit date window from transformed sales/paint_analysis if available
    try:
        sales_file = INPUT_FILES["sales_table"]
        if sales_file.exists():
            s_df = pd.read_csv(sales_file, encoding="utf-8-sig")
            if "sales_date" in s_df.columns and not s_df["sales_date"].dropna().empty:
                min_d = s_df["sales_date"].dropna().min()
                max_d = s_df["sales_date"].dropna().max()
                handover["batch_window"] = f"{min_d} to {max_d}"
    except Exception:
        pass

    return handover


# ---------------------------------------------------------------------------
# TARGET DATABASE INITIALIZATION (SQLite)
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS product_table (
        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT NOT NULL UNIQUE,
        product_name TEXT NOT NULL,
        unit_purchase_price NUMERIC CHECK (unit_purchase_price IS NULL OR unit_purchase_price >= 0),
        estimated_price_php NUMERIC CHECK (estimated_price_php IS NULL OR estimated_price_php >= 0),
        stocks TEXT,
        category TEXT,
        brand TEXT,
        is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
        batch_id TEXT NOT NULL,
        transformation_run_id TEXT NOT NULL,
        loaded_at TEXT NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_product_table_name
    ON product_table (product_name);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_product_table_batch
    ON product_table (batch_id);
    """,
    """
    CREATE TABLE IF NOT EXISTS sales_table (
        sales_key INTEGER PRIMARY KEY AUTOINCREMENT,
        sales_id TEXT NOT NULL UNIQUE,
        product_key INTEGER NOT NULL,
        brand TEXT,
        product TEXT NOT NULL,
        category TEXT,
        sales_date TEXT NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity >= 0),
        unit_sold NUMERIC NOT NULL CHECK (unit_sold >= 0),
        season TEXT,
        batch_id TEXT NOT NULL,
        transformation_run_id TEXT NOT NULL,
        loaded_at TEXT NOT NULL,
        FOREIGN KEY (product_key) REFERENCES product_table (product_key)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sales_table_product_key
    ON sales_table (product_key);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sales_table_date
    ON sales_table (sales_date);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sales_table_batch
    ON sales_table (batch_id);
    """,
    """
    CREATE TABLE IF NOT EXISTS paint_analysis_table (
        analysis_key INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT,
        record_hash TEXT NOT NULL UNIQUE,
        color TEXT NOT NULL,
        rgb TEXT,
        paint_components TEXT,
        analysis_date TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        transformation_run_id TEXT NOT NULL,
        loaded_at TEXT NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_paint_analysis_date
    ON paint_analysis_table (analysis_date);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_paint_analysis_batch
    ON paint_analysis_table (batch_id);
    """,
    """
    CREATE TABLE IF NOT EXISTS load_log (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        transformation_run_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        batch_window TEXT NOT NULL,
        table_name TEXT NOT NULL,
        load_type TEXT NOT NULL,
        load_start_ts TEXT NOT NULL,
        load_end_ts TEXT NOT NULL,
        load_duration_sec REAL NOT NULL,
        records_received INTEGER NOT NULL,
        records_inserted INTEGER NOT NULL,
        records_updated INTEGER NOT NULL,
        records_rejected INTEGER NOT NULL,
        status TEXT NOT NULL,
        pre_load_result TEXT NOT NULL,
        post_load_result TEXT NOT NULL,
        error_code TEXT,
        error_message TEXT,
        load_code_version TEXT NOT NULL
    );
    """,
    """
    CREATE VIEW IF NOT EXISTS dim_product AS
    SELECT
        product_key,
        product_id,
        product_name,
        unit_purchase_price,
        estimated_price_php,
        stocks,
        category,
        brand,
        is_active,
        batch_id,
        transformation_run_id,
        loaded_at
    FROM product_table;
    """,
    """
    CREATE VIEW IF NOT EXISTS fact_sales AS
    SELECT
        sales_key,
        sales_id,
        product_key,
        brand,
        product,
        category,
        sales_date,
        quantity,
        unit_sold,
        season,
        batch_id,
        transformation_run_id,
        loaded_at
    FROM sales_table;
    """,
    """
    CREATE VIEW IF NOT EXISTS fact_paint_analysis AS
    SELECT
        analysis_key,
        id,
        record_hash,
        color,
        rgb,
        paint_components,
        analysis_date,
        batch_id,
        transformation_run_id,
        loaded_at
    FROM paint_analysis_table;
    """,
]


def initialize_target_database(conn: sqlite3.Connection) -> None:
    """Ensure foreign keys are enabled and all target tables and indexes exist."""
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    for ddl in DDL_STATEMENTS:
        conn.execute(ddl)
    conn.commit()


def backup_target_database(batch_id: str) -> Path | None:
    """
    Create a snapshot backup of the SQLite database before loading,
    reusing a single backup file per batch_id so rerunning the same batch
    never produces duplicate .db files in `load/backups/`.
    """
    if not TARGET_DB_PATH.exists() or TARGET_DB_PATH.stat().st_size == 0:
        return None
    safe_batch = re.sub(r"[^A-Za-z0-9_-]", "_", batch_id)
    backup_path = BACKUP_DIR / f"paintelligent_{safe_batch}.db"

    # Clean up any legacy timestamped duplicate backups for this same batch
    for old_dup in BACKUP_DIR.glob(f"paintelligent_{safe_batch}_*.db"):
        try:
            if not backup_path.exists():
                old_dup.rename(backup_path)
            else:
                old_dup.unlink()
        except OSError:
            pass

    if backup_path.exists():
        return backup_path
    shutil.copy2(TARGET_DB_PATH, backup_path)
    return backup_path


# ---------------------------------------------------------------------------
# SECTION 4: PRE-LOAD VALIDATION CHECKS
# ---------------------------------------------------------------------------

def run_pre_load_checks(
    conn: sqlite3.Connection,
    handover: dict[str, Any],
    incoming_dfs: dict[str, pd.DataFrame],
    allow_idempotent_rerun: bool = True,
) -> tuple[str, list[dict[str, Any]]]:
    """Run all 6 required Pre-Load Validation Checks before writing data."""
    checks: list[dict[str, Any]] = []

    # 1. Target connection and write permissions
    conn_ok = False
    try:
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _write_probe (id INTEGER);")
        conn.execute("INSERT INTO _write_probe (id) VALUES (1);")
        conn.execute("DROP TABLE _write_probe;")
        conn.commit()
        conn_ok = True
    except Exception as exc:
        logger.error("Target connection/write probe failed: %s", exc)

    checks.append({
        "check_name": "Target connection",
        "target": str(TARGET_DB_PATH.name),
        "purpose": "Ensure target SQLite database is reachable and writable",
        "criteria": "Connection succeeds and write probe completes within timeout",
        "severity": "Critical",
        "result": "PASS" if conn_ok else "FAIL",
    })

    # 2. Schema match (target tables exist & incoming CSV columns match contract)
    schema_ok = True
    schema_details = []
    for table_name, expected_cols in EXPECTED_SCHEMAS.items():
        df = incoming_dfs.get(table_name)
        if df is None or list(df.columns) != expected_cols:
            schema_ok = False
            actual_cols = list(df.columns) if df is not None else []
            schema_details.append(f"{table_name}: expected {expected_cols}, got {actual_cols}")

        cur = conn.execute(f"PRAGMA table_info({table_name});")
        db_cols = {row[1] for row in cur.fetchall()}
        if not set(expected_cols).issubset(db_cols):
            schema_ok = False
            schema_details.append(f"{table_name}: missing target columns in DB")

    checks.append({
        "check_name": "Schema match",
        "target": "All target tables & incoming CSVs",
        "purpose": "Prevent column mismatch or type errors during load",
        "criteria": "All required columns exist in incoming CSVs and target SQLite tables",
        "severity": "Critical",
        "result": "PASS" if schema_ok else "FAIL",
        "details": "; ".join(schema_details) if schema_details else "All schemas matched",
    })

    # 3. Handover status check
    approved_statuses = {"SUCCESS", "SUCCESS_WITH_QUARANTINE", "APPROVED"}
    handover_status = str(handover.get("status", "UNKNOWN")).upper()
    status_ok = handover_status in approved_statuses
    checks.append({
        "check_name": "Handover status",
        "target": "Transformation metadata (logs/transformation.log)",
        "purpose": "Load only validated and approved transformation outputs",
        "criteria": "Transformation status in {'SUCCESS', 'SUCCESS_WITH_QUARANTINE', 'APPROVED'}",
        "severity": "Critical",
        "result": "PASS" if status_ok else "FAIL",
        "details": f"Transformation status = {handover_status}",
    })

    # 4. Input record counts match handover metadata
    counts_ok = True
    count_details = []
    expected_counts = handover.get("expected_counts", {})
    if not expected_counts:
        counts_ok = False
        count_details.append("No expected dataset counts found in handover metadata")
    else:
        for table_name, df in incoming_dfs.items():
            expected_n = expected_counts.get(table_name)
            actual_n = len(df)
            if expected_n is None or expected_n != actual_n:
                counts_ok = False
                count_details.append(f"{table_name}: expected {expected_n}, got {actual_n}")
            else:
                count_details.append(f"{table_name}={actual_n}")

    checks.append({
        "check_name": "Handover record count",
        "target": "All transformed CSV files",
        "purpose": "Confirm complete handover without truncated or missing files",
        "criteria": "Incoming CSV row counts equal transformation output counts",
        "severity": "Critical",
        "result": "PASS" if counts_ok else "FAIL",
        "details": ", ".join(count_details),
    })

    # 5. Sufficient storage space
    usage = shutil.disk_usage(LOAD_DIR)
    space_ok = usage.free >= MIN_FREE_DISK_BYTES
    checks.append({
        "check_name": "Storage space availability",
        "target": "Target disk volume",
        "purpose": "Prevent mid-transaction disk exhaustion during SQLite write and backup",
        "criteria": f"Free disk space >= {MIN_FREE_DISK_BYTES // (1024 * 1024)} MB",
        "severity": "Critical",
        "result": "PASS" if space_ok else "FAIL",
        "details": f"Free space = {usage.free / (1024 * 1024):.1f} MB",
    })

    # 6. Duplicate batch check
    batch_id = handover.get("batch_id", "UNKNOWN")
    cur = conn.execute(
        "SELECT COUNT(*) FROM load_log WHERE batch_id = ? AND status = 'SUCCESS';",
        (batch_id,),
    )
    prev_success_count = cur.fetchone()[0]
    if prev_success_count == 0:
        dup_result = "PASS"
        dup_details = f"Batch {batch_id} has not been previously loaded"
    elif allow_idempotent_rerun:
        dup_result = "PASS"
        dup_details = (
            f"Batch {batch_id} detected in load_log ({prev_success_count} table entries); "
            "idempotent rerun strategy active (upsert/delete-by-batch-ID)"
        )
    else:
        dup_result = "FAIL"
        dup_details = f"Batch {batch_id} already loaded with SUCCESS"

    checks.append({
        "check_name": "Duplicate batch check",
        "target": "load_log.batch_id",
        "purpose": "Prevent duplicate row insertion or inflated totals on batch rerun",
        "criteria": "Batch ID not previously loaded, or idempotent rerun mode enabled",
        "severity": "Critical",
        "result": dup_result,
        "details": dup_details,
    })

    overall = "PASS" if all(c["result"] == "PASS" for c in checks) else "FAIL"
    return overall, checks


# ---------------------------------------------------------------------------
# SECTION 3: TABLE LOADERS (INCREMENTAL UPSERT / BATCH-ID IDEMPOTENCY)
# ---------------------------------------------------------------------------

def clean_val(val: Any) -> Any:
    if val is None or pd.isna(val):
        return None
    if isinstance(val, str):
        s = val.strip()
        return s if s != "" else None
    return val


def to_float_or_none(val: Any) -> float | None:
    v = clean_val(val)
    if v is None:
        return None
    return float(Decimal(str(v)))


def to_int_or_none(val: Any) -> int | None:
    v = clean_val(val)
    if v is None:
        return None
    return int(Decimal(str(v)))


def load_products_table(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    run_id: str,
    transformation_run_id: str,
    batch_id: str,
    rejected_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Load `product_table` (Dimension) using Incremental Upsert (SCD Type 1).
    Match key: (product_name, estimated_price_php, unit_purchase_price, brand, category)
    so that distinct catalog items (including items sharing a base name with
    different price tiers, e.g., TEST 1 @ 750 vs 1000) are preserved while
    rerunning the same batch updates existing rows idempotently without
    duplicating products or breaking foreign keys from `sales_table`.
    """
    t0 = time.perf_counter()
    start_ts = now_str()
    loaded_at = utc_iso()

    received = len(df)
    inserted = 0
    updated = 0
    rejected = 0

    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        raw_dict = row.to_dict()
        try:
            product_id = clean_val(row.get("product_id"))
            product_name = clean_val(row.get("product_name"))
            if not product_id or not product_name:
                raise ValueError("Missing required product_id or product_name")

            unit_cost = to_float_or_none(row.get("unit_purchase_price"))
            est_price = to_float_or_none(row.get("estimated_price_php"))
            if (unit_cost is not None and unit_cost < 0) or (est_price is not None and est_price < 0):
                raise ValueError("Negative price violates CHECK constraint")

            stocks = clean_val(row.get("stocks"))
            category = clean_val(row.get("category"))
            brand = clean_val(row.get("brand"))
            is_active = 0 if "inactive" in product_name.lower() else 1

            # Check if matching product already exists in dimension table
            cur = conn.execute(
                """
                SELECT product_key FROM product_table
                WHERE product_name = ?
                  AND COALESCE(unit_purchase_price, -1) = COALESCE(?, -1)
                  AND COALESCE(estimated_price_php, -1) = COALESCE(?, -1)
                  AND COALESCE(category, '') = COALESCE(?, '')
                  AND COALESCE(brand, '') = COALESCE(?, '')
                LIMIT 1;
                """,
                (product_name, unit_cost, est_price, category, brand),
            )
            existing = cur.fetchone()

            if existing is None:
                conn.execute(
                    """
                    INSERT INTO product_table (
                        product_id, product_name, unit_purchase_price,
                        estimated_price_php, stocks, category, brand,
                        is_active, batch_id, transformation_run_id, loaded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        product_id, product_name, unit_cost,
                        est_price, stocks, category, brand,
                        is_active, batch_id, transformation_run_id, loaded_at,
                    ),
                )
                inserted += 1
            else:
                product_key = existing[0]
                conn.execute(
                    """
                    UPDATE product_table
                    SET product_id = ?,
                        unit_purchase_price = ?,
                        estimated_price_php = ?,
                        stocks = ?,
                        category = ?,
                        brand = ?,
                        is_active = ?,
                        batch_id = ?,
                        transformation_run_id = ?,
                        loaded_at = ?
                    WHERE product_key = ?;
                    """,
                    (
                        product_id, unit_cost, est_price, stocks,
                        category, brand, is_active, batch_id,
                        transformation_run_id, loaded_at, product_key,
                    ),
                )
                updated += 1

        except Exception as exc:
            rejected += 1
            rejected_records.append({
                "run_id": run_id,
                "batch_id": batch_id,
                "table_name": "product_table",
                "source_row_number": row_num,
                "original_record": json.dumps(raw_dict, ensure_ascii=False, default=str),
                "error_code": "LOAD_PRODUCT_REJECT",
                "rejection_reason": str(exc),
                "rejected_timestamp": utc_iso(),
            })

    duration = round(time.perf_counter() - t0, 3)
    end_ts = now_str()
    return {
        "table_name": "product_table",
        "load_type": "INCREMENTAL",
        "load_start_ts": start_ts,
        "load_end_ts": end_ts,
        "load_duration_sec": duration,
        "records_received": received,
        "records_inserted": inserted,
        "records_updated": updated,
        "records_rejected": rejected,
    }


def load_sales_table(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    run_id: str,
    transformation_run_id: str,
    batch_id: str,
    rejected_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Load `sales_table` (Fact) using Incremental Delete-by-Batch-ID + Insert.
    Links each sale record to `product_table(product_key)` via canonical product name.
    """
    t0 = time.perf_counter()
    start_ts = now_str()
    loaded_at = utc_iso()

    received = len(df)
    inserted = 0
    updated = 0
    rejected = 0

    # Idempotency: remove any existing records from the same batch_id before inserting
    conn.execute("DELETE FROM sales_table WHERE batch_id = ?;", (batch_id,))

    # Build lookup map from product_name -> product_key
    cur = conn.execute("SELECT product_name, MIN(product_key) FROM product_table GROUP BY product_name;")
    product_lookup = {row[0]: row[1] for row in cur.fetchall()}

    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        raw_dict = row.to_dict()
        try:
            sales_id = clean_val(row.get("sales_id"))
            product_name = clean_val(row.get("product"))
            sales_date = clean_val(row.get("sales_date"))
            quantity = to_int_or_none(row.get("quantity"))
            unit_sold = to_float_or_none(row.get("unit_sold"))

            if not sales_id or not product_name or not sales_date:
                raise ValueError("Missing required sales_id, product, or sales_date")
            if quantity is None or quantity < 0:
                raise ValueError(f"Invalid or negative quantity: {quantity}")
            if unit_sold is None or unit_sold < 0:
                raise ValueError(f"Invalid or negative unit_sold: {unit_sold}")

            product_key = product_lookup.get(product_name)
            if product_key is None:
                raise ValueError(f"Foreign key violation: product '{product_name}' not found in product_table")

            brand = clean_val(row.get("brand"))
            category = clean_val(row.get("category"))
            season = clean_val(row.get("season"))

            conn.execute(
                """
                INSERT INTO sales_table (
                    sales_id, product_key, brand, product, category,
                    sales_date, quantity, unit_sold, season,
                    batch_id, transformation_run_id, loaded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    sales_id, product_key, brand, product_name, category,
                    sales_date, quantity, unit_sold, season,
                    batch_id, transformation_run_id, loaded_at,
                ),
            )
            inserted += 1

        except Exception as exc:
            rejected += 1
            rejected_records.append({
                "run_id": run_id,
                "batch_id": batch_id,
                "table_name": "sales_table",
                "source_row_number": row_num,
                "original_record": json.dumps(raw_dict, ensure_ascii=False, default=str),
                "error_code": "LOAD_SALES_REJECT",
                "rejection_reason": str(exc),
                "rejected_timestamp": utc_iso(),
            })

    duration = round(time.perf_counter() - t0, 3)
    end_ts = now_str()
    return {
        "table_name": "sales_table",
        "load_type": "INCREMENTAL",
        "load_start_ts": start_ts,
        "load_end_ts": end_ts,
        "load_duration_sec": duration,
        "records_received": received,
        "records_inserted": inserted,
        "records_updated": updated,
        "records_rejected": rejected,
    }


def load_paint_analysis_table(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    run_id: str,
    transformation_run_id: str,
    batch_id: str,
    rejected_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Load `paint_analysis_table` using Incremental Upsert on `record_hash`
    (derived deterministically from id, color, rgb, paint_components, analysis_date).
    """
    t0 = time.perf_counter()
    start_ts = now_str()
    loaded_at = utc_iso()

    received = len(df)
    inserted = 0
    updated = 0
    rejected = 0

    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        raw_dict = row.to_dict()
        try:
            src_id = clean_val(row.get("id"))
            color = clean_val(row.get("color"))
            rgb = clean_val(row.get("rgb"))
            paint_components = clean_val(row.get("paint_components"))
            analysis_date = clean_val(row.get("analysis_date"))

            if not color or not analysis_date:
                raise ValueError("Missing required color or analysis_date")

            hash_input = f"{src_id or ''}|{color}|{rgb or ''}|{paint_components or ''}|{analysis_date}"
            record_hash = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()[:24].upper()

            cur = conn.execute(
                "SELECT analysis_key FROM paint_analysis_table WHERE record_hash = ? LIMIT 1;",
                (record_hash,),
            )
            existing = cur.fetchone()

            if existing is None:
                conn.execute(
                    """
                    INSERT INTO paint_analysis_table (
                        id, record_hash, color, rgb, paint_components,
                        analysis_date, batch_id, transformation_run_id, loaded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        src_id, record_hash, color, rgb, paint_components,
                        analysis_date, batch_id, transformation_run_id, loaded_at,
                    ),
                )
                inserted += 1
            else:
                analysis_key = existing[0]
                conn.execute(
                    """
                    UPDATE paint_analysis_table
                    SET id = ?,
                        color = ?,
                        rgb = ?,
                        paint_components = ?,
                        analysis_date = ?,
                        batch_id = ?,
                        transformation_run_id = ?,
                        loaded_at = ?
                    WHERE analysis_key = ?;
                    """,
                    (
                        src_id, color, rgb, paint_components,
                        analysis_date, batch_id, transformation_run_id,
                        loaded_at, analysis_key,
                    ),
                )
                updated += 1

        except Exception as exc:
            rejected += 1
            rejected_records.append({
                "run_id": run_id,
                "batch_id": batch_id,
                "table_name": "paint_analysis_table",
                "source_row_number": row_num,
                "original_record": json.dumps(raw_dict, ensure_ascii=False, default=str),
                "error_code": "LOAD_PAINT_REJECT",
                "rejection_reason": str(exc),
                "rejected_timestamp": utc_iso(),
            })

    duration = round(time.perf_counter() - t0, 3)
    end_ts = now_str()
    return {
        "table_name": "paint_analysis_table",
        "load_type": "INCREMENTAL",
        "load_start_ts": start_ts,
        "load_end_ts": end_ts,
        "load_duration_sec": duration,
        "records_received": received,
        "records_inserted": inserted,
        "records_updated": updated,
        "records_rejected": rejected,
    }


# ---------------------------------------------------------------------------
# SECTION 5: POST-LOAD VALIDATION AND RECONCILIATION
# ---------------------------------------------------------------------------

def run_post_load_checks(
    conn: sqlite3.Connection,
    batch_id: str,
    incoming_dfs: dict[str, pd.DataFrame],
    table_stats: dict[str, dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    """Run all required Post-Load Validation and Reconciliation checks."""
    checks: list[dict[str, Any]] = []

    # 1. Record reconciliation: received == inserted + updated + rejected
    recon_ok = True
    recon_details = []
    for t_name, st in table_stats.items():
        rec = st["records_received"]
        loaded = st["records_inserted"] + st["records_updated"]
        rej = st["records_rejected"]
        if rec != loaded + rej or rej > 0:
            recon_ok = False
        recon_details.append(f"{t_name}: received={rec}, loaded={loaded} (ins={st['records_inserted']}, upd={st['records_updated']}), rejected={rej}")

    checks.append({
        "check_name": "Record reconciliation",
        "target": "All target tables",
        "purpose": "Detect lost, extra, or rejected records during load",
        "criteria": "records_received = records_loaded + records_rejected (and records_rejected = 0)",
        "severity": "Critical",
        "result": "PASS" if recon_ok else "FAIL",
        "details": "; ".join(recon_details),
    })

    # 2. Target row count for the batch matches expected count
    row_count_ok = True
    count_details = []
    for t_name, df in incoming_dfs.items():
        cur = conn.execute(f"SELECT COUNT(*) FROM {t_name} WHERE batch_id = ?;", (batch_id,))
        db_count = cur.fetchone()[0]
        expected_count = len(df)
        if db_count != expected_count:
            row_count_ok = False
        count_details.append(f"{t_name}: target_batch_rows={db_count}, expected={expected_count}")

    checks.append({
        "check_name": "Target batch row count",
        "target": "product_table, sales_table, paint_analysis_table",
        "purpose": "Confirm target database holds the exact expected batch volume",
        "criteria": "Target row count for batch_id equals incoming handover row count",
        "severity": "Critical",
        "result": "PASS" if row_count_ok else "FAIL",
        "details": "; ".join(count_details),
    })

    # 3. Primary key & surrogate key uniqueness
    pk_ok = True
    pk_details = []
    for t_name, pk_col, nat_col in [
        ("product_table", "product_key", "product_id"),
        ("sales_table", "sales_key", "sales_id"),
        ("paint_analysis_table", "analysis_key", "record_hash"),
    ]:
        cur = conn.execute(
            f"""
            SELECT
                COUNT(*) - COUNT(DISTINCT {pk_col}),
                COUNT(*) - COUNT(DISTINCT {nat_col}),
                SUM(CASE WHEN {pk_col} IS NULL OR {nat_col} IS NULL THEN 1 ELSE 0 END)
            FROM {t_name};
            """
        )
        dup_pk, dup_nat, null_keys = cur.fetchone()
        dup_pk = dup_pk or 0
        dup_nat = dup_nat or 0
        null_keys = null_keys or 0
        if dup_pk > 0 or dup_nat > 0 or null_keys > 0:
            pk_ok = False
        pk_details.append(f"{t_name}: dup_pk={dup_pk}, dup_id={dup_nat}, null_keys={null_keys}")

    checks.append({
        "check_name": "Primary key uniqueness",
        "target": "product_table, sales_table, paint_analysis_table keys",
        "purpose": "Prevent duplicate primary/surrogate keys or null identifiers",
        "criteria": "Duplicate key count = 0 and null key count = 0",
        "severity": "Critical",
        "result": "PASS" if pk_ok else "FAIL",
        "details": "; ".join(pk_details),
    })

    # 4. Referential integrity between fact (sales_table) and dimension (product_table)
    cur = conn.execute(
        """
        SELECT COUNT(*)
        FROM sales_table s
        LEFT JOIN product_table p ON s.product_key = p.product_key
        WHERE p.product_key IS NULL;
        """
    )
    orphan_sales = cur.fetchone()[0]
    fk_ok = orphan_sales == 0
    checks.append({
        "check_name": "Referential integrity",
        "target": "sales_table.product_key -> product_table.product_key",
        "purpose": "Prevent orphan sales fact records with no matching product dimension",
        "criteria": "Orphan fact count = 0 (100% match in product_table)",
        "severity": "Critical",
        "result": "PASS" if fk_ok else "FAIL",
        "details": f"Orphan sales records = {orphan_sales}",
    })

    # 5. Required columns NOT NULL check
    cur_p = conn.execute(
        "SELECT COUNT(*) FROM product_table WHERE product_id IS NULL OR product_name IS NULL;"
    )
    null_p = cur_p.fetchone()[0]
    cur_s = conn.execute(
        "SELECT COUNT(*) FROM sales_table WHERE sales_id IS NULL OR product IS NULL OR sales_date IS NULL OR quantity IS NULL OR unit_sold IS NULL;"
    )
    null_s = cur_s.fetchone()[0]
    cur_pa = conn.execute(
        "SELECT COUNT(*) FROM paint_analysis_table WHERE color IS NULL OR analysis_date IS NULL;"
    )
    null_pa = cur_pa.fetchone()[0]
    req_null_ok = (null_p + null_s + null_pa) == 0

    checks.append({
        "check_name": "Required columns not null",
        "target": "Required columns across all target tables",
        "purpose": "Confirm mandatory business attributes are populated",
        "criteria": "Null count in mandatory columns = 0",
        "severity": "Critical",
        "result": "PASS" if req_null_ok else "FAIL",
        "details": f"Mandatory nulls: product_table={null_p}, sales_table={null_s}, paint_analysis_table={null_pa}",
    })

    # 6. Aggregate reconciliation (sales unit_sold, quantity, seasonal breakdown, and product prices)
    sales_df = incoming_dfs["sales_table"]
    src_sales_sum = float(sales_df["unit_sold"].dropna().apply(lambda x: Decimal(str(x))).sum())
    src_qty_sum = int(sales_df["quantity"].dropna().apply(lambda x: int(Decimal(str(x)))).sum())

    # Seasonal breakdown in source CSV (Dry Season vs Rainy Season)
    dry_mask = sales_df["season"].astype(str).str.strip().str.lower().isin({"dry", "dry season"})
    rainy_mask = sales_df["season"].astype(str).str.strip().str.lower().isin({"rainy", "rainy season", "wet", "wet season"})
    src_dry_rows = int(dry_mask.sum())
    src_dry_sales = float(sales_df.loc[dry_mask, "unit_sold"].dropna().apply(lambda x: Decimal(str(x))).sum())
    src_dry_qty = int(sales_df.loc[dry_mask, "quantity"].dropna().apply(lambda x: int(Decimal(str(x)))).sum())
    src_rainy_rows = int(rainy_mask.sum())
    src_rainy_sales = float(sales_df.loc[rainy_mask, "unit_sold"].dropna().apply(lambda x: Decimal(str(x))).sum())
    src_rainy_qty = int(sales_df.loc[rainy_mask, "quantity"].dropna().apply(lambda x: int(Decimal(str(x)))).sum())

    cur = conn.execute(
        "SELECT COALESCE(SUM(unit_sold), 0), COALESCE(SUM(quantity), 0) FROM sales_table WHERE batch_id = ?;",
        (batch_id,),
    )
    db_sales_sum, db_qty_sum = cur.fetchone()
    db_sales_sum = float(db_sales_sum)
    db_qty_sum = int(db_qty_sum)

    cur = conn.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(unit_sold), 0),
            COALESCE(SUM(quantity), 0)
        FROM sales_table
        WHERE batch_id = ? AND LOWER(TRIM(season)) IN ('dry', 'dry season');
        """,
        (batch_id,),
    )
    db_dry_rows, db_dry_sales, db_dry_qty = cur.fetchone()
    db_dry_rows = int(db_dry_rows)
    db_dry_sales = float(db_dry_sales)
    db_dry_qty = int(db_dry_qty)

    cur = conn.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(unit_sold), 0),
            COALESCE(SUM(quantity), 0)
        FROM sales_table
        WHERE batch_id = ? AND LOWER(TRIM(season)) IN ('rainy', 'rainy season', 'wet', 'wet season');
        """,
        (batch_id,),
    )
    db_rainy_rows, db_rainy_sales, db_rainy_qty = cur.fetchone()
    db_rainy_rows = int(db_rainy_rows)
    db_rainy_sales = float(db_rainy_sales)
    db_rainy_qty = int(db_rainy_qty)

    prod_df = incoming_dfs["product_table"]
    src_est_price_sum = float(prod_df["estimated_price_php"].dropna().apply(lambda x: Decimal(str(x))).sum())
    cur = conn.execute(
        "SELECT COALESCE(SUM(estimated_price_php), 0) FROM product_table WHERE batch_id = ?;",
        (batch_id,),
    )
    db_est_price_sum = float(cur.fetchone()[0])

    agg_ok = (
        abs(src_sales_sum - db_sales_sum) < 0.01
        and src_qty_sum == db_qty_sum
        and abs(src_dry_sales - db_dry_sales) < 0.01
        and abs(src_rainy_sales - db_rainy_sales) < 0.01
        and abs(src_est_price_sum - db_est_price_sum) < 0.01
    )
    checks.append({
        "check_name": "Amount and quantity reconciliation",
        "target": "sales_table (unit_sold, quantity, season), product_table (estimated_price_php)",
        "purpose": "Confirm no financial, seasonal, or inventory value loss during load",
        "criteria": "Source batch sum == Target batch sum (including Dry & Rainy season sums within 0.01 tolerance)",
        "severity": "Critical",
        "result": "PASS" if agg_ok else "FAIL",
        "details": (
            f"sales.unit_sold: src={src_sales_sum:,.2f} vs db={db_sales_sum:,.2f}; "
            f"sales.quantity: src={src_qty_sum:,} vs db={db_qty_sum:,}; "
            f"product.estimated_price_php: src={src_est_price_sum:,.2f} vs db={db_est_price_sum:,.2f}"
        ),
        "season_details": (
            f"Dry Season ({db_dry_rows} rows): sales={db_dry_sales:,.2f} (src={src_dry_sales:,.2f}), qty={db_dry_qty:,} | "
            f"Rainy Season ({db_rainy_rows} rows): sales={db_rainy_sales:,.2f} (src={src_rainy_sales:,.2f}), qty={db_rainy_qty:,}"
        ),
        "season_metrics": {
            "dry_season": {"rows": db_dry_rows, "unit_sold_sum": db_dry_sales, "quantity_sum": db_dry_qty},
            "rainy_season": {"rows": db_rainy_rows, "unit_sold_sum": db_rainy_sales, "quantity_sum": db_rainy_qty},
        },
    })

    # 7. Non-critical optional attribute completeness check (category/brand null rate)
    cur = conn.execute(
        """
        SELECT
            COUNT(*),
            SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END)
        FROM product_table
        WHERE batch_id = ?;
        """,
        (batch_id,),
    )
    total_p, null_cat, null_brand = cur.fetchone()
    cat_rate = (null_cat / total_p * 100.0) if total_p else 0.0
    brand_rate = (null_brand / total_p * 100.0) if total_p else 0.0
    # Transformation profile documented 28 missing categories (4.84%) and 26 missing brands (4.49%)
    opt_null_ok = cat_rate <= 5.0 and brand_rate <= 5.0

    checks.append({
        "check_name": "Optional attribute null rate check",
        "target": "product_table.category, product_table.brand",
        "purpose": "Monitor optional catalog attribute completeness",
        "criteria": "Null rate <= 5.0% per transformation baseline (28 category, 26 brand)",
        "severity": "Non-critical",
        "result": "PASS" if opt_null_ok else "WARNING",
        "details": f"category nulls={null_cat}/{total_p} ({cat_rate:.2f}%), brand nulls={null_brand}/{total_p} ({brand_rate:.2f}%)",
    })

    critical_failed = any(c["severity"] == "Critical" and c["result"] != "PASS" for c in checks)
    has_warning = any(c["result"] == "WARNING" for c in checks)
    if critical_failed:
        overall = "FAIL"
    elif has_warning:
        overall = "WARNING"
    else:
        overall = "PASS"

    return overall, checks


# ---------------------------------------------------------------------------
# MAIN LOAD ORCHESTRATION
# ---------------------------------------------------------------------------

def deduplicate_log_for_batch(batch_id: str) -> None:
    """
    If `logs/load.log` already contains a report block for `batch_id`,
    remove the old block for that batch so re-running the same batch
    does not stack duplicate reports in `logs/load.log`.
    """
    global file_handler
    if not LOAD_LOG_FILE.exists():
        return
    try:
        logger.removeHandler(file_handler)
        file_handler.close()

        content = LOAD_LOG_FILE.read_text(encoding="utf-8", errors="replace")
        # Split into report blocks starting at each top banner line before "PAINTELLIGENT STAGE 3"
        pattern = re.compile(
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \| INFO \| ={78}\r?\n"
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \| INFO \| PAINTELLIGENT STAGE 3: DATA LOAD REPORT",
            re.MULTILINE,
        )
        starts = [m.start() for m in pattern.finditer(content)]
        kept_chunks: list[str] = []
        for idx, start_pos in enumerate(starts):
            end_pos = starts[idx + 1] if idx + 1 < len(starts) else len(content)
            chunk = content[start_pos:end_pos].strip()
            if batch_id not in chunk:
                kept_chunks.append(chunk)

        new_text = "\n".join(kept_chunks)
        if new_text:
            new_text += "\n"
        LOAD_LOG_FILE.write_text(new_text, encoding="utf-8")

        file_handler = logging.FileHandler(LOAD_LOG_FILE, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        pass


def run_load() -> int:
    run_id = make_load_run_id()
    overall_start = time.perf_counter()
    start_ts = now_str()

    handover = read_transformation_handover()
    transformation_run_id = handover.get("run_id", "UNKNOWN")
    batch_id = handover.get("batch_id", "UNKNOWN")
    batch_window = handover.get("batch_window", "Full available source data")

    # Remove any previous log block for this same batch_id so load.log never repeats
    deduplicate_log_for_batch(batch_id)

    # Read transformed handover CSVs without re-extracting or re-transforming
    incoming_dfs: dict[str, pd.DataFrame] = {}
    for table_name, csv_path in INPUT_FILES.items():
        if not csv_path.exists():
            logger.error("Missing required handover file: %s", csv_path)
            return 1
        incoming_dfs[table_name] = pd.read_csv(csv_path, encoding="utf-8-sig")

    # Ensure single backup per batch_id
    backup_path = backup_target_database(batch_id)

    conn = sqlite3.connect(TARGET_DB_PATH, timeout=30.0)
    initialize_target_database(conn)

    rejected_records: list[dict[str, Any]] = []
    table_stats: dict[str, dict[str, Any]] = {}
    pre_checks: list[dict[str, Any]] = []
    post_checks: list[dict[str, Any]] = []
    pre_load_result = "FAIL"
    post_load_result = "FAIL"
    overall_status = "FAILED"
    acceptance_decision = "REJECTED"
    error_code = None
    error_message = None

    try:
        # Step 1: Pre-Load Validation Checks
        pre_load_result, pre_checks = run_pre_load_checks(conn, handover, incoming_dfs)
        if pre_load_result != "PASS":
            error_code = "PRE_LOAD_VALIDATION_FAILED"
            failed_names = [c["check_name"] for c in pre_checks if c["result"] != "PASS"]
            error_message = f"Critical pre-load checks failed: {', '.join(failed_names)}"
            raise RuntimeError(error_message)

        # Step 2: Load tables in strict parent-before-child order inside a single transaction
        if conn.in_transaction:
            conn.commit()
        conn.execute("BEGIN IMMEDIATE;")

        # 2a. Load Dimension: product_table
        table_stats["product_table"] = load_products_table(
            conn, incoming_dfs["product_table"], run_id, transformation_run_id, batch_id, rejected_records
        )
        # 2b. Load Fact: sales_table (depends on product_table)
        table_stats["sales_table"] = load_sales_table(
            conn, incoming_dfs["sales_table"], run_id, transformation_run_id, batch_id, rejected_records
        )
        # 2c. Load Analytical Table: paint_analysis_table
        table_stats["paint_analysis_table"] = load_paint_analysis_table(
            conn, incoming_dfs["paint_analysis_table"], run_id, transformation_run_id, batch_id, rejected_records
        )

        # Step 3: Post-Load Validation and Reconciliation (before committing transaction!)
        post_load_result, post_checks = run_post_load_checks(conn, batch_id, incoming_dfs, table_stats)
        if post_load_result == "FAIL":
            error_code = "POST_LOAD_VALIDATION_FAILED"
            failed_names = [c["check_name"] for c in post_checks if c["result"] == "FAIL"]
            error_message = f"Critical post-load checks failed: {', '.join(failed_names)}"
            raise RuntimeError(error_message)

        # Commit atomic batch transaction
        conn.commit()

        if post_load_result == "WARNING":
            overall_status = "WARNING"
            acceptance_decision = "MANUAL REVIEW"
        else:
            overall_status = "SUCCESS"
            acceptance_decision = "APPROVED"

    except Exception as exc:
        conn.rollback()
        overall_status = "FAILED"
        acceptance_decision = "REJECTED"
        if not error_code:
            error_code = type(exc).__name__
        if not error_message:
            error_message = str(exc)

    # Write rejected records file
    pd.DataFrame(rejected_records, columns=REJECTED_COLUMNS).to_csv(
        LOAD_REJECTED_FILE, index=False, encoding="utf-8-sig"
    )

    # Deduplicate SQLite `load_log` for this batch_id so reruns do not stack duplicate rows
    conn.execute("DELETE FROM load_log WHERE batch_id = ?;", (batch_id,))
    for t_name in ("product_table", "sales_table", "paint_analysis_table"):
        st = table_stats.get(t_name, {
            "load_type": "INCREMENTAL",
            "load_start_ts": start_ts,
            "load_end_ts": now_str(),
            "load_duration_sec": 0.0,
            "records_received": len(incoming_dfs.get(t_name, [])),
            "records_inserted": 0,
            "records_updated": 0,
            "records_rejected": 0,
        })
        conn.execute(
            """
            INSERT INTO load_log (
                run_id, transformation_run_id, batch_id, batch_window,
                table_name, load_type, load_start_ts, load_end_ts,
                load_duration_sec, records_received, records_inserted,
                records_updated, records_rejected, status,
                pre_load_result, post_load_result, error_code,
                error_message, load_code_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                run_id,
                transformation_run_id,
                batch_id,
                batch_window,
                t_name,
                st["load_type"],
                st["load_start_ts"],
                st["load_end_ts"],
                st["load_duration_sec"],
                st["records_received"],
                st["records_inserted"],
                st["records_updated"],
                st["records_rejected"],
                overall_status,
                pre_load_result,
                post_load_result,
                error_code,
                error_message,
                LOAD_CODE_VERSION,
            ),
        )
    conn.commit()

    # Export a human-readable SQL file alongside paintelligent.db (and remove any legacy paintelligent_dump.sql)
    try:
        legacy_dump = LOAD_DIR / "paintelligent_dump.sql"
        if legacy_dump.exists():
            legacy_dump.unlink()
        with open(TARGET_SQL_DUMP_PATH, "w", encoding="utf-8") as sql_file:
            for line in conn.iterdump():
                sql_file.write(f"{line}\n")
    except Exception as dump_exc:
        logger.warning("Could not write readable SQL dump: %s", dump_exc)

    conn.close()

    total_duration = round(time.perf_counter() - overall_start, 3)
    end_ts = now_str()

    agg_check = next((c for c in post_checks if c["check_name"] == "Amount and quantity reconciliation"), {})
    agg_detail = agg_check.get("details", "N/A")
    season_detail = agg_check.get("season_details", "N/A")
    season_metrics = agg_check.get("season_metrics", {})
    null_detail = next((c["details"] for c in post_checks if c["check_name"] == "Optional attribute null rate check"), "N/A")

    summary_payload = {
        "run_id": run_id,
        "transformation_run_id": transformation_run_id,
        "batch_id": batch_id,
        "batch_window": batch_window,
        "target_database": str(TARGET_DB_PATH),
        "load_start_ts": start_ts,
        "load_end_ts": end_ts,
        "load_duration_sec": total_duration,
        "status": overall_status,
        "acceptance_decision": acceptance_decision,
        "pre_load_result": pre_load_result,
        "post_load_result": post_load_result,
        "error_code": error_code,
        "error_message": error_message,
        "load_code_version": LOAD_CODE_VERSION,
        "operational_log_retention_days": OPERATIONAL_LOG_RETENTION_DAYS,
        "audit_log_retention_days": AUDIT_LOG_RETENTION_DAYS,
        "table_metrics": table_stats,
        "seasonal_sales_metrics": season_metrics,
    }
    with open(LOAD_SUMMARY_JSON, "w", encoding="utf-8") as f:
        json.dump(summary_payload, f, indent=2, ensure_ascii=False)

    # Emit a single, clean, non-repetitive report to load.log following load.md
    pre_summary = " | ".join(f"{c['check_name']}={c['result']}" for c in pre_checks)
    post_summary = " | ".join(f"{c['check_name']}={c['result']}" for c in post_checks)

    logger.info("=" * 78)
    logger.info("PAINTELLIGENT STAGE 3: DATA LOAD REPORT")
    logger.info("  Run ID               : %s", run_id)
    logger.info("  Transformation Run ID: %s", transformation_run_id)
    logger.info("  Batch ID             : %s", batch_id)
    logger.info("  Batch Window         : %s", batch_window)
    logger.info("  Target Database      : %s (Backup: %s)", TARGET_DB_PATH.name, backup_path.name if backup_path else "N/A")
    logger.info("  Code & Retention     : %s | Operational=%dd | Audit=%dd", LOAD_CODE_VERSION, OPERATIONAL_LOG_RETENTION_DAYS, AUDIT_LOG_RETENTION_DAYS)
    logger.info("-" * 78)
    logger.info("PRE-LOAD CHECKS (%s) : %s", pre_load_result, pre_summary)
    logger.info("-" * 78)
    logger.info("TABLE LOAD METRICS:")
    for t_name, st in table_stats.items():
        logger.info(
            "  %-20s | type=%s | start=%s | end=%s | duration=%.3fs | received=%d | inserted=%d | updated=%d | rejected=%d | status=%s",
            t_name,
            st["load_type"],
            st["load_start_ts"],
            st["load_end_ts"],
            st["load_duration_sec"],
            st["records_received"],
            st["records_inserted"],
            st["records_updated"],
            st["records_rejected"],
            overall_status,
        )
    logger.info("-" * 78)
    logger.info("POST-LOAD CHECKS (%s): %s", post_load_result, post_summary)
    logger.info("  Reconciled Totals    : %s", agg_detail)
    logger.info("  Seasonal Sales       : %s", season_detail)
    logger.info("  Optional Null Rates  : %s", null_detail)
    logger.info("-" * 78)
    logger.info(
        "FINAL RESULT           : status=%s | decision=%s | error=%s | rejected_rows=%d | total_duration=%.3fs",
        overall_status,
        acceptance_decision,
        error_code or "NULL",
        len(rejected_records),
        total_duration,
    )
    if error_message:
        logger.error("  Error Details        : %s", error_message)
    logger.info("=" * 78)

    return 0 if overall_status in {"SUCCESS", "WARNING"} else 1


if __name__ == "__main__":
    raise SystemExit(run_load())

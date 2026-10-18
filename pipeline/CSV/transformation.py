"""
Paintelligent ETL - Transformation Stage

This script calls the existing extraction functions, transforms their returned
DataFrames, generates product_id and sales_id values during transformation,
writes transformed CSV outputs and quarantine.csv, and records run metadata.

Expected project layout:
    ETL-PIPELINE-G4/
        logs/
        pipeline/
            CSV/
                csv_extraction.py
                transformation.py
            .../
                analysis_extraction.py
        transformation/
            output/

Requirements:
    pip install pandas python-dotenv supabase

Notes:
    - product_id and sales_id are generated here; source IDs are not required.
    - Generated IDs are unique per record but will change on each run.
    - Sales records are matched to products using normalized product names.
    - Product stocks are permissive by project request: parse whole-number
      values as integers and preserve other nonblank stock labels as text.
    - The analysis extractor currently returns color_hex, rgb,
      paint_components, and analysis_date; this script adds a null id column
      if the source does not provide one.
"""

from __future__ import annotations

import sys
sys.dont_write_bytecode = True

import importlib.util
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent

if SCRIPT_DIR.name.lower() == "csv" and SCRIPT_DIR.parent.name.lower() == "pipeline":
    PROJECT_ROOT = SCRIPT_DIR.parent.parent
else:
    PROJECT_ROOT = SCRIPT_DIR.parent

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = PROJECT_ROOT / "transformation" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TRANSFORMATION_LOG = LOG_DIR / "transformation.log"
QUARANTINE_FILE = OUTPUT_DIR / "quarantine.csv"

CODE_VERSION = "1.4.0"
RULES_VERSION = "1.4.0"
LOG_RETENTION_DAYS = 30


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logger = logging.getLogger("paintelligent.transformation")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.propagate = False

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

file_handler = logging.FileHandler(TRANSFORMATION_LOG, mode="a", encoding="utf-8")
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.__stdout__)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


def timestamp() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def make_run_id() -> str:
    """Create a unique transformation run ID."""
    return f"TR-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"


def make_record_id(prefix: str) -> str:
    """Create a generated record ID, such as PROD-... or SALE-..."""
    return f"{prefix}-{uuid.uuid4().hex[:12].upper()}"


# ---------------------------------------------------------------------------
# FIND AND LOAD EXTRACTION SCRIPTS
# ---------------------------------------------------------------------------

def find_script(filename: str) -> Path:
    """Find an extraction script below the project root."""
    ignored = {".venv", "venv", "env", "node_modules", ".git", "__pycache__"}
    for path in PROJECT_ROOT.rglob(filename):
        if any(part in ignored for part in path.parts):
            continue
        if path.resolve() != Path(__file__).resolve():
            return path
    raise FileNotFoundError(
        f"Could not find {filename} under project root: {PROJECT_ROOT}. "
        "Check the project layout or update the script path."
    )


def load_module(module_name: str, path: Path):
    """Load a Python module from an explicit file path."""
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def get_extracted_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    """Run the existing extraction functions and return their DataFrames."""
    csv_script = find_script("csv_extraction.py")
    analysis_script = find_script("analysis_extraction.py")

    csv_module = load_module("paintelligent_csv_extraction", csv_script)
    analysis_module = load_module("paintelligent_analysis_extraction", analysis_script)

    products_df, sales_df = csv_module.extract_both_csvs()
    analysis_df = analysis_module.extract_specific_columns(
        "paint_analysis_history",
        ["color_hex", "rgb", "paint_components", "analysis_date"],
    )

    return products_df, sales_df, analysis_df, "NOT_RETURNED_BY_EXTRACTOR"


# ---------------------------------------------------------------------------
# COLUMN NORMALIZATION AND SCHEMAS
# ---------------------------------------------------------------------------

def normalize_column_name(value: Any) -> str:
    """Convert a source header to lowercase snake_case."""
    text = str(value).strip()
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()
    return text


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.columns = [normalize_column_name(column) for column in result.columns]
    return result


def rename_aliases(df: pd.DataFrame, aliases: dict[str, set[str]]) -> pd.DataFrame:
    """Map flexible source headers to canonical names using aliases and safe heuristics.

    Header matching is intentionally tolerant of punctuation, capitalization,
    abbreviations, and common wording variations. Unknown columns are preserved
    rather than guessed, and the selected mappings are logged for auditability.
    """
    result = normalize_columns(df)
    rename_map: dict[str, str] = {}
    claimed: set[str] = set()

    normalized_aliases = {
        canonical: {normalize_column_name(alias) for alias in names}
        for canonical, names in aliases.items()
    }

    def compact(value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    compact_aliases = {
        canonical: {compact(alias) for alias in names}
        for canonical, names in normalized_aliases.items()
    }

    # Exact normalized aliases first, then compact-equivalent headers such as
    # ProductName, Product_Name, or PRODUCT-NAME.
    for column in result.columns:
        matches = [
            canonical for canonical, names in normalized_aliases.items()
            if column in names or compact(column) in compact_aliases[canonical]
        ]
        available = [canonical for canonical in matches if canonical not in claimed]
        if len(available) == 1:
            canonical = available[0]
            rename_map[column] = canonical
            claimed.add(canonical)
        elif len(available) > 1:
            logger.warning(
                "Ambiguous source header '%s' could map to %s; leaving unmapped",
                column, available,
            )

    # Tolerant fallbacks for common real-world spreadsheet header variations.
    # These only run when an exact alias has not already claimed the field.
    for column in result.columns:
        if column in rename_map:
            continue
        c = compact(column)
        canonical = None

        if "product_id" in aliases and "product" in c and any(k in c for k in ("name", "description", "item", "title")):
            canonical = "product_name"
        elif "product" in aliases and "product" in c and any(k in c for k in ("name", "description", "item", "title")):
            canonical = "product"
        elif "brand" in aliases and any(k in c for k in ("brand", "manufacturer", "make")):
            canonical = "brand"
        elif "category" in aliases and any(k in c for k in ("category", "classification", "producttype")):
            canonical = "category"
        elif "sales_date" in aliases and any(k in c for k in ("salesdate", "transactiondate", "dateofsale", "datesold", "orderdate")):
            canonical = "sales_date"
        elif "analysis_date" in aliases and any(k in c for k in ("analysisdate", "createdat", "timestamp")):
            canonical = "analysis_date"
        elif "unit_purchase_price" in aliases and any(k in c for k in ("purchaseprice", "costprice", "unitcost", "buyingprice")):
            canonical = "unit_purchase_price"
        elif "estimated_price_php" in aliases and any(k in c for k in ("sellingprice", "retailprice", "estimatedprice", "unitprice", "pricephp", "srp")):
            canonical = "estimated_price_php"
        elif "stocks" in aliases and any(k in c for k in ("stock", "inventory", "onhand", "availableqty", "stockqty", "currentqty", "quantityavailable")):
            canonical = "stocks"
        elif "quantity" in aliases and any(k in c for k in ("quantity", "qty", "unitssold", "soldqty", "numberofunits")):
            canonical = "quantity"
        elif "unit_sold" in aliases and any(k in c for k in ("unitsold", "salesamount", "totalamount", "lineamount", "totalprice", "revenue", "amountsold", "transactionamount", "totalsales")):
            canonical = "unit_sold"
        elif "season" in aliases and any(k in c for k in ("season", "seasonality", "weatherseason")):
            canonical = "season"
        elif "color" in aliases and any(k in c for k in ("colorhex", "hexcolor", "colour", "hexcode")):
            canonical = "color"
        elif "rgb" in aliases and c in ("rgbvalue", "rgbcolor", "rgb"):
            canonical = "rgb"
        elif "paint_components" in aliases and any(k in c for k in ("paintcomponents", "components", "formulation", "mixture", "ingredients")):
            canonical = "paint_components"

        if canonical and canonical not in claimed:
            rename_map[column] = canonical
            claimed.add(canonical)

    if rename_map:
        logger.info("Flexible header mapping: %s", json.dumps(rename_map, ensure_ascii=False))
    unmatched = [column for column in result.columns if column not in rename_map]
    if unmatched:
        logger.info("Unmapped source headers retained for reference: %s", unmatched)

    # Avoid duplicate canonical columns if a source contains both an alias and
    # a variant of the same header. Prefer the first recognized source column;
    # a warning is emitted so potentially conflicting source values are reviewed.
    mapped = result.rename(columns=rename_map)
    if mapped.columns.duplicated().any():
        duplicate_names = mapped.columns[mapped.columns.duplicated()].tolist()
        logger.warning("Duplicate canonical headers detected after mapping: %s", duplicate_names)
        mapped = mapped.loc[:, ~mapped.columns.duplicated(keep="first")]
    return mapped


# Source product IDs and sales IDs are deliberately not mapped. The transformation
# stage creates new IDs, regardless of whether the source has ID columns.
PRODUCT_ALIASES = {
    "product_name": {"product_name", "product name", "productname", "product", "item", "item name", "product description", "item description", "description", "product title", "name of product", "item description name", "product label", "item description"},
    "unit_purchase_price": {"unit_purchase_price", "unit purchase price", "purchase price", "cost price", "unit cost", "buying price", "acquisition cost", "purchase cost", "cost per unit", "unit purchase cost"},
    "estimated_price_php": {
        "estimated_price_php", "estimated price (php)", "estimated price php",
        "estimated_price", "price", "unit price", "selling price", "retail price",
        "selling price php", "price php", "srp", "retail price php",
        "unit selling price", "selling price per unit", "estimated selling price", "selling price (php)",
    },
    "stocks": {"stocks", "stock", "inventory", "stock quantity", "quantity in stock", "current stock", "available stock", "on hand", "qty on hand", "available quantity", "quantity available", "stock on hand", "current inventory", "inventory quantity"},
    "category": {"category", "product category", "classification", "product type"},
    "brand": {"brand", "manufacturer", "make"},
}

SALES_ALIASES = {
    "brand": {"brand"},
    "product": {"product", "product_name", "product name", "item", "item name", "product description", "item description", "description", "product title"},
    "category": {"category", "product category", "classification", "product type"},
    "sales_date": {"sales_date", "sales date", "date", "transaction date", "date sold", "order date", "date of sale"},
    "quantity": {"quantity", "qty", "units sold", "sold quantity", "number of units", "quantity sold", "sales quantity", "qty sold", "units"},
    "unit_sold": {"unit_sold", "unit sold", "sales amount", "total amount", "line amount", "total price", "revenue", "sales revenue", "amount sold", "transaction amount", "total sales"},
    "season": {"season", "seasonality", "weather season"},
}

PAINT_ALIASES = {
    "id": {"id"},
    "color": {"color", "color_hex", "color hex", "hex color", "colour", "hex code", "color code"},
    "rgb": {"rgb"},
    "paint_components": {"paint_components", "paint components", "components", "formulation", "mixture", "ingredients", "paint formula"},
    "analysis_date": {"analysis_date", "analysis date", "created_at", "created at", "timestamp", "date analyzed", "date of analysis"},
}

PRODUCT_COLUMNS = [
    "product_id", "product_name", "unit_purchase_price",
    "estimated_price_php", "stocks", "category", "brand",
]
SALES_COLUMNS = [
    "sales_id", "brand", "product", "category",
    "sales_date", "quantity", "unit_sold", "season",
]
PAINT_COLUMNS = ["id", "color", "rgb", "paint_components", "analysis_date"]

QUARANTINE_COLUMNS = [
    "batch_id",
    "source_dataset",
    "source_row_number",
    "original_record",
    "rule_id",
    "quarantine_reason",
    "quarantine_timestamp",
]


# ---------------------------------------------------------------------------
# CLEANING AND TYPE CONVERSION HELPERS
# ---------------------------------------------------------------------------

def is_missing_scalar(value: Any) -> bool:
    """Safely detect scalar nulls without evaluating arrays as booleans."""
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, (list, tuple, dict, set)):
        return False
    try:
        result = pd.isna(value)
        return bool(result) if not hasattr(result, "__len__") else False
    except (TypeError, ValueError):
        return False


def is_blank(value: Any) -> bool:
    if is_missing_scalar(value):
        return True
    return isinstance(value, str) and value.strip() == ""


def clean_text(value: Any) -> Any:
    """Trim text while preserving structured values."""
    if is_missing_scalar(value):
        return pd.NA
    if isinstance(value, str):
        return value.strip()
    return value


def as_decimal(value: Any) -> Any:
    if is_missing_scalar(value) or (isinstance(value, str) and not value.strip()):
        return pd.NA
    cleaned = str(value).strip().replace(",", "").replace("₱", "").replace("$", "")
    try:
        number = Decimal(cleaned)
        return number if number.is_finite() else pd.NA
    except (InvalidOperation, ValueError):
        return pd.NA


def as_integer(value: Any) -> Any:
    number = as_decimal(value)
    if is_missing_scalar(number):
        return pd.NA
    if number != number.to_integral_value():
        return pd.NA
    return int(number)


def parse_date(value: Any, date_only: bool = False) -> Any:
    """Parse common date formats without raising on missing or invalid values."""
    if is_blank(value):
        return pd.NaT
    try:
        # Explicit formats make day/month and month/day source exports easier
        # to handle consistently. Fall back to pandas for ISO timestamps.
        if isinstance(value, str):
            text_value = value.strip()
            formats = (
                "%Y-%m-%d", "%Y-%m-%d %H:%M:%S",
                "%m/%d/%Y", "%m/%d/%Y %H:%M:%S",
                "%d/%m/%Y", "%d/%m/%Y %H:%M:%S",
                "%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y",
            )
            parsed = None
            for fmt in formats:
                try:
                    parsed = datetime.strptime(text_value, fmt)
                    break
                except ValueError:
                    continue
            if parsed is None:
                parsed = pd.to_datetime(text_value, errors="raise")
        else:
            parsed = pd.to_datetime(value, errors="raise")
        if date_only:
            return pd.Timestamp(parsed).date().isoformat()
        return pd.Timestamp(parsed).isoformat()
    except (ValueError, TypeError, OverflowError, pd.errors.OutOfBoundsDatetime):
        return pd.NaT


def validate_rgb(value: Any) -> bool:
    """Validate an RGB triple, accepting JSON, tuple/list, dictionary, or text."""
    if is_missing_scalar(value) or (isinstance(value, str) and not value.strip()):
        return True

    parsed = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            numbers = re.findall(r"-?\d+", value)
            if len(numbers) != 3:
                return False
            parsed = [int(number) for number in numbers]

    if isinstance(parsed, dict):
        lowered = {str(key).lower(): val for key, val in parsed.items()}
        if {"r", "g", "b"}.issubset(lowered):
            parsed = [lowered["r"], lowered["g"], lowered["b"]]
        else:
            return False

    if not isinstance(parsed, (list, tuple)) or len(parsed) != 3:
        return False

    try:
        values = [int(channel) for channel in parsed]
        return all(0 <= channel <= 255 for channel in values)
    except (TypeError, ValueError, OverflowError):
        return False


def validate_json_like(value: Any) -> bool:
    """Validate JSON-looking strings; preserve ordinary text and structured values."""
    if is_missing_scalar(value) or (isinstance(value, str) and not value.strip()):
        return True
    if not isinstance(value, str):
        return True
    text_value = value.strip()
    if not text_value.startswith(("{", "[")):
        return True
    try:
        json.loads(text_value)
        return True
    except json.JSONDecodeError:
        return False


def add_quarantine(
    quarantine: list[dict[str, Any]],
    batch_id: str,
    dataset: str,
    row_number: int,
    record: dict[str, Any],
    rule_id: str,
    reason: str,
) -> None:
    quarantine.append({
        "batch_id": batch_id,
        "source_dataset": dataset,
        "source_row_number": row_number,
        "original_record": json.dumps(record, ensure_ascii=False, default=str),
        "rule_id": rule_id,
        "quarantine_reason": reason,
        "quarantine_timestamp": timestamp(),
    })


def ensure_columns(df: pd.DataFrame, expected: list[str], dataset: str) -> pd.DataFrame:
    """Add missing output columns and keep only the documented schema."""
    result = df.copy()
    for column in expected:
        if column not in result.columns:
            result[column] = pd.NA
    return result[expected]


def normalize_product_key(value: Any) -> str:
    """Build a flexible product key for common catalog/sales name variations.

    Ignores case, spaces, punctuation, and a leading SKU/code such as ``DR-5``;
    expands common paint pack abbreviations (for example, GAL -> GALLON).
    Product descriptors and pack sizes are otherwise retained to reduce false
    matches between genuinely different products.
    """
    if is_blank(value):
        return ""

    text_value = str(value).strip().casefold()
    text_value = text_value.replace("₱", " ")
    text_value = re.sub(r"[^a-z0-9]+", " ", text_value)
    tokens = text_value.split()

    # Remove one leading SKU-like code (e.g. DR-5, ABC123, SKU-001) when the
    # remaining name still contains descriptive tokens.
    if len(tokens) > 1 and re.fullmatch(r"(?:[a-z]{1,6}\d+[a-z0-9]*|sku\d*)", tokens[0]):
        tokens = tokens[1:]
    elif (
        len(tokens) > 2
        and re.fullmatch(r"[a-z]{1,6}", tokens[0])
        and re.fullmatch(r"\d+[a-z0-9]*", tokens[1])
    ):
        # Hyphenated codes such as DR-5 become two tokens after punctuation
        # cleanup, so remove the pair together.
        tokens = tokens[2:]

    # Normalize common packaging abbreviations found in sales exports.
    aliases = {
        "gal": "gallon", "gals": "gallon", "gallon": "gallon", "gallons": "gallon",
        "ltr": "liter", "litre": "liter", "litres": "liter", "liters": "liter",
        "lt": "liter", "l": "liter",
        "kg": "kilogram", "kgs": "kilogram", "kilograms": "kilogram",
        "pc": "piece", "pcs": "piece", "pieces": "piece",
    }
    tokens = [aliases.get(token, token) for token in tokens]
    return "".join(tokens)


def values_match(left: Any, right: Any) -> bool:
    """Compare product names using conservative code/pack normalization.

    This is deterministic normalization, not fuzzy matching: sizes and product
    descriptors remain part of the key to avoid linking distinct products.
    """
    left_key = normalize_product_key(left)
    right_key = normalize_product_key(right)
    return bool(left_key) and left_key == right_key


def log_dataset_profile(dataset_name: str, frame: pd.DataFrame) -> None:
    """Log a lightweight source profile before transformation, as documented."""
    logger.info(
        "Source profile | dataset=%s | rows=%d | columns=%d | headers=%s",
        dataset_name, len(frame), len(frame.columns), list(map(str, frame.columns)),
    )
    if frame.empty:
        logger.info("Source profile | dataset=%s | dataset is empty", dataset_name)
        return

    missing_counts = {}
    for column in frame.columns:
        values = frame[column]
        missing = values.isna() | values.astype(str).str.strip().eq("")
        count = int(missing.sum())
        if count:
            missing_counts[str(column)] = count
    logger.info(
        "Source profile | dataset=%s | blank_or_missing_by_column=%s",
        dataset_name, json.dumps(missing_counts, ensure_ascii=False),
    )


# ---------------------------------------------------------------------------
# DATASET TRANSFORMATIONS
# ---------------------------------------------------------------------------

def transform_products(
    source: pd.DataFrame, batch_id: str, quarantine: list[dict[str, Any]]
) -> tuple[pd.DataFrame, dict[str, int], list[str]]:
    original = source.copy()
    df = rename_aliases(source, PRODUCT_ALIASES)
    # product_id is generated here and never read from the source.
    df = df.drop(columns=["product_id"], errors="ignore")
    df = ensure_columns(df, PRODUCT_COLUMNS, "products")

    # Generate every product_id before scanning rows for cleaning and validation.
    df["product_id"] = [make_record_id("PROD") for _ in range(len(df))]

    read_count = len(df)
    modified_count = 0
    kept_rows = []

    for position, (_, row) in enumerate(df.iterrows()):
        original_record = original.iloc[position].to_dict() if position < len(original) else row.to_dict()
        row_data = row.to_dict()
        source_row = position + 2
        row_modified = False
        rejection = None

        for column in ["product_name", "category", "brand"]:
            value = row_data.get(column)
            cleaned = clean_text(value)
            if not (is_missing_scalar(value) and is_missing_scalar(cleaned)) and str(value) != str(cleaned):
                row_modified = True
            row_data[column] = cleaned

        if is_blank(row_data["product_name"]):
            rejection = ("C-02", "Missing required product name")

        for column in ["unit_purchase_price", "estimated_price_php"]:
            raw = row_data[column]
            converted = as_decimal(raw)
            if not is_missing_scalar(raw) and not is_missing_scalar(converted) and str(raw) != str(converted):
                row_modified = True
            if not is_blank(raw) and is_missing_scalar(converted):
                rejection = rejection or ("C-03", f"Invalid decimal value in {column}")
            row_data[column] = converted

        # STOCKS IS PERMISSIVE: keep any supplied value instead of rejecting
        # the product row when stock is non-numeric (e.g. "N/A", "Out of stock",
        # "10 pcs", or other source-specific labels). Clean numeric values to
        # integers when possible; otherwise preserve the original trimmed value.
        raw_stocks = row_data["stocks"]
        cleaned_stocks = clean_text(raw_stocks)
        converted_stocks = as_integer(cleaned_stocks)
        if not is_blank(cleaned_stocks) and not is_missing_scalar(converted_stocks):
            if str(cleaned_stocks) != str(converted_stocks):
                row_modified = True
            row_data["stocks"] = converted_stocks
        else:
            if not is_blank(cleaned_stocks) and str(raw_stocks) != str(cleaned_stocks):
                row_modified = True
            row_data["stocks"] = cleaned_stocks

        for column in ["unit_purchase_price", "estimated_price_php"]:
            value = row_data[column]
            if not is_missing_scalar(value) and value < 0:
                rejection = rejection or ("C-03", f"Negative value in {column}")

        if rejection:
            add_quarantine(quarantine, batch_id, "products", source_row, original_record, *rejection)
            continue

        kept_rows.append(row_data)
        if row_modified:
            modified_count += 1

    result = pd.DataFrame(kept_rows, columns=PRODUCT_COLUMNS)
    result = result.astype(object).where(pd.notna(result), None)
    stats = {
        "records_read": read_count,
        "records_output": len(result),
        "records_rejected": read_count - len(result),
        "records_modified": modified_count,
    }
    return result, stats, ["C-01", "C-02", "C-03"]


def transform_sales(
    source: pd.DataFrame,
    products: pd.DataFrame,
    batch_id: str,
    quarantine: list[dict[str, Any]],
) -> tuple[pd.DataFrame, dict[str, int], list[str]]:
    original = source.copy()
    df = rename_aliases(source, SALES_ALIASES)
    # sales_id is generated here and never read from the source.
    df = df.drop(columns=["sales_id"], errors="ignore")
    df = ensure_columns(df, SALES_COLUMNS, "sales")

    # Generate every sales_id before scanning rows for cleaning and validation.
    df["sales_id"] = [make_record_id("SALE") for _ in range(len(df))]

    read_count = len(df)
    modified_count = 0
    kept_rows = []

    product_names = []
    product_name_by_key: dict[str, str] = {}
    ambiguous_product_keys: set[str] = set()
    if not products.empty and "product_name" in products.columns:
        product_names = [
            str(name).strip() for name in products["product_name"].tolist()
            if not is_blank(name)
        ]
        # Use only unambiguous normalized names to avoid linking a sale to the
        # wrong product when two catalog names normalize to the same key.
        for product_name in product_names:
            key = normalize_product_key(product_name)
            if not key:
                continue
            if key in product_name_by_key and product_name_by_key[key] != product_name:
                ambiguous_product_keys.add(key)
            else:
                product_name_by_key[key] = product_name
        for key in ambiguous_product_keys:
            product_name_by_key.pop(key, None)

    for position, (_, row) in enumerate(df.iterrows()):
        original_record = original.iloc[position].to_dict() if position < len(original) else row.to_dict()
        row_data = row.to_dict()
        source_row = position + 2
        row_modified = False
        rejection = None

        for column in ["brand", "product", "category", "season"]:
            value = row_data.get(column)
            cleaned = clean_text(value)
            if not (is_missing_scalar(value) and is_missing_scalar(cleaned)) and str(value) != str(cleaned):
                row_modified = True
            row_data[column] = cleaned

        if is_blank(row_data["product"]):
            rejection = ("C-02", "Missing product reference")
        elif product_names:
            product_key = normalize_product_key(row_data["product"])
            canonical_name = product_name_by_key.get(product_key)
            if canonical_name:
                # Store the catalog's exact spelling for consistent downstream joins.
                if str(row_data["product"]).strip() != canonical_name:
                    row_modified = True
                row_data["product"] = canonical_name
            elif product_key in ambiguous_product_keys:
                rejection = ("C-05", "Sales product matches multiple Product records after normalization")
            else:
                rejection = ("C-05", "Sales product does not match a Product record")

        raw_quantity = row_data["quantity"]
        converted_quantity = as_integer(raw_quantity)
        if not is_missing_scalar(raw_quantity) and not is_missing_scalar(converted_quantity) and str(raw_quantity) != str(converted_quantity):
            row_modified = True
        if not is_blank(raw_quantity) and is_missing_scalar(converted_quantity):
            rejection = rejection or ("C-03", "Invalid integer value in quantity")
        if not is_missing_scalar(converted_quantity) and converted_quantity < 0:
            rejection = rejection or ("C-03", "Negative value in quantity")
        row_data["quantity"] = converted_quantity

        raw_unit_sold = row_data["unit_sold"]
        converted_unit_sold = as_decimal(raw_unit_sold)
        if not is_missing_scalar(raw_unit_sold) and not is_missing_scalar(converted_unit_sold) and str(raw_unit_sold) != str(converted_unit_sold):
            row_modified = True
        if not is_blank(raw_unit_sold) and is_missing_scalar(converted_unit_sold):
            rejection = rejection or ("C-03", "Invalid decimal value in unit_sold")
        if not is_missing_scalar(converted_unit_sold) and converted_unit_sold < 0:
            rejection = rejection or ("C-03", "Negative value in unit_sold")
        row_data["unit_sold"] = converted_unit_sold

        raw_date = row_data["sales_date"]
        converted_date = parse_date(raw_date, date_only=True)
        if not is_missing_scalar(raw_date) and not is_missing_scalar(converted_date) and str(raw_date) != str(converted_date):
            row_modified = True
        if not is_blank(raw_date) and is_missing_scalar(converted_date):
            rejection = rejection or ("C-06", "Invalid sales_date")
        row_data["sales_date"] = converted_date

        if rejection:
            add_quarantine(quarantine, batch_id, "sales", source_row, original_record, *rejection)
            continue

        kept_rows.append(row_data)
        if row_modified:
            modified_count += 1

    result = pd.DataFrame(kept_rows, columns=SALES_COLUMNS)
    result = result.astype(object).where(pd.notna(result), None)
    stats = {
        "records_read": read_count,
        "records_output": len(result),
        "records_rejected": read_count - len(result),
        "records_modified": modified_count,
    }
    return result, stats, ["C-01", "C-02", "C-03", "C-05", "C-06"]


def transform_paint_analysis(
    source: pd.DataFrame, batch_id: str, quarantine: list[dict[str, Any]]
) -> tuple[pd.DataFrame, dict[str, int], list[str]]:
    original = source.copy()
    df = rename_aliases(source, PAINT_ALIASES)
    df = ensure_columns(df, PAINT_COLUMNS, "paint_analysis")

    read_count = len(df)
    modified_count = 0
    kept_rows = []
    seen_ids: set[str] = set()

    for position, (_, row) in enumerate(df.iterrows()):
        original_record = original.iloc[position].to_dict() if position < len(original) else row.to_dict()
        row_data = row.to_dict()
        source_row = position + 2
        row_modified = False
        rejection = None

        for column in ["id", "color", "rgb", "paint_components"]:
            value = row_data.get(column)
            cleaned = clean_text(value)
            if not (is_missing_scalar(value) and is_missing_scalar(cleaned)) and str(value) != str(cleaned):
                row_modified = True
            row_data[column] = cleaned

        if "id" in df.columns and not is_blank(row_data["id"]):
            identifier = str(row_data["id"])
            if identifier in seen_ids:
                rejection = ("C-04", "Duplicate paint analysis id")
            else:
                seen_ids.add(identifier)

        if not is_blank(row_data["rgb"]) and not validate_rgb(row_data["rgb"]):
            rejection = rejection or ("C-07", "Malformed RGB value or channel outside 0-255")

        if not is_blank(row_data["paint_components"]) and not validate_json_like(row_data["paint_components"]):
            rejection = rejection or ("C-08", "Malformed JSON in paint_components")

        raw_date = row_data["analysis_date"]
        converted_date = parse_date(raw_date, date_only=False)
        if not is_missing_scalar(raw_date) and not is_missing_scalar(converted_date) and str(raw_date) != str(converted_date):
            row_modified = True
        if not is_blank(raw_date) and is_missing_scalar(converted_date):
            rejection = rejection or ("C-06", "Invalid analysis_date")
        row_data["analysis_date"] = converted_date

        if rejection:
            add_quarantine(quarantine, batch_id, "paint_analysis", source_row, original_record, *rejection)
            continue

        kept_rows.append(row_data)
        if row_modified:
            modified_count += 1

    result = pd.DataFrame(kept_rows, columns=PAINT_COLUMNS)
    result = result.astype(object).where(pd.notna(result), None)
    stats = {
        "records_read": read_count,
        "records_output": len(result),
        "records_rejected": read_count - len(result),
        "records_modified": modified_count,
    }
    return result, stats, ["C-01", "C-04", "C-06", "C-07", "C-08"]


# ---------------------------------------------------------------------------
# MAIN TRANSFORMATION RUN
# ---------------------------------------------------------------------------

def run_transformation() -> int:
    run_id = make_run_id()
    batch_id = f"BATCH-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"
    start_clock = time.perf_counter()
    start_ts = timestamp()

    datasets = {
        name: {
            "records_read": 0,
            "records_output": 0,
            "records_rejected": 0,
            "records_modified": 0,
        }
        for name in ("products", "sales", "paint_analysis")
    }
    validation_results: dict[str, str] = {}
    rules_applied: dict[str, list[str]] = {}
    errors: list[str] = []
    quarantine: list[dict[str, Any]] = []
    overall_status = "FAILED"
    parent_run_id = "NOT_RETURNED_BY_EXTRACTOR"

    logger.info("=" * 78)
    logger.info("PAINTELLIGENT TRANSFORMATION REPORT")
    logger.info("Run ID: %s", run_id)
    logger.info("Batch ID: %s", batch_id)
    logger.info("Start Timestamp: %s", start_ts)
    logger.info("Code Version: %s | Rules Version: %s", CODE_VERSION, RULES_VERSION)

    output_schemas = {
        "products_transformed.csv": PRODUCT_COLUMNS,
        "sales_transformed.csv": SALES_COLUMNS,
        "paint_analysis_transformed.csv": PAINT_COLUMNS,
    }
    for filename, columns in output_schemas.items():
        pd.DataFrame(columns=columns).to_csv(
            OUTPUT_DIR / filename, index=False, encoding="utf-8-sig"
        )
    pd.DataFrame(columns=QUARANTINE_COLUMNS).to_csv(
        QUARANTINE_FILE, index=False, encoding="utf-8-sig"
    )

    try:
        products_raw, sales_raw, paint_raw, parent_run_id = get_extracted_data()

        logger.info(
            "Extracted DataFrames received: products=%s, sales=%s, paint_analysis=%s",
            len(products_raw), len(sales_raw), len(paint_raw),
        )

        # Profile the raw inputs before cleaning so source problems can be
        # distinguished from transformation problems in the run log.
        log_dataset_profile("products", products_raw)
        log_dataset_profile("sales", sales_raw)
        log_dataset_profile("paint_analysis", paint_raw)

        products, datasets["products"], rules_applied["products"] = transform_products(
            products_raw, batch_id, quarantine
        )
        sales, datasets["sales"], rules_applied["sales"] = transform_sales(
            sales_raw, products, batch_id, quarantine
        )
        paint, datasets["paint_analysis"], rules_applied["paint_analysis"] = transform_paint_analysis(
            paint_raw, batch_id, quarantine
        )

        for name, frame, expected_columns in [
            ("products", products, PRODUCT_COLUMNS),
            ("sales", sales, SALES_COLUMNS),
            ("paint_analysis", paint, PAINT_COLUMNS),
        ]:
            schema_ok = list(frame.columns) == expected_columns
            stats = datasets[name]
            reconciliation_ok = (
                stats["records_read"] == stats["records_output"] + stats["records_rejected"]
            )
            validation_results[f"{name}.schema"] = "PASS" if schema_ok else "FAIL"
            validation_results[f"{name}.record_reconciliation"] = (
                "PASS" if reconciliation_ok else "FAIL"
            )
            if not schema_ok:
                errors.append(f"{name}: output schema mismatch")
            if not reconciliation_ok:
                errors.append(f"{name}: record reconciliation failed")

        # Check generated IDs are populated and unique within this output.
        for name, frame, id_column in [
            ("products", products, "product_id"),
            ("sales", sales, "sales_id"),
        ]:
            ids = frame[id_column].dropna().astype(str)
            ids_ok = bool((ids.str.strip() != "").all()) and ids.is_unique
            validation_results[f"{name}.generated_ids"] = "PASS" if ids_ok else "FAIL"
            if not ids_ok:
                errors.append(f"{name}: generated IDs are missing or duplicated")

        products.to_csv(
            OUTPUT_DIR / "products_transformed.csv", index=False, encoding="utf-8-sig"
        )
        sales.to_csv(
            OUTPUT_DIR / "sales_transformed.csv", index=False, encoding="utf-8-sig"
        )
        paint.to_csv(
            OUTPUT_DIR / "paint_analysis_transformed.csv", index=False, encoding="utf-8-sig"
        )
        pd.DataFrame(quarantine, columns=QUARANTINE_COLUMNS).to_csv(
            QUARANTINE_FILE, index=False, encoding="utf-8-sig"
        )

        # Log the resulting schemas and a small profile of the actual outputs.
        log_dataset_profile("products_transformed", products)
        log_dataset_profile("sales_transformed", sales)
        log_dataset_profile("paint_analysis_transformed", paint)
        logger.info(
            "Transformation operations applied: canonical header mapping; "
            "text trimming; decimal conversion for prices/sales amounts; "
            "integer conversion for sales quantity; permissive stock cleaning "
            "(numeric stock values become integers, other nonblank values are "
            "preserved); date standardization; normalized product-reference "
            "matching; RGB and JSON-like component validation; generated "
            "Product/Sales IDs; rejected records written to quarantine."
        )

        rejected_total = sum(item["records_rejected"] for item in datasets.values())
        read_total = sum(item["records_read"] for item in datasets.values())
        rejection_rate = rejected_total / read_total * 100 if read_total else 0.0
        validation_results["rejection_rate"] = (
            "PASS" if read_total > 0 and rejection_rate <= 1.0
            else "WARNING" if read_total > 0
            else "FAIL (no input records)"
        )

        critical_failures = any(
            value == "FAIL"
            for key, value in validation_results.items()
            if key.endswith(".schema")
            or key.endswith(".record_reconciliation")
            or key.endswith(".generated_ids")
        )
        if critical_failures:
            overall_status = "FAILED"
        elif read_total == 0:
            overall_status = "FAILED"
            errors.append("No input records were read; automatic approval is not allowed.")
        elif rejection_rate > 1.0:
            overall_status = "MANUAL_REVIEW"
        elif rejected_total > 0:
            overall_status = "SUCCESS_WITH_QUARANTINE"
        else:
            overall_status = "SUCCESS"

        logger.info("Dataset Summary:")
        for name, stats in datasets.items():
            logger.info(
                "  %s | read=%d | output=%d | modified=%d | rejected=%d",
                name, stats["records_read"], stats["records_output"],
                stats["records_modified"], stats["records_rejected"],
            )
            logger.info("  %s rules applied: %s", name, ", ".join(rules_applied[name]))

        logger.info("Validation Results: %s", json.dumps(validation_results, ensure_ascii=False))
        logger.info("Quarantine file: %s (%d records)", QUARANTINE_FILE, len(quarantine))

        # Explain the rejection rate by dataset and rule, not only as one total.
        if quarantine:
            quarantine_summary = pd.DataFrame(quarantine).groupby(
                ["source_dataset", "rule_id", "quarantine_reason"], dropna=False
            ).size().reset_index(name="count")
            for _, item in quarantine_summary.iterrows():
                logger.warning(
                    "Rejection breakdown | dataset=%s | rule=%s | count=%d | reason=%s",
                    item["source_dataset"], item["rule_id"], int(item["count"]),
                    item["quarantine_reason"],
                )
        else:
            logger.info("Rejection breakdown: no records quarantined")

        logger.info("Rejection Rate: %.3f%%", rejection_rate)
        logger.info("Transformation Status: %s", overall_status)

    except Exception as exc:
        overall_status = "FAILED"
        short_error = f"{type(exc).__name__}: {exc}"
        errors.append(short_error)
        logger.exception("Transformation stopped: %s", short_error)

        try:
            quarantine_df = pd.DataFrame(quarantine, columns=QUARANTINE_COLUMNS)
            quarantine_df.to_csv(QUARANTINE_FILE, index=False, encoding="utf-8-sig")
            logger.info(
                "Quarantine file updated after failure: %s (%d records)",
                QUARANTINE_FILE, len(quarantine_df),
            )
            for _, quarantine_row in quarantine_df.iterrows():
                logger.info(
                    "Dataset: %s | Source row: %s | Rule ID: %s | Reason: %s",
                    quarantine_row["source_dataset"],
                    quarantine_row["source_row_number"],
                    quarantine_row["rule_id"],
                    quarantine_row["quarantine_reason"],
                )
        except Exception:
            logger.exception("Could not write quarantine.csv after failure")

    end_ts = timestamp()
    duration_seconds = round(time.perf_counter() - start_clock, 3)
    total_read = sum(item["records_read"] for item in datasets.values())
    total_output = sum(item["records_output"] for item in datasets.values())
    total_rejected = sum(item["records_rejected"] for item in datasets.values())
    total_modified = sum(item["records_modified"] for item in datasets.values())

    metadata = {
        "run_id": run_id,
        "parent_run_id": parent_run_id,
        "batch_id": batch_id,
        "batch_window": "Full available source data",
        "table_name": ["products", "sales", "paint_analysis"],
        "start_timestamp": start_ts,
        "end_timestamp": end_ts,
        "status": overall_status,
        "records_read": total_read,
        "records_output": total_output,
        "records_rejected": total_rejected,
        "records_modified": total_modified,
        "dataset_metrics": datasets,
        "rules_applied": rules_applied,
        "validation_results": validation_results,
        "error_message": " | ".join(errors) if errors else "N/A",
        "error_code": "TRANSFORMATION_FAILED" if errors and overall_status == "FAILED" else "N/A",
        "duration_seconds": duration_seconds,
        "code_version": CODE_VERSION,
        "rules_version": RULES_VERSION,
        "log_retention_days": LOG_RETENTION_DAYS,
        "transformation_log": str(TRANSFORMATION_LOG),
        "quarantine_file": str(QUARANTINE_FILE),
    }

    logger.info("Summary:")
    logger.info("  Status: %s", overall_status)
    logger.info(
        "  Records: read=%d, output=%d, modified=%d, rejected=%d",
        total_read, total_output, total_modified, total_rejected,
    )
    for name, stats in datasets.items():
        logger.info(
            "  %s: read=%d, output=%d, modified=%d, rejected=%d",
            name, stats["records_read"], stats["records_output"],
            stats["records_modified"], stats["records_rejected"],
        )
    if validation_results:
        failed_checks = [
            key for key, value in validation_results.items() if value != "PASS"
        ]
        logger.info(
            "  Validation: %s",
            "All checks passed" if not failed_checks else ", ".join(failed_checks),
        )
    logger.info("  Quarantined records: %d", len(quarantine))
    logger.info("  Quarantine file: %s", QUARANTINE_FILE)
    logger.info("  Parent run: %s", metadata["parent_run_id"])
    logger.info("  Batch window: %s", metadata["batch_window"])
    logger.info("  Code/rules version: %s/%s", CODE_VERSION, RULES_VERSION)
    logger.info("  Log retention policy: %d days", LOG_RETENTION_DAYS)
    logger.info("  Finished: %s (%.2f seconds)", end_ts, duration_seconds)
    if errors:
        logger.info("  Issue: %s", " | ".join(errors))
    logger.info("=" * 60)

    return 0 if overall_status in {"SUCCESS", "SUCCESS_WITH_QUARANTINE"} else 1


if __name__ == "__main__":
    raise SystemExit(run_transformation())

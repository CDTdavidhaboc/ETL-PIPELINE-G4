
# Stage 3: Data Load Documentation (Lab 5)

## Objective

The goal of Stage 3 (Data Load) is to load the validated, transformed dataset handed over from the Transformation Stage (`pipeline/CSV/transformation.py`) into the target Paintelligent SQLite analytical database (`load/paintelligent.db`), and to confirm that the loaded data is complete, accurate, traceable, and ready for use by business reports, SQL queries, dashboards, and inventory/sales analysis.

This pipeline is **batch only**. Data is loaded in scheduled, bounded batches following the completion of the Transformation Stage. Streaming, micro-batch, and real-time loading are out of scope.

> **Requirement Compliance:** The Load Stage uses **only** the validated data files and metadata handed over from the Transformation Stage (`transformation/output/` and `logs/transformation.log`). It does **not** re-extract data from Supabase or raw CSVs, and it does **not** perform new cleaning or business transformations.

---

## 0. Codebase Analysis and Lab 5 Justification (Current Project vs. Added Load Stage)

Before implementing Stage 3, the existing `ETL-PIPELINE-G4` repository was analyzed to ensure full architectural continuity across Labs 2, 3, 4, and 5.

### 0.1 What Was Already Present in the Project

| Project Component | Existing File(s) | Analysis of Current Implementation |
|---|---|---|
| **Lab 2 Technical Metadata** | `README.md` | Defines the six-stage Paintelligent ETL architecture using **Python** for orchestration and **SQLite** for storage and SQL-based analysis (`product_table`, `sales_table`, and `paint_analysis_table`). |
| **Stage 1: Extraction** | `pipeline/CSV/csv_extraction.py`, `pipeline/analysis_extraction.py`, `docs/extraction/extraction.md`, `logs/csv_extraction.log`, `logs/analysis_extraction.log` | Extracts client CSVs (`All Products - Cleaned.csv` with 579 rows; `GPC_Sales - make equal.csv` with 796 rows) and queries Supabase PostgreSQL (`paint_analysis_history` with 7 rows), writing audit reports to `logs/`. |
| **Stage 2: Transformation** | `pipeline/CSV/transformation.py`, `docs/transformation/transformation.md`, `logs/transformation.log` | Normalizes headers via flexible alias mapping, trims text, validates decimals/integers/dates/RGB/JSON, matches `sales` product names to the `products` catalog, generates run-specific `PROD-...` and `SALE-...` IDs, writes quarantine records (`quarantine.csv`), and logs run metrics (`Run ID: TR-20261018-F8AA3D`, `Batch ID: BATCH-20261018-0B474A`, `Status: SUCCESS`). |
| **Transformation Handover Outputs** | `transformation/output/products_transformed.csv` (579 rows), `sales_transformed.csv` (796 rows), `paint_analysis_transformed.csv` (7 rows), `quarantine.csv` (0 rows) | Clean, standardized CSV outputs waiting to be loaded into SQLite. Total accepted records = **1,382 rows** (`579 + 796 + 7`), with **0 quarantined records** (`0.000%` rejection rate). |

### 0.2 Code-Level Findings That Justify the Stage 3 Load Design

1. **Run-Specific Generated Identifiers (`PROD-...` and `SALE-...`):**
   In `transformation.py` (lines 112–114, 589, 674), `product_id` and `sales_id` are generated using random UUIDs (`uuid.uuid4().hex[:12].upper()`) on every transformation run. Because these IDs change across reruns of the transformation script, the Load Stage cannot rely solely on `product_id` or `sales_id` to prevent duplicates across reruns. Therefore:
   - `product_table` uses an auto-incrementing warehouse surrogate key (`product_key`) and matches existing catalog items on their natural attributes (`product_name`, `unit_purchase_price`, `estimated_price_php`, `category`, `brand`) for **SCD Type 1 Upsert**.
   - `sales_table` uses **Delete-by-Batch-ID then Insert** (`DELETE FROM sales_table WHERE batch_id = ?`) and resolves `product_key` via foreign-key lookup against `product_table(product_name)`.
2. **Duplicate Product Name with Distinct Price Tiers (`TEST 1`):**
   In `products_transformed.csv` (rows 577 and 578), the product name `TEST 1` appears twice with two different `estimated_price_php` values (`750.00` and `1000.00`). Including price and category/brand in the dimension match key preserves all **579** handed-over product rows without data loss while still guaranteeing idempotency when a batch is rerun.
3. **Permissive Stock Labels in `products_transformed.csv`:**
   In `transformation.py` (lines 621–635), `stocks` is intentionally permissive and retains unit labels and placeholders such as `'12.00 Pieces'`, `'6.00 Gallon'`, `'--'`, and `'120'`. Accordingly, `product_table.stocks` is typed as `TEXT` in SQLite so no handover rows are rejected or altered during loading.
4. **Null Source `id` in `paint_analysis_transformed.csv`:**
   In `transformation.py` (lines 155–158), `extract_specific_columns` requests `["color_hex", "rgb", "paint_components", "analysis_date"]` and fills `id` with `None`. To guarantee uniqueness and rerun idempotency without modifying the business data, `load.py` assigns a surrogate key (`analysis_key`) and computes a deterministic `record_hash` (`SHA-256` over `id|color|rgb|paint_components|analysis_date`).
5. **Inactive Product Records in Catalog:**
   Several products in `products_transformed.csv` carry `'Inactive'` in `product_name` (e.g., `1L Custom Paint #2 Inactive`, `DGM Paint Brush 1 in Inactive`). Without altering `product_name`, the Load Stage sets a boolean flag `is_active` (`0` for inactive, `1` for active) to support soft-delete filtering in downstream reports.

### 0.3 What Was Added to the Project for Lab 5

| Added File / Artifact | Purpose |
|---|---|
| `pipeline/CSV/load.py` & `pipeline/load.py` | Stage 3 Data Load script implementing Pre-Load Validation, atomic SQLite batch transactions, parent-before-child table loading, Post-Load Validation & Reconciliation, database snapshots, and operational/audit logging. |
| `transformation/output/handover_metadata.json` (in `transformation.py`) | Added JSON handover serialization of the existing `metadata` dictionary in `transformation.py` so `load.py` can read either `handover_metadata.json` or parse `logs/transformation.log` directly. |
| `load/paintelligent.db` & `load/paintelligent_readable.sql` | Target SQLite data warehouse and human-readable SQL dump containing `product_table`, `sales_table`, `paint_analysis_table`, `load_log`, and analytical views (`dim_product`, `fact_sales`, `fact_paint_analysis`). |
| `load/load_metadata.json` & `logs/load.log` | Structured JSON metadata and concise, non-repetitive operational execution logs for Stage 3. |
| `load/rejected/load_rejected.csv` & `load/backups/` | Storage for load-rejected records (if any) and single-file-per-batch pre-load SQLite database backup snapshots (`paintelligent_<batch_id>.db`). |
| `docs/load/load.md` | This comprehensive Stage 3 Data Load Documentation answering all Lab 5 requirements. |

---

# 1. Load Source and Target Specification

## 1.1 Load Input

The Load Stage consumes only the validated output files and run metadata produced by the Transformation Stage (`pipeline/CSV/transformation.py`):

| Input Artifact | Location | Format | Latest Handover Metrics |
|---|---|---|---|
| **Transformed Products** | `transformation/output/products_transformed.csv` | UTF-8-SIG CSV | **579 records** (`product_id`, `product_name`, `unit_purchase_price`, `estimated_price_php`, `stocks`, `category`, `brand`) |
| **Transformed Sales** | `transformation/output/sales_transformed.csv` | UTF-8-SIG CSV | **796 records** (`sales_id`, `brand`, `product`, `category`, `sales_date`, `quantity`, `unit_sold`, `season`) |
| **Transformed Paint Analysis** | `transformation/output/paint_analysis_transformed.csv` | UTF-8-SIG CSV | **7 records** (`id`, `color`, `rgb`, `paint_components`, `analysis_date`) |
| **Transformation Quarantine** | `transformation/output/quarantine.csv` | UTF-8-SIG CSV | **0 records** (excluded during transformation; not loaded into target tables) |
| **Handover Metadata / Log** | `logs/transformation.log` & `transformation/output/handover_metadata.json` | Log / JSON | Links `transformation_run_id`, `batch_id`, `status`, and expected output row counts |

- **Transformation Run ID Loaded:** `TR-20261018-F8AA3D`
- **Batch ID Loaded:** `BATCH-20261018-0B474A`
- **Batch Window Covered:** `2025-01-01 to 2025-12-31` (Sales transaction window; Paint Analysis covers `2026-09-03` to `2026-09-13`; Products represent the full current catalog).

## 1.2 Target System

- **Target Name and Purpose:** **Paintelligent Analytical Data Warehouse (`paintelligent.db`)** — A dedicated local analytical and reporting database supporting Garcia Paint Center's business intelligence queries, including top-selling products, slow-moving inventory, seasonal sales trends, and paint formulation usage.
- **Database Management System (DBMS):** **SQLite 3** (configured with `PRAGMA foreign_keys = ON` for relational integrity and `PRAGMA journal_mode = WAL` for write-ahead logging concurrency and crash recovery).
- **Target Database / Schema Location:** `load/paintelligent.db` (default `main` schema, exposing core tables `product_table`, `sales_table`, `paint_analysis_table`, `load_log`, reporting views `dim_product`, `fact_sales`, `fact_paint_analysis`, and readable SQL export `load/paintelligent_readable.sql`).

## 1.3 Load Method

- **Load Tool / Technology:** Python 3 (`pipeline/CSV/load.py` / `pipeline/load.py`) using the standard `sqlite3` transactional driver and `pandas` for reading the transformed handover CSV files.
- **Batch Frequency and Schedule:** **Batch-only execution**, triggered immediately after `transformation.py` completes with an approved status (`SUCCESS` or `SUCCESS_WITH_QUARANTINE`), or on a scheduled daily off-peak batch run at **04:00 local time**.
- **How Data Is Written:**
  - All three target tables are written inside a single atomic SQLite transaction (`BEGIN IMMEDIATE ... COMMIT`).
  - `product_table` uses **Incremental Upsert (SCD Type 1)** matching on natural catalog attributes (`product_name`, `unit_purchase_price`, `estimated_price_php`, `category`, `brand`).
  - `sales_table` uses **Idempotent Batch Replace (Delete by `batch_id` first, then parameterized batch Insert)** while resolving `product_key` foreign keys against `product_table`.
  - `paint_analysis_table` uses **Incremental Upsert (SCD Type 1)** matching on `record_hash` (`SHA-256` of the record's canonical fields).

## 1.4 Load Scope

- **Load Order:**
  1. `product_table` (**Parent Dimension Table** — loaded first so every product has a valid surrogate key `product_key`).
  2. `sales_table` (**Child Fact Table** — loaded second because `sales_table.product_key` references `product_table.product_key`).
  3. `paint_analysis_table` (**Analytical Fact / Event Table** — loaded third; independent of `sales_table`).
  4. `load_log` (**Operational & Audit Metadata Table** — updated immediately after transaction commit or rollback).
- **Expected Volume and Execution Time Window:**
  - Expected records per batch: **~1,382 total records** (`579` products, `796` sales, `7` paint analysis records; scales up to 50,000 records per batch).
  - Maximum allowed load window: **5 minutes (300 seconds)** during the off-peak window (`04:00–04:05`). Actual measured load time for the 1,382-record batch is **~0.13 seconds**.

## 1.5 Limitations and Assumptions

- **Single-Writer Concurrency Limitation:** SQLite supports multiple concurrent readers but only one active writer at a time (`BEGIN IMMEDIATE`). Running heavy external write operations on `load/paintelligent.db` during the batch window can cause lock contention (`sqlite3.OperationalError: database is locked`). A 30-second connection timeout and WAL mode (`PRAGMA journal_mode = WAL`) are configured to mitigate reader/writer blocking.
- **Storage & File-System Access:** Because SQLite is a file-based RDBMS, the process requires OS-level read/write/create permissions on the `load/` and `logs/` directories (to create `.db`, `.db-wal`, `.db-shm`, `.sql`, and backup snapshots) and at least **50 MB** of free disk space.
- **Loading Account / Role and Privileges:** Executed under the local **ETL Pipeline Service / Authorized Developer OS Account** with:
  - **Read-only (`r`)** access to `transformation/output/` and `logs/transformation.log`.
  - **Read/Write/Create (`rwc`)** access to `load/paintelligent.db`, `load/paintelligent_readable.sql`, `load/backups/`, `load/rejected/`, and `logs/load.log`.
  - No database passwords, API keys, or plaintext secrets are used by the SQLite target or stored in documentation.

---

# 2. Target Tables and Column Specification

## 2.1 Target Tables Overview

| Table | Type | Primary / Surrogate Key | Foreign Keys | Key Constraints | Purpose |
|---|---|---|---|---|---|
| `product_table` *(View: `dim_product`)* | Dimension | `product_key` (`INTEGER PRIMARY KEY AUTOINCREMENT`) | None | `product_id NOT NULL UNIQUE`; `product_name NOT NULL`; `unit_purchase_price >= 0`; `estimated_price_php >= 0`; `is_active IN (0, 1)` | Stores cleaned product catalog, pricing, stock labels, and active status for reporting and dimension joins. |
| `sales_table` *(View: `fact_sales`)* | Fact | `sales_key` (`INTEGER PRIMARY KEY AUTOINCREMENT`) | `product_key` → `product_table(product_key)` | `sales_id NOT NULL UNIQUE`; `product_key NOT NULL`; `product NOT NULL`; `sales_date NOT NULL`; `quantity >= 0`; `unit_sold >= 0` | Stores validated sales transactions linked to `product_table` for revenue, volume, and seasonal trend analysis. |
| `paint_analysis_table` *(View: `fact_paint_analysis`)* | Fact / Analytical | `analysis_key` (`INTEGER PRIMARY KEY AUTOINCREMENT`) | None | `record_hash NOT NULL UNIQUE`; `color NOT NULL`; `analysis_date NOT NULL` | Stores color mixing history, RGB triples, and JSON paint component formulations from Paintelligent. |
| `load_log` | Metadata / Audit | `log_id` (`INTEGER PRIMARY KEY AUTOINCREMENT`) | None | `run_id NOT NULL`; `transformation_run_id NOT NULL`; `batch_id NOT NULL`; `table_name NOT NULL`; `status NOT NULL` | Records per-table batch load metrics, timestamps, validation outcomes, and lineage for auditing and recovery. |

---

## 2.2 Detailed Column Specifications

### 2.2.1 `product_table` (Dimension Table)

- **Purpose:** Master product catalog for Garcia Paint Center used to analyze pricing margins, stock availability, brand/category breakdowns, and sales performance.
- **Indexes:** `idx_product_table_name` on `(product_name)`, `idx_product_table_batch` on `(batch_id)`, and unique index on `(product_id)`.
- **Load Dependency Columns:**
  - **Match Key:** `(product_name, unit_purchase_price, estimated_price_php, category, brand)`
  - **Batch ID Column:** `batch_id`
  - **Status / History Columns:** `is_active` (SCD Type 1 soft-delete / active indicator), `loaded_at` (last load timestamp).

| Column Name | Data Type (SQLite) | Key / Role | Constraints | Reason for Inclusion |
|---|---|---|---|---|
| `product_key` | `INTEGER` | Surrogate PK | `PRIMARY KEY AUTOINCREMENT` | Stable warehouse surrogate key referenced by `sales_table.product_key`. |
| `product_id` | `TEXT` | Pipeline ID | `NOT NULL UNIQUE` | Preserves the `PROD-...` identifier generated in the Transformation Stage. |
| `product_name` | `TEXT` | Natural Key | `NOT NULL` | Canonical product name used to match sales records and identify items in reports. |
| `unit_purchase_price` | `NUMERIC` | Attribute | `CHECK (unit_purchase_price IS NULL OR unit_purchase_price >= 0)` | Acquisition cost per unit for gross margin analysis. |
| `estimated_price_php` | `NUMERIC` | Attribute | `CHECK (estimated_price_php IS NULL OR estimated_price_php >= 0)` | Estimated retail selling price in PHP (`₱`). |
| `stocks` | `TEXT` | Attribute | Nullable | Preserves permissive stock values (`12.00 Pieces`, `6.00 Gallon`, `--`, `120`) handed over from Stage 2. |
| `category` | `TEXT` | Attribute | Nullable | Product classification (e.g., `Latex`, `Enamel`, `Acrylic`, `Hardware`). |
| `brand` | `TEXT` | Attribute | Nullable | Manufacturer brand (e.g., `Boysen`, `Davies`, `Weber`, `Omega`). |
| `is_active` | `INTEGER` | Status Flag | `NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1))` | Distinguishes active catalog items (`1`) from items marked `Inactive` (`0`). |
| `batch_id` | `TEXT` | Lineage | `NOT NULL` | Identifies the transformation batch (`BATCH-...`) that last inserted or updated the row. |
| `transformation_run_id` | `TEXT` | Lineage | `NOT NULL` | Links the record to the transformation execution (`TR-...`). |
| `loaded_at` | `TEXT` | Audit Timestamp | `NOT NULL` | UTC ISO-8601 timestamp when the record was written to SQLite. |

---

### 2.2.2 `sales_table` (Fact Table)

- **Purpose:** Stores individual sales transactions for revenue aggregation, seasonal demand forecasting (`Dry` vs. `Rainy`), and top/slow-moving product identification.
- **Indexes:** `idx_sales_table_product_key` on `(product_key)`, `idx_sales_table_date` on `(sales_date)`, `idx_sales_table_batch` on `(batch_id)`, and unique index on `(sales_id)`.
- **Load Dependency Columns:**
  - **Match / Idempotency Key:** `batch_id` (batch-level delete-and-reload) and `sales_id`
  - **Foreign Key Resolution Column:** `product` → resolves `product_table.product_key`.

| Column Name | Data Type (SQLite) | Key / Role | Constraints | Reason for Inclusion |
|---|---|---|---|---|
| `sales_key` | `INTEGER` | Surrogate PK | `PRIMARY KEY AUTOINCREMENT` | Unique warehouse surrogate key for each fact row. |
| `sales_id` | `TEXT` | Pipeline ID | `NOT NULL UNIQUE` | Preserves the `SALE-...` identifier generated in Stage 2. |
| `product_key` | `INTEGER` | Foreign Key | `NOT NULL REFERENCES product_table(product_key)` | Enforces referential integrity between sales facts and the product dimension. |
| `brand` | `TEXT` | Attribute | Nullable | Brand associated with the transaction (`Boysen`, `Davies`, `Weber`, `Omega`). |
| `product` | `TEXT` | Natural Ref | `NOT NULL` | Canonical product name standardized during Stage 2 transformation. |
| `category` | `TEXT` | Attribute | Nullable | Product category at time of sale for category-level sales reporting. |
| `sales_date` | `TEXT` | Date Dimension | `NOT NULL` | Standardized ISO date (`YYYY-MM-DD`) for time-series and monthly/seasonal trends. |
| `quantity` | `INTEGER` | Fact Measure | `NOT NULL CHECK (quantity >= 0)` | Number of units sold in the transaction. |
| `unit_sold` | `NUMERIC` | Fact Measure | `NOT NULL CHECK (unit_sold >= 0)` | Total sales amount in PHP (`Total Sales (PHP)` mapped to `unit_sold` in Stage 2). |
| `season` | `TEXT` | Attribute | Nullable | Weather season (`Dry` or `Rainy`) for seasonal demand analysis. |
| `batch_id` | `TEXT` | Lineage | `NOT NULL` | Identifies the loaded transformation batch (`BATCH-...`). |
| `transformation_run_id` | `TEXT` | Lineage | `NOT NULL` | Links the record to the transformation run (`TR-...`). |
| `loaded_at` | `TEXT` | Audit Timestamp | `NOT NULL` | UTC ISO-8601 timestamp when the row was loaded. |

---

### 2.2.3 `paint_analysis_table` (Analytical Fact / Event Table)

- **Purpose:** Stores paint color formulation records extracted from the Paintelligent Supabase `paint_analysis_history` table for color trend and component usage analysis.
- **Indexes:** `idx_paint_analysis_date` on `(analysis_date)`, `idx_paint_analysis_batch` on `(batch_id)`, and unique index on `(record_hash)`.
- **Load Dependency Columns:**
  - **Match Key:** `record_hash` (`SHA-256` hash of `id|color|rgb|paint_components|analysis_date`)
  - **Batch ID Column:** `batch_id`.

| Column Name | Data Type (SQLite) | Key / Role | Constraints | Reason for Inclusion |
|---|---|---|---|---|
| `analysis_key` | `INTEGER` | Surrogate PK | `PRIMARY KEY AUTOINCREMENT` | Warehouse primary key (essential because source `id` is null in handover CSV). |
| `id` | `TEXT` | Source ID | Nullable | Preserves source `paint_analysis_history.id` when provided by the extractor. |
| `record_hash` | `TEXT` | Match Key | `NOT NULL UNIQUE` | Deterministic hash ensuring idempotent upserts even when source `id` is null. |
| `color` | `TEXT` | Attribute | `NOT NULL` | Hexadecimal color code (e.g., `#5CB85C`, `#1F1F1F`). |
| `rgb` | `TEXT` | Attribute | Nullable | Validated RGB channel dictionary/tuple string (`{'b': 92, 'g': 184, 'r': 92}`). |
| `paint_components` | `TEXT` | Attribute | Nullable | Validated JSON-like array of base paints, volumes (`amountMl`), and percentages. |
| `analysis_date` | `TEXT` | Timestamp | `NOT NULL` | Standardized ISO-8601 timestamp of the paint analysis event. |
| `batch_id` | `TEXT` | Lineage | `NOT NULL` | Identifies the transformation batch (`BATCH-...`). |
| `transformation_run_id` | `TEXT` | Lineage | `NOT NULL` | Links to the transformation run (`TR-...`). |
| `loaded_at` | `TEXT` | Audit Timestamp | `NOT NULL` | UTC ISO-8601 timestamp when the row was loaded. |

---

# 3. Load Strategy and Rules

## 3.1 Summary of Table Load Strategies

| Table | Load Type | Write Logic | Match Key | On Failure |
|---|---|---|---|---|
| `product_table` | Incremental | Upsert (SCD Type 1) | `(product_name, unit_purchase_price, estimated_price_php, category, brand)` | Roll back transaction, log error, stop batch |
| `sales_table` | Incremental | Insert (delete by `batch_id` first) | `batch_id` (batch replace) + `sales_id` | Roll back transaction, log error, stop batch |
| `paint_analysis_table` | Incremental | Upsert (SCD Type 1) | `record_hash` (`id`, `color`, `rgb`, `paint_components`, `analysis_date`) | Roll back transaction, log error, stop batch |

## 3.2 Detailed Rules by Category

- **Load Type (Incremental Load):**
  - All three target tables use **Incremental Load** rather than destructive table truncation so historical batches and stable surrogate keys (`product_key`, `analysis_key`) remain intact across runs.
- **Write Logic and Handling of Changing Data:**
  - `product_table` applies **SCD Type 1 (Overwrite)** via `INSERT` for new catalog products and `UPDATE` when an existing product's attributes (`stocks`, `product_id`, `is_active`, `batch_id`, `loaded_at`) are refreshed. Historical price tracking (SCD Type 2) is out of scope for the current schema, though `batch_id` and `loaded_at` track the latest update.
  - `sales_table` applies **Delete-by-Batch-ID then Insert**: before inserting rows for `batch_id`, `DELETE FROM sales_table WHERE batch_id = ?` removes any prior rows from that same batch, then inserts all incoming sales transactions.
  - `paint_analysis_table` applies **SCD Type 1 Upsert** on `record_hash`: new paint analysis events are inserted, while existing records matching the same `record_hash` have their batch lineage refreshed.
- **Handling of Deleted or Inactive Records:**
  - Products are never hard-deleted from `product_table` so historical `sales_table` foreign keys (`product_key`) remain valid.
  - Products containing `'Inactive'` in `product_name` (such as `1L Custom Paint #2 Inactive` or `DGM Paint Brush 1 in Inactive` — **13 products** in the current catalog) automatically receive `is_active = 0` (soft-delete flag), while active products receive `is_active = 1`.
- **Handling of Late-Arriving Records and Schema Mismatches:**
  - **Schema Mismatches:** Checked during Pre-Load Validation (`Schema match`). If any handover CSV has missing, extra, or reordered columns compared to the data contract (`EXPECTED_SCHEMAS`), the entire batch is **rejected before any database writes occur**.
  - **Late-Arriving Records:** Late-arriving sales or paint analysis records included in a new approved handover batch are loaded normally via their respective `sales_date` / `analysis_date` values and tagged with the current `batch_id`. If a late-arriving sales record references a product not present in `product_table`, it fails the foreign key lookup and triggers a critical batch rollback.
- **Load Order and Dependencies:**
  - **Strict Parent-Before-Child Order:** `product_table` (parent dimension) is always loaded before `sales_table` (child fact), followed by `paint_analysis_table`.
  - **Unmatched Fact Handling:** Every `sales_table` record must resolve a valid `product_key` in `product_table`. Because Stage 2 (`transformation.py` rule `C-05`) already quarantines unmatched sales products, 100% of handed-over sales records match `product_table`. If an unmatched fact ever bypasses Stage 2, `load.py` logs it to `load_rejected.csv` and fails the post-load reconciliation check, rolling back the batch.
- **Transaction and Rollback Handling:**
  - **Whole-Batch Atomic Transaction:** All three target tables (`product_table`, `sales_table`, `paint_analysis_table`) and all Post-Load Validation queries execute inside a single `BEGIN IMMEDIATE ... COMMIT` transaction block in SQLite.
  - **Rollback on Failure:** If any table write fails or any Critical Post-Load Validation check fails, `conn.rollback()` is called immediately. Because `COMMIT` is only executed after all Post-Load checks pass, **zero partial data** can ever remain in `paintelligent.db`. Additionally, a file-level snapshot backup (`load/backups/paintelligent_<batch_id>.db`) is created prior to loading.
- **Rerun and Idempotency:**
  - Running `python pipeline/CSV/load.py` twice on the same batch (`BATCH-20261018-0B474A`) produces **zero duplicate rows** and **zero inflated totals**:
    - On the first run: `product_table` inserts `579` rows (`updated=0`), `sales_table` inserts `796` rows, `paint_analysis_table` inserts `7` rows.
    - On a rerun of the same batch: `product_table` updates `579` existing rows (`inserted=0, updated=579`), `sales_table` deletes the `796` rows for `BATCH-20261018-0B474A` and re-inserts `796` rows, and `paint_analysis_table` updates `7` rows (`inserted=0, updated=7`). Total rows in `paintelligent.db` remain strictly `579`, `796`, and `7`, and `SUM(unit_sold)` remains exactly `574,810.00`.

---

# 4. Pre-Load Validation Checks

Before opening the write transaction on `load/paintelligent.db`, `load.py` executes six mandatory Pre-Load Validation Checks to confirm that both the target SQLite warehouse and the incoming Transformation Stage handover package are ready.

| Check Name | Target | Purpose | Validation Criteria | Severity | Actual Result (`LOAD-20261115-DE2F3E`) |
|---|---|---|---|---|---|
| **Target connection** | `load/paintelligent.db` | Ensure the SQLite database is reachable and writable before starting the load. | Connection succeeds within 30s timeout and temporary table write probe (`CREATE/INSERT/DROP`) succeeds. | **Critical** | `PASS` |
| **Schema match** | All target tables & incoming handover CSVs | Prevent column mismatch, missing field, or data type errors during loading. | Incoming CSV headers exactly match `EXPECTED_SCHEMAS` (`7`, `8`, and `5` columns) and all required columns exist in SQLite target tables. | **Critical** | `PASS` (All schemas matched) |
| **Handover status** | Transformation metadata (`logs/transformation.log` / `handover_metadata.json`) | Ensure only validated and approved transformation outputs are loaded. | Latest transformation run status is in `{'SUCCESS', 'SUCCESS_WITH_QUARANTINE', 'APPROVED'}`. | **Critical** | `PASS` (`Status = SUCCESS`) |
| **Handover record count** | `products_transformed.csv`, `sales_transformed.csv`, `paint_analysis_transformed.csv` | Confirm complete handover without truncated or missing files. | Row count of each incoming CSV equals the `records_output` logged by `transformation.py` (`579`, `796`, `7`). | **Critical** | `PASS` (`579`, `796`, `7`) |
| **Storage space availability** | Target disk volume (`load/`) | Prevent mid-transaction disk exhaustion during SQLite WAL writes and pre-load snapshot backup. | Available free disk space on the target drive is $\ge 50\text{ MB}$. | **Critical** | `PASS` (`58,741.2 MB` free) |
| **Duplicate batch check** | `load_log.batch_id` | Detect whether `batch_id` was already loaded with `SUCCESS` and enforce idempotent rerun rules. | `batch_id` has 0 prior `SUCCESS` entries in `load_log`, or idempotent rerun mode (`allow_idempotent_rerun=True`) is explicitly active. | **Critical** | `PASS` (`BATCH-20261018-0B474A` verified) |

---

# 5. Post-Load Validation and Reconciliation

Immediately after writing `product_table`, `sales_table`, and `paint_analysis_table` — and **before** committing the SQLite transaction — `load.py` runs seven Post-Load Validation and Reconciliation checks against the live transaction state.

> **Scope Limitation:** No data cleaning, value modification, or business transformation is performed during Post-Load Validation. Any discrepancy triggers the decision and rollback rules in Section 8.

| Check Name | Target | Purpose | Validation Criteria | Severity | Actual Result (`LOAD-20261115-DE2F3E`) |
|---|---|---|---|---|---|
| **Record reconciliation** | `product_table`, `sales_table`, `paint_analysis_table` | Detect lost, extra, or unrecorded rejected rows during loading. | `records_received = records_inserted + records_updated + records_rejected` and `records_rejected = 0` for every table. | **Critical** | `PASS` (`579=579+0`, `796=796+0`, `7=7+0`) |
| **Target batch row count** | All target tables (`WHERE batch_id = ?`) | Confirm that the target database contains the exact expected row count for the batch. | `COUNT(*)` in SQLite for `BATCH-20261018-0B474A` equals incoming CSV row counts (`579`, `796`, `7`). | **Critical** | `PASS` (`579`, `796`, `7`) |
| **Primary key uniqueness** | `product_key`/`product_id`, `sales_key`/`sales_id`, `analysis_key`/`record_hash` | Prevent duplicate surrogate keys, duplicate pipeline IDs, or null keys. | `COUNT(*) - COUNT(DISTINCT key) = 0` and `null_keys = 0` across all three target tables. | **Critical** | `PASS` (`dup_pk=0`, `dup_id=0`, `null_keys=0`) |
| **Referential integrity** | `sales_table.product_key` → `product_table.product_key` | Prevent orphan sales fact records that do not link to a valid product dimension record. | `LEFT JOIN` orphan count between `sales_table` and `product_table` equals `0` (`100%` match). | **Critical** | `PASS` (`Orphan sales = 0 / 796`) |
| **Required columns not null** | Mandatory columns across all target tables | Verify that no required identifiers, names, dates, quantities, or amounts are `NULL`. | `NULL` count in mandatory columns (`product_id`, `product_name`, `sales_id`, `product`, `sales_date`, `quantity`, `unit_sold`, `color`, `analysis_date`) equals `0`. | **Critical** | `PASS` (`Mandatory nulls = 0`) |
| **Amount and quantity reconciliation** | `sales_table` (`unit_sold`, `quantity`, `season`), `product_table` (`estimated_price_php`) | Confirm zero financial, seasonal, or inventory value loss between the handover CSVs and SQLite tables. | Source batch sum equals target SQLite sum for the batch (including Dry Season and Rainy Season subtotals within `0.01` rounding tolerance). | **Critical** | `PASS` (`sales.unit_sold`: `₱574,810.00 = ₱574,810.00` [**Dry Season**: `₱273,670.00` across `368` rows / `974` qty; **Rainy Season**: `₱301,140.00` across `428` rows / `1,077` qty]; `sales.quantity`: `2,051 = 2,051`; `product.estimated_price_php`: `₱373,611.54 = ₱373,611.54`) |
| **Optional attribute null rate check** | `product_table.category`, `product_table.brand` | Monitor completeness of optional product catalog attributes against the Stage 2 baseline. | Null rate is $\le 5.0\%$ (`28/579 = 4.84%` for `category`; `26/579 = 4.49%` for `brand`). | **Non-critical** | `PASS` (`category: 4.84%`, `brand: 4.49%`) |

---

# 6. Load Metadata and Log Specification

## 6.1 Metadata and Log Schema

Every execution of `load.py` records a structured entry per target table in the SQLite `load_log` table, writes a full batch summary to `load/load_metadata.json`, and writes a clean, non-repetitive operational report per batch to `logs/load.log`.

| Field | Purpose | Example (`sales_table` from actual run) |
|---|---|---|
| `run_id` | Unique identifier of the Stage 3 load run | `LOAD-20261115-DE2F3E` |
| `transformation_run_id` | Link to the upstream Stage 2 transformation run ID | `TR-20261018-F8AA3D` |
| `batch_id` | Identifier of the transformation batch being loaded | `BATCH-20261018-0B474A` |
| `batch_window` | Date range covered by the batch | `2025-01-01 to 2025-12-31` |
| `table_name` | Target SQLite table loaded | `sales_table` (also logged for `product_table` and `paint_analysis_table`) |
| `load_type` | Full or incremental load strategy | `INCREMENTAL` |
| `load_start_ts` | Timestamp when the table load started | `2026-11-15 10:03:12` |
| `load_end_ts` | Timestamp when the table load ended | `2026-11-15 10:03:12` |
| `load_duration_sec` | Elapsed time in seconds taken to load the table | `0.056` (`0.039` for `product_table`, `0.002` for `paint_analysis_table`, `0.158` total) |
| `records_received` | Rows received from the Transformation Stage handover CSV | `796` (`579` for `product_table`, `7` for `paint_analysis_table`) |
| `records_inserted` | New rows inserted into the target table | `796` (`579` for `product_table`, `7` for `paint_analysis_table`) |
| `records_updated` | Existing rows updated in the target table | `0` (`579` on idempotent rerun of `product_table`) |
| `records_rejected` | Rows that failed to load during Stage 3 | `0` |
| `status` | Final outcome of the table/batch load | `SUCCESS` |
| `pre_load_result` | Aggregate outcome of Section 4 pre-load validation checks | `PASS` |
| `post_load_result` | Aggregate outcome of Section 5 post-load validation checks | `PASS` |
| `error_code` | Failure or rejection code, if any | `NULL` |
| `error_message` | Detailed error description if a check or write failed | `NULL` |
| `load_code_version` | Version identifier of the Stage 3 load script | `v1.4.0` |

## 6.2 How Metadata and Logs Are Used

1. **Monitoring:** Pipeline operators inspect `load/load_metadata.json` and `logs/load.log` (or query `SELECT * FROM load_log ORDER BY log_id DESC`) to track batch completion status (`SUCCESS`, `WARNING`, `FAILED`), execution duration (`load_duration_sec`), and row volumes across runs.
2. **Troubleshooting:** When a load fails, `pre_load_result`, `post_load_result`, `error_code`, and `error_message` pinpoint whether the failure occurred before writing (e.g., `PRE_LOAD_VALIDATION_FAILED` due to schema mismatch or unapproved handover) or after writing (e.g., `POST_LOAD_VALIDATION_FAILED` due to reconciliation mismatch), while `load/rejected/load_rejected.csv` provides row-level diagnostics.
3. **Auditing:** Every loaded record in `product_table`, `sales_table`, and `paint_analysis_table` is stamped with `batch_id`, `transformation_run_id`, and `loaded_at`, and `load_log` preserves an immutable historical ledger of who ran what code version (`load_code_version = v1.4.0`) and when.
4. **Data Lineage:** Linking `run_id` (`LOAD-20261115-DE2F3E`) → `transformation_run_id` (`TR-20261018-F8AA3D`) → `batch_id` (`BATCH-20261018-0B474A`) allows any analytical query result in SQLite to be traced back to the exact transformation run in `logs/transformation.log` and the upstream extraction runs in `logs/csv_extraction.log` and `logs/analysis_extraction.log`.
5. **Recovery of a Failed Batch:** If a batch fails mid-process, SQLite automatically rolls back the uncommitted transaction. Operators can inspect `load_log` for the failed `batch_id`, fix the root cause (or restore the pre-load snapshot from `load/backups/`), and safely re-execute `python pipeline/CSV/load.py`.

---

# 7. Log Retention, Rejected Data, and Access

| Policy Area | Operational Logs & Rejected Files | Audit Records (`load_log` & `load_metadata.json`) |
|---|---|---|
| **Storage Location** | Operational log: `logs/load.log`<br>Rejected records: `load/rejected/load_rejected.csv`<br>DB backups: `load/backups/` | SQLite audit table: `load/paintelligent.db` (`load_log` table)<br>Latest batch manifest: `load/load_metadata.json` |
| **Retention Period** | **30 days minimum** (aligned with Stage 1 & Stage 2 `LOG_RETENTION_DAYS = 30`) | **365 days (1 year) minimum** in `load_log` for long-term compliance, lineage, and financial reconciliation |
| **Justification** | 30 days provides sufficient history to troubleshoot recent batch runs, investigate rejected rows, and verify monthly reporting cycles without consuming excessive disk space. | Audit records in `load_log` are compact metadata rows required to prove historical batch integrity, financial totals, and lineage across quarterly/annual business reviews. |
| **Location & Retention of Rejected Records** | Any row rejected during Stage 3 is written to `load/rejected/load_rejected.csv` with `run_id`, `batch_id`, `table_name`, `source_row_number`, `original_record` (JSON), `error_code`, `rejection_reason`, and `rejected_timestamp`, and retained for **at least 30 days** or until the issue is resolved and re-verified. | Stage 2 quarantine records remain preserved separately in `transformation/output/quarantine.csv` for 30 days. |
| **Access Permissions** | - **Read (`r`):** ETL developers, Data Analysts, QA reviewers.<br>- **Write (`w`):** Automated `load.py` service process only.<br>- **Delete (`d`):** Designated ETL Pipeline Administrator only, strictly after the retention window expires. | - **Read (`r`):** ETL developers, Data Analysts, Auditors.<br>- **Insert (`w`):** `load.py` process only.<br>- **Update/Delete:** Prohibited during normal operations; restricted to ETL Administrator after 365 days. |
| **Archiving Requirements** | Operational logs (`logs/load.log`) and rejected CSVs older than 30 days (or associated with an active investigation) are compressed (`.zip`/`.gz`) into `logs/archive/` before cleanup. | Pre-load SQLite snapshots in `load/backups/` are retained for the last **5 successful batches** (or 30 days) and archived before major schema migrations. |
| **Deletion Rules & Responsible Process** | Logs, backups, and rejected records **must never be deleted** before the 30-day retention period expires or while needed for active troubleshooting, auditing, or recovery. The **ETL Pipeline Administrator** reviews and purges expired archives monthly. | Audit rows in `load_log` older than 365 days may only be exported to cold archive and purged by the ETL Pipeline Administrator after annual reconciliation sign-off. |
| **Pre-Load Backup / Snapshot** | Before each load run, `backup_target_database()` in `load.py` copies `load/paintelligent.db` to `load/backups/paintelligent_<batch_id>.db` (keeping one snapshot per `batch_id` so rerunning the same batch never produces duplicate `.db` files). | Logged in `logs/load.log` prior to `BEGIN IMMEDIATE`. |

---

# 8. Load Acceptance, Rollback, and Completion Rules

## 8.1 Acceptance Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action | Decision Mode | Responsible Role |
|---|---|---|---|---|
| All 6 Pre-Load checks `PASS` **and** all 7 Post-Load checks `PASS` (0 rejected records, 100% reconciliation & aggregate match) | **APPROVED** | Commit SQLite transaction (`COMMIT`), log `status = SUCCESS` in `load_log`, write `load_metadata.json`, and publish views (`dim_product`, `fact_sales`, `fact_paint_analysis`) for downstream reporting. | **Automatic** | Automated Load Pipeline (`load.py`) |
| All Critical checks `PASS`, but one or more Non-critical checks return `WARNING` (e.g., optional attribute `category`/`brand` null rate exceeds `5.0%`) | **MANUAL REVIEW** | Commit transaction, mark batch `status = WARNING` (`acceptance_decision = MANUAL REVIEW`), hold downstream dashboard publication, and notify the Data / ETL Owner to inspect the warning. | **Manual Approval Required** | ETL Pipeline Owner / Data Analyst |
| Any Critical Pre-Load check returns `FAIL`, any Critical Post-Load check returns `FAIL`, `records_rejected > 0`, or an unhandled exception occurs | **REJECTED** | Execute `conn.rollback()` immediately so zero partial changes remain in `paintelligent.db`, write diagnostics to `load_rejected.csv` and `load_log` (`status = FAILED`), and alert the ETL Engineering Team. | **Automatic Rollback & Stop** | ETL Engineering Team (for root-cause fix & rerun) |

## 8.2 Detailed Operational Policies

1. **Automatic Approval:**
   - A batch is automatically marked **APPROVED** (`status = SUCCESS`) only when every Critical Pre-Load and Post-Load check passes, `records_received == records_inserted + records_updated`, `records_rejected == 0`, orphan foreign keys equal `0`, and `SUM(unit_sold)` (`₱574,810.00` — Dry Season `₱273,670.00` + Rainy Season `₱301,140.00`), `SUM(quantity)` (`2,051`), and `SUM(estimated_price_php)` (`₱373,611.54`) match the incoming handover CSVs within `0.01`.
2. **Manual Review:**
   - Triggered when all Critical checks pass but a Non-critical check raises a `WARNING` (for example, if missing `category` or `brand` values in `product_table` exceed the `5.0%` threshold). The ETL Pipeline Owner reviews `logs/load.log` and either approves publication or requests a source catalog update.
3. **Rejection, Rollback, and Retry Policy:**
   - **Conditions that fail the batch:** Database connection failure, schema mismatch, unapproved transformation status (`FAILED` or `MANUAL_REVIEW`), handover row-count mismatch, insufficient disk space ($< 50\text{ MB}$), primary key duplicate, foreign key orphan in `sales_table`, mandatory column `NULL`, or aggregate sum discrepancy.
   - **Rollback and Retry Rules:**
     - On any failure during loading or post-load validation, `conn.rollback()` is executed immediately, leaving `load/paintelligent.db` in its exact pre-batch state.
     - For **transient lock or I/O errors** (`sqlite3.OperationalError: database is locked`), SQLite's built-in busy timeout waits up to **30 seconds**, and the scheduler is permitted up to **3 automatic retries** with a **60-second wait interval** between retries.
     - For **data, schema, or reconciliation failures** (`PRE_LOAD_VALIDATION_FAILED` or `POST_LOAD_VALIDATION_FAILED`), **0 automatic retries** are performed—the pipeline stops immediately because retrying invalid data without fixing the upstream handover will fail identically.
   - **Failure Notification:** The ETL Engineering Team and Pipeline Administrator are notified via the error log entry in `logs/load.log` and the `REJECTED` status in `load/load_metadata.json` (and non-zero process exit code `1`).
4. **Completion and Handover to Downstream Users:**
   - **What is reported on completion:** Final `acceptance_decision` (`APPROVED`), `run_id`, `transformation_run_id`, `batch_id`, `batch_window`, per-table counts (`records_received`, `records_inserted`, `records_updated`, `records_rejected`), reconciled financial/unit/seasonal totals, and target database path (`load/paintelligent.db` & `load/paintelligent_readable.sql`).
   - **How downstream users are notified:** `load/load_metadata.json` is updated with `"status": "SUCCESS"` and `"acceptance_decision": "APPROVED"`, and the final summary banner is written to `logs/load.log`, signaling to BI analysts and reporting queries that `product_table` (`dim_product`), `sales_table` (`fact_sales`), and `paint_analysis_table` (`fact_paint_analysis`) are ready for querying.

## 8.3 Validation Summary (Actual Execution: `BATCH-20261018-0B474A`)

| Validation Summary | Result | Details |
|---|---|---|
| Pre-load validation checks (all 6 checks) | `PASS` | Connection, schema, handover status (`SUCCESS`), row counts (`579`, `796`, `7`), disk space (`58.7 GB`), and duplicate batch check all passed. |
| `product_table` (`dim_product`) load | `PASS` | `579` received, `579` loaded, `0` rejected (`₱373,611.54` price sum reconciled). |
| `sales_table` (`fact_sales`) load | `PASS` | `796` received, `796` inserted, `0` updated, `0` rejected, `0` orphan FKs, `₱574,810.00` (`Dry: ₱273,670.00`, `Rainy: ₱301,140.00`) & `2,051` units reconciled. |
| `paint_analysis_table` (`fact_paint_analysis`) load | `PASS` | `7` received, `7` loaded, `0` rejected. |
| **Overall Load** | **APPROVED** (`SUCCESS`) | Atomic transaction committed; data published in `load/paintelligent.db` and `load/paintelligent_readable.sql`. |

---

# 9. Common Load Problems and Mitigation

The table below documents realistic load problems specific to the **SQLite target database (`load/paintelligent.db`)** and the **Paintelligent batch handover design**, along with their causes, impacts, and implemented mitigations.

| Problem | Possible Cause | Impact on the Load Process | Proposed Solution / Implemented Mitigation |
|---|---|---|---|
| **Duplicate rows or inflated sales totals after a batch rerun** | Running `load.py` twice on the same handover batch, or re-running after `transformation.py` regenerates random `PROD-...` and `SALE-...` UUIDs. | Inflates revenue (`SUM(unit_sold)`) and unit counts (`SUM(quantity)`) in `sales_table` and duplicates catalog items in `product_table`. | Check `load_log.batch_id` before loading; use natural-attribute SCD Type 1 Upsert on `product_table`, deterministic `record_hash` Upsert on `paint_analysis_table`, and `DELETE FROM sales_table WHERE batch_id = ?` before inserting sales facts. |
| **Foreign key violation between `sales_table` and `product_table`** | Loading `sales_table` before `product_table`, or a sales row referencing a product name not present in `product_table`. | SQLite raises a foreign key error or creates orphan sales facts that break product-level revenue reports. | Enforce strict parent-before-child load order (`product_table` → `sales_table` → `paint_analysis_table`), enable `PRAGMA foreign_keys = ON`, verify `product_key` lookup for every sale, and run the Post-Load Referential Integrity check (`orphan_sales == 0`). |
| **Partial database state after a mid-load crash or check failure** | Power loss, OS interruption, or a post-load validation failure after `product_table` is written but before `sales_table` finishes. | Target database contains updated dimensions without matching sales facts, resulting in inconsistent analytics. | Wrap all three table loads and all Post-Load Validation checks inside a single `BEGIN IMMEDIATE ... COMMIT` transaction, call `conn.rollback()` on any exception or check failure, and create a pre-load file snapshot in `load/backups/`. |
| **SQLite `database is locked` timeout during batch write** | An analyst or external BI tool holds an open write/exclusive lock on `load/paintelligent.db` while `load.py` attempts to write. | `BEGIN IMMEDIATE` times out and the batch load fails with `sqlite3.OperationalError`. | Enable Write-Ahead Logging (`PRAGMA journal_mode = WAL`) so concurrent readers never block the writer, configure a `30.0s` connection timeout, and schedule batch loads during the off-peak window (`04:00`). |
| **Rejection of valid products due to non-integer `stocks` values** | Handover CSV `products_transformed.csv` contains permissive stock strings such as `'12.00 Pieces'`, `'6.00 Gallon'`, and `'--'` (by design in Stage 2). | Strict `INTEGER` column typing or integer casting in Stage 3 would reject 500+ valid catalog products or corrupt stock descriptions. | Define `product_table.stocks` as `TEXT` in SQLite to honor the Stage 2 data contract without re-transforming or rejecting valid handover records. |
| **Null source `id` in `paint_analysis_transformed.csv` causing key failure** | Stage 2 outputs blank `id` values for all 7 rows of `paint_analysis_transformed.csv` because `id` was omitted in the extractor column list. | Using source `id` as `PRIMARY KEY NOT NULL` would reject all 7 paint analysis rows, or allow duplicates on rerun if left nullable without a match key. | Use an auto-incrementing surrogate primary key (`analysis_key`), keep source `id` nullable, and enforce uniqueness/idempotency via a deterministic `SHA-256` `record_hash` column. |
| **Collapsing distinct products that share a base `product_name`** | Two catalog rows in `products_transformed.csv` share the name `TEST 1` but have different selling prices (`750.00` vs. `1000.00`). | Matching `product_table` solely on `product_name` would overwrite the first row during the initial load, causing `target_batch_rows` (`578`) to fail reconciliation against `records_received` (`579`). | Use the composite natural key `(product_name, unit_purchase_price, estimated_price_php, category, brand)` for `product_table` upserts so all `579` distinct catalog items load cleanly and reconcile 100%. |

---

# Expected Output

Stage 3 produces a fully populated, validated, and reconciled SQLite analytical database (`load/paintelligent.db`), a human-readable SQL export (`load/paintelligent_readable.sql`), a single-file-per-batch database backup snapshot (`load/backups/paintelligent_<batch_id>.db`), a rejected-records file (`load/rejected/load_rejected.csv`), structured batch metadata (`load/load_metadata.json`), and a non-repetitive operational load report (`logs/load.log`).

The documentation and implemented load process (`pipeline/CSV/load.py`) are fully aligned so that any team member can understand, validate, monitor, and maintain the Load Stage:

- **Validated and Reconciled Target Data:** All `579` products (`product_table` / `dim_product`), `796` sales transactions (`sales_table` / `fact_sales`), and `7` paint analysis records (`paint_analysis_table` / `fact_paint_analysis`) from batch `BATCH-20261018-0B474A` (`TR-20261018-F8AA3D`) are loaded inside an atomic transaction (`BEGIN IMMEDIATE ... COMMIT`) and reconciled 100% against the Transformation Stage handover inputs (`₱574,810.00` total sales — **Dry Season:** `₱273,670.00` across `368` rows / `974` units; **Rainy Season:** `₱301,140.00` across `428` rows / `1,077` units — `2,051` total units sold, and `₱373,611.54` total product estimated price).
- **Traceable, Auditable, and Recoverable Batches:** Every loaded record and `load_log` entry is stamped with `run_id`, `transformation_run_id`, `batch_id`, and `loaded_at`. Failed loads automatically roll back (`conn.rollback()`) and can be recovered from `load/backups/paintelligent_<batch_id>.db` and safely re-executed without creating duplicate rows or inflating financial totals.
- **Strict Stage Boundary:** Only approved data handed over from the Transformation Stage (`transformation/output/`) is loaded. No new data extraction or business transformation logic is performed in this stage.
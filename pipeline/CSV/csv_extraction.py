import os
import sys
import atexit
import time
import uuid
import pandas as pd
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
CSV_DIR = Path(__file__).resolve().parent          # pipeline/CSV/
PROJECT_ROOT = CSV_DIR.parents[1]                  # ETL-PIPELINE-G4/
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------
log_file = open(
    LOG_DIR / "csv_extraction.log",
    "a",
    encoding="utf-8"
)


class Tee:
    """Write output to both the terminal and the log file."""
    def __init__(self, terminal, log_file):
        self.terminal = terminal
        self.log_file = log_file

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.log_file.flush()
        return len(message)

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

    def isatty(self):
        return self.terminal.isatty()

    def fileno(self):
        return self.terminal.fileno()

    @property
    def encoding(self):
        return self.terminal.encoding


sys.stdout = Tee(sys.stdout, log_file)
sys.stderr = Tee(sys.stderr, log_file)
atexit.register(log_file.close)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def get_timestamp() -> str:
    """Returns the current time as a formatted string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def generate_run_id() -> str:
    """Generates a unique Pipeline Run ID, e.g. RUN-20260514-A1B2C3."""
    date_part = datetime.now().strftime("%Y%m%d")
    unique_part = uuid.uuid4().hex[:6].upper()
    return f"RUN-{date_part}-{unique_part}"


def log_extraction_report(report: dict) -> None:
    """
    Writes a structured extraction report to the log file (and terminal)
    following the audit spec. Same format as analysis_extraction.py.
    """
    divider = "=" * 70

    # ---- Leading space before the report ----
    print()
    print()
    print(divider)
    print(f"[{get_timestamp()}] 📋 EXTRACTION REPORT")
    print(divider)

    print(f"  Pipeline Run ID           : {report['pipeline_run_id']}")
    print(f"  Table or File Name        : {report['source_name']}")
    print(f"  Extraction Start Timestamp: starts at: {report['start_timestamp']}")
    print(f"  Extraction End Timestamp  : ends at: {report['end_timestamp']}")
    print(f"  Extraction Status         : {report['status']}")
    print(f"  Number of Records Read    : {report['records_read']}")
    print(f"  Number of Records Extracted: {report['records_extracted']}")
    print(f"  Number of Records Rejected: {report['records_rejected']}")
    print(f"  Extraction Window         : {report['data_range']}")
    print(f"  Validation Results        : {report['validation_result']}")
    print(f"  Error Message or Error Code: {report['error']}")
    print(f"  Extraction Duration       : {report['duration']}")

    print(divider)
    # ---- Trailing space after the report ----
    print()
    print()


# ---------------------------------------------------------------------------
# CSV EXTRACTION
# ---------------------------------------------------------------------------
def extract_csv(file_name: str, **kwargs) -> pd.DataFrame:
    """
    Extracts data from a single CSV file inside pipeline/CSV/ and logs a
    full audit report matching the required schema.
    """
    file_path = CSV_DIR / file_name
    run_id = generate_run_id()
    start_time = time.time()
    start_ts = get_timestamp()

    # Defaults for the report
    status = "FAILED"
    records_read = 0
    records_extracted = 0
    records_rejected = 0
    data_range = "N/A for CSV files"
    validation_result = "FAIL"
    error_msg = "N/A"
    df = pd.DataFrame()

    try:
        # ---- File existence check ----
        if not file_path.exists():
            error_msg = f"File not found at {file_path}"
            raise FileNotFoundError(error_msg)

        # ---- Read the CSV ----
        df = pd.read_csv(file_path, encoding="utf-8-sig", **kwargs)
        records_read = len(df)
        records_extracted = len(df)

        # ---- Validation ----
        if df.empty:
            # Empty file — still considered a valid extraction with 0 rows
            status = "SUCCESS"
            validation_result = "PASS (empty file)"
        elif df.columns.isnull().any() or len(df.columns) == 0:
            status = "FAILED"
            validation_result = "FAIL (missing or empty headers)"
            error_msg = "CSV has no valid column headers"
        else:
            status = "SUCCESS"
            validation_result = "PASS"

        # ---- Data Range: CSV files report "N/A for CSV files" per spec ----
        data_range = "N/A for CSV files"

    except Exception as e:
        error_msg = str(e)
        status = "FAILED"
        validation_result = "FAIL"

    end_time = time.time()
    end_ts = get_timestamp()
    duration = f"{end_time - start_time:.3f}s"

    # ---- Emit the structured report ----
    log_extraction_report({
        "pipeline_run_id": run_id,
        "source_name": file_name,
        "start_timestamp": start_ts,
        "end_timestamp": end_ts,
        "status": status,
        "records_read": records_read,
        "records_extracted": records_extracted,
        "records_rejected": records_rejected,
        "data_range": data_range,
        "validation_result": validation_result,
        "error": error_msg,
        "duration": duration,
    })

    return df


def extract_both_csvs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extracts both specific CSVs and returns them as a tuple.
    Logs the start and end of the entire extraction batch.
    """
    batch_start = time.time()
    print("=" * 60)
    print(f"[{get_timestamp()}] 🚀 STARTING CSV EXTRACTION BATCH")
    print("=" * 60)

    # 1. Extract the Products file
    products_df = extract_csv("All Products - Cleaned.csv")

    # 2. Extract the Sales file
    sales_df = extract_csv("GPC_Sales - make equal.csv")

    return products_df, sales_df


if __name__ == "__main__":
    # Run the extraction (no preview prints)
    products_df, sales_df = extract_both_csvs()
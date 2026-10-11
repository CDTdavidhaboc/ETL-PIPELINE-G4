import os
import sys
import atexit
import time
import uuid
import pandas as pd
from datetime import datetime
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_file = open(
    LOG_DIR / "analysis_extraction.log",
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
    following the audit spec. Includes spacing before the report block.
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
# SUPABASE EXTRACTION
# ---------------------------------------------------------------------------
def get_supabase_client() -> Client:
    """Creates and returns a Supabase client."""
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in environment variables.")

    return create_client(url, key)


def extract_specific_columns(table_name: str, columns: list) -> pd.DataFrame:
    """
    Extracts specific columns from a Supabase table and logs a full audit
    report matching the required schema.
    """
    run_id = generate_run_id()
    start_time = time.time()
    start_ts = get_timestamp()

    # Defaults for the report
    status = "FAILED"
    records_read = 0
    records_extracted = 0
    records_rejected = 0
    data_range = "N/A"
    validation_result = "FAIL"
    error_msg = "N/A"
    df = pd.DataFrame()

    try:
        supabase = get_supabase_client()
        column_string = ",".join(columns)
        response = supabase.table(table_name).select(column_string).execute()

        df = pd.DataFrame(response.data)
        records_read = len(df)
        records_extracted = len(df)

        # ---- Validation ----
        if records_read == 0:
            status = "SUCCESS"
            validation_result = "PASS (empty result)"
        elif list(df.columns) == columns:
            status = "SUCCESS"
            validation_result = "PASS"
        else:
            status = "FAILED"
            validation_result = "FAIL (column mismatch)"
            error_msg = f"Expected {columns}, got {list(df.columns)}"

        # ---- Data Range ----
        if "analysis_date" in df.columns and not df.empty:
            try:
                min_date = pd.to_datetime(df["analysis_date"]).min()
                max_date = pd.to_datetime(df["analysis_date"]).max()
                data_range = f"{min_date.date()} to {max_date.date()}"
            except Exception:
                data_range = "Full available table"
        else:
            data_range = "Full available table"

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
        "source_name": table_name,
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


if __name__ == "__main__":
    print(f"[{get_timestamp()}] 🚀 STARTING SUPABASE EXTRACTION BATCH")

    TARGET_TABLE = "paint_analysis_history"
    TARGET_COLUMNS = [
        "color_hex",
        "rgb",
        "paint_components",
        "analysis_date",
    ]

    df = extract_specific_columns(TARGET_TABLE, TARGET_COLUMNS)
    # Preview removed — the structured report above is sufficient.
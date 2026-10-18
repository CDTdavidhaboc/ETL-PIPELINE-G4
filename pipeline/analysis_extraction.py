
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
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def generate_run_id() -> str:
    date_part = datetime.now().strftime("%Y%m%d")
    unique_part = uuid.uuid4().hex[:6].upper()
    return f"RUN-{date_part}-{unique_part}"


def log_extraction_report(report: dict) -> None:
    divider = "=" * 70

    print()
    print(divider)
    print(f"[{get_timestamp()}] EXTRACTION REPORT")
    print(divider)

    print(f"  Pipeline Run ID            : {report['pipeline_run_id']}")
    print(f"  Table or File Name         : {report['source_name']}")
    print(f"  Extraction Start Timestamp : {report['start_timestamp']}")
    print(f"  Extraction End Timestamp   : {report['end_timestamp']}")
    print(f"  Extraction Status          : {report['status']}")
    print(f"  Number of Records Read     : {report['records_read']}")
    print(f"  Number of Records Extracted: {report['records_extracted']}")
    print(f"  Number of Records Rejected : {report['records_rejected']}")
    print(f"  Extraction Window          : {report['data_range']}")
    print(f"  Validation Results         : {report['validation_result']}")
    print(f"  Error Message or Error Code: {report['error']}")
    print(f"  Extraction Duration        : {report['duration']}")

    print(divider)
    print()


# ---------------------------------------------------------------------------
# SUPABASE EXTRACTION
# ---------------------------------------------------------------------------
def get_supabase_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise ValueError(
            "Missing SUPABASE_URL or SUPABASE_KEY in environment variables."
        )

    return create_client(url, key)


def extract_specific_columns(
    table_name: str,
    columns: list[str]
) -> pd.DataFrame:
    """Extract selected columns from a Supabase table and log the results."""

    run_id = generate_run_id()
    start_time = time.time()
    start_ts = get_timestamp()

    status = "FAILED"
    records_read = 0
    records_extracted = 0
    records_rejected = 0
    data_range = "N/A"
    validation_result = "FAIL"
    error_msg = "N/A"
    df = pd.DataFrame(columns=columns)

    try:
        supabase = get_supabase_client()
        column_string = ",".join(columns)

        response = (
            supabase.table(table_name)
            .select(column_string)
            .execute()
        )

        df = pd.DataFrame(response.data, columns=columns)

        records_read = len(df)
        records_extracted = len(df)

        if records_read == 0:
            status = "SUCCESS"
            validation_result = "PASS (empty result)"
        elif list(df.columns) == columns:
            status = "SUCCESS"
            validation_result = "PASS"
        else:
            status = "FAILED"
            validation_result = "FAIL (column mismatch)"
            error_msg = (
                f"Expected {columns}, got {list(df.columns)}"
            )

        if "analysis_date" in df.columns and not df.empty:
            try:
                dates = pd.to_datetime(
                    df["analysis_date"],
                    errors="coerce"
                ).dropna()

                if not dates.empty:
                    data_range = (
                        f"{dates.min().date()} to "
                        f"{dates.max().date()}"
                    )
                else:
                    data_range = "No valid analysis dates"
            except (ValueError, TypeError):
                data_range = "Full available table"
        else:
            data_range = "Full available table"

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        status = "FAILED"
        validation_result = "FAIL"

    end_time = time.time()
    end_ts = get_timestamp()
    duration = f"{end_time - start_time:.3f}s"

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


# ---------------------------------------------------------------------------
# RUN EXTRACTION DIRECTLY
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"[{get_timestamp()}] STARTING SUPABASE EXTRACTION")

    TARGET_TABLE = "paint_analysis_history"

    TARGET_COLUMNS = [
        "id",
        "color_hex",
        "rgb",
        "paint_components",
        "analysis_date",
    ]

    df = extract_specific_columns(
        TARGET_TABLE,
        TARGET_COLUMNS
    )

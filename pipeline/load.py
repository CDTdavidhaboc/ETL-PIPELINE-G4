"""
Entry-point wrapper for Paintelligent ETL Stage 3: Data Load.
Allows running `python pipeline/load.py` or `python pipeline/CSV/load.py`.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

sys.dont_write_bytecode = True

CSV_LOAD_SCRIPT = Path(__file__).resolve().parent / "CSV" / "load.py"

spec = importlib.util.spec_from_file_location("paintelligent_csv_load", CSV_LOAD_SCRIPT)
if spec is None or spec.loader is None:
    raise ImportError(f"Unable to load {CSV_LOAD_SCRIPT}")
module = importlib.util.module_from_spec(spec)
sys.modules["paintelligent_csv_load"] = module
spec.loader.exec_module(module)

if __name__ == "__main__":
    raise SystemExit(module.run_load())

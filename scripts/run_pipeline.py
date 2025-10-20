"""Run the demo pipeline without Airflow."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from bitemporal import merge_logic

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT_DIR / "data" / "synthetic_refdata.csv"
DB_PATH = ROOT_DIR / "reference_data.db"


def extract(path: Path) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            records.append(dict(row))
    return records
def load(records: List[Dict[str, Any]], knowledge_time: datetime, allow_late: bool) -> merge_logic.MergeSummary:
    conn = merge_logic.connect(DB_PATH)
    try:
        summary = merge_logic.merge_records(
            conn,
            records,
            knowledge_time=knowledge_time,
            allow_late_arrivals=allow_late,
        )
    finally:
        conn.close()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-path",
        type=Path,
        default=DATA_PATH,
        help=f"Path to the input CSV (default: {DATA_PATH})",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DB_PATH,
        help=f"Location of the SQLite database (default: {DB_PATH})",
    )
    parser.add_argument(
        "--allow-late",
        action="store_true",
        help="Allow processing records older than the stored watermark",
    )
    args = parser.parse_args()

    knowledge_time = datetime.now(tz=timezone.utc)
    records = extract(args.data_path)
    summary = load(records, knowledge_time, args.allow_late)

    print("Processed:", summary.to_dict())


if __name__ == "__main__":  # pragma: no cover
    main()

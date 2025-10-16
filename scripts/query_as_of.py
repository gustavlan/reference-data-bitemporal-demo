"""CLI helper to query the reference data snapshot as-of a knowledge time."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from bitemporal import merge_logic

ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "reference_data.db"


def _parse_iso(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "knowledge_time",
        help="Knowledge timestamp in ISO-8601 format (e.g. 2025-03-01T00:00:00Z)",
    )
    parser.add_argument(
        "--effective-time",
        help="Optional effective timestamp (defaults to knowledge time).",
    )
    parser.add_argument(
        "--security-id",
        action="append",
        dest="security_ids",
        help="Filter to specific security IDs (may be provided multiple times)",
    )
    args = parser.parse_args()

    knowledge_dt = _parse_iso(args.knowledge_time)
    effective_dt = _parse_iso(args.effective_time) if args.effective_time else None

    conn = merge_logic.connect(DB_PATH)
    rows = merge_logic.query_as_of(
        conn,
        knowledge_time=knowledge_dt,
        effective_time=effective_dt,
        security_ids=args.security_ids,
    )
    for row in rows:
        print(row)
    if not rows:
        print("No records found for the provided snapshot.")


if __name__ == "__main__":  # pragma: no cover
    main()

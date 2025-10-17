"""Core bi-temporal merge logic for reference data.

This module provides helpers to store reference data with separate valid
(event) and knowledge (transaction) timelines using a Slowly Changing
Dimension Type 2 (SCD2) pattern on top of SQLite.

Key concepts
------------
* Each record carries ``valid_from``/``valid_to`` (event timeline) and
  ``knowledge_from``/``knowledge_to`` (knowledge timeline).
* New facts never overwrite history. Instead, we close knowledge windows for
  superseded rows and insert new versions that reflect the latest knowledge.
* Idempotency is enforced by skipping inserts when an identical open knowledge
  row already exists. Re-processing the same input therefore has no effect.
"""

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
DEFAULT_TZ = timezone.utc
WATERMARK_NAME = "event_time"


@dataclass
class MergeSummary:
    """High level merge statistics returned by :func:`merge_records`."""

    processed: int
    skipped_as_late: int
    inserted_rows: int
    knowledge_time: datetime
    max_event_time: Optional[datetime]

    def to_dict(self) -> Dict[str, Optional[str]]:
        """Return a serialisable representation of the summary."""

        return {
            "processed": self.processed,
            "skipped_as_late": self.skipped_as_late,
            "inserted_rows": self.inserted_rows,
            "knowledge_time": _format_ts(self.knowledge_time),
            "max_event_time": _format_ts(self.max_event_time)
            if self.max_event_time
            else None,
        }


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Create a SQLite connection with sensible defaults."""

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db(conn: sqlite3.Connection) -> None:
    """Create the required tables and indexes if they do not already exist."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reference_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id TEXT NOT NULL,
            attributes TEXT NOT NULL,
            event_time TEXT NOT NULL,
            valid_from TEXT NOT NULL,
            valid_to TEXT,
            knowledge_from TEXT NOT NULL,
            knowledge_to TEXT,
            is_current INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_reference_data_entity_valid
            ON reference_data (security_id, valid_from)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_reference_data_current
            ON reference_data (security_id, is_current)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_watermarks (
            name TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.commit()


def merge_records(
    conn: sqlite3.Connection,
    records: Sequence[Dict[str, str]],
    *,
    knowledge_time: Optional[datetime] = None,
    allow_late_arrivals: bool = True,
) -> MergeSummary:
    """Merge incoming records into the reference table.

    Parameters
    ----------
    conn:
        SQLite connection targeting the reference database.
    records:
        Iterable of dictionaries with at least ``security_id`` and
        ``event_time`` keys. All additional keys are persisted inside a JSON
        ``attributes`` column.
    knowledge_time:
        Timestamp representing when the pipeline learned about this batch. A
        UTC ``datetime`` is recommended. Defaults to ``datetime.utcnow``.
    allow_late_arrivals:
        When ``False`` records with ``event_time`` <= the stored watermark are
        skipped. The caller can re-run with ``True`` to perform controlled
        backfills.
    """

    initialize_db(conn)

    if knowledge_time is None:
        knowledge_time = datetime.now(tz=DEFAULT_TZ)
    else:
        knowledge_time = _ensure_utc(knowledge_time)

    sorted_records = sorted(
        (_normalise_record(rec) for rec in records),
        key=lambda item: (item[0], item[1]),  # (security_id, event_time)
    )

    watermark = get_event_watermark(conn)
    processed_count = 0
    skipped_count = 0
    inserted_rows = 0
    max_event_time: Optional[datetime] = None

    with conn:  # encapsulate the entire batch in a single transaction
        for security_id, event_dt, payload in sorted_records:
            if watermark and not allow_late_arrivals and event_dt <= watermark:
                skipped_count += 1
                continue

            inserted = _merge_single_record(
                conn,
                security_id,
                event_dt,
                payload,
                knowledge_time,
            )
            if inserted:
                processed_count += 1
                inserted_rows += inserted
                if not max_event_time or event_dt > max_event_time:
                    max_event_time = event_dt
            else:
                processed_count += 1

        if max_event_time and (
            not watermark or max_event_time > watermark
        ):
            update_event_watermark(conn, max_event_time)

    return MergeSummary(
        processed=processed_count,
        skipped_as_late=skipped_count,
        inserted_rows=inserted_rows,
        knowledge_time=knowledge_time,
        max_event_time=max_event_time,
    )


def query_as_of(
    conn: sqlite3.Connection,
    knowledge_time: datetime,
    *,
    effective_time: Optional[datetime] = None,
    security_ids: Optional[Iterable[str]] = None,
) -> List[Dict[str, object]]:
    """Return the reference data as it was known on ``knowledge_time``.

    Parameters
    ----------
    knowledge_time:
        The knowledge (transaction) time snapshot. Only rows with
    ``knowledge_from`` <= time < ``knowledge_to`` (if present) are
        returned.
    effective_time:
        Optional cut-off for the valid (event) timeline. Defaults to the same
        as ``knowledge_time`` to produce a single point-in-time snapshot.
    security_ids:
        Optional iterable filtering to specific business keys.
    """

    initialize_db(conn)

    knowledge_time = _ensure_utc(knowledge_time)
    effective_time = (
        _ensure_utc(effective_time)
        if effective_time is not None
        else knowledge_time
    )

    params: List[object] = [
        _format_ts(knowledge_time),
        _format_ts(knowledge_time),
        _format_ts(effective_time),
        _format_ts(effective_time),
    ]
    filters = ""
    if security_ids:
        placeholders = ",".join("?" for _ in security_ids)
        filters = f" AND security_id IN ({placeholders})"
        params.extend(security_ids)

    query = (
        "SELECT security_id, attributes, event_time, valid_from, valid_to, "
        "knowledge_from, knowledge_to, is_current "
        "FROM reference_data "
        "WHERE knowledge_from <= ? AND (knowledge_to IS NULL OR knowledge_to > ?) "
        "AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)"
        f"{filters} "
        "ORDER BY security_id, valid_from"
    )

    cursor = conn.execute(query, params)
    rows = []
    for row in cursor.fetchall():
        payload = json.loads(row["attributes"])
        rows.append(
            {
                "security_id": row["security_id"],
                "attributes": payload,
                "event_time": row["event_time"],
                "valid_from": row["valid_from"],
                "valid_to": row["valid_to"],
                "knowledge_from": row["knowledge_from"],
                "knowledge_to": row["knowledge_to"],
                "is_current": bool(row["is_current"]),
            }
        )
    return rows


def get_event_watermark(conn: sqlite3.Connection) -> Optional[datetime]:
    """Return the current event-time watermark, if any."""

    initialize_db(conn)

    cursor = conn.execute(
        "SELECT value FROM pipeline_watermarks WHERE name = ?",
        (WATERMARK_NAME,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return _parse_ts(row["value"])


def update_event_watermark(
    conn: sqlite3.Connection, new_value: datetime
) -> None:
    """Persist the event-time watermark."""

    initialize_db(conn)

    new_value = _ensure_utc(new_value)
    conn.execute(
        "INSERT INTO pipeline_watermarks (name, value) VALUES (?, ?) "
        "ON CONFLICT(name) DO UPDATE SET value = excluded.value",
        (WATERMARK_NAME, _format_ts(new_value)),
    )


# Helper functions


def _normalise_record(record: Dict[str, object]) -> Tuple[str, datetime, Dict[str, object]]:
    try:
        security_id = str(record["security_id"]).strip()
    except KeyError as exc:
        raise KeyError("record missing 'security_id'") from exc

    try:
        event_raw = record["event_time"]
    except KeyError as exc:
        raise KeyError("record missing 'event_time'") from exc
    event_dt = _parse_ts(event_raw)

    attributes = {
        key: value
        for key, value in record.items()
        if key not in {"security_id", "event_time"}
    }

    return security_id, event_dt, attributes


def _merge_single_record(
    conn: sqlite3.Connection,
    security_id: str,
    event_time: datetime,
    attributes: Dict[str, object],
    knowledge_time: datetime,
) -> int:
    """Merge a single record, returning the number of inserted rows."""

    knowledge_iso = _format_ts(knowledge_time)
    event_iso = _format_ts(event_time)
    attrs_json = json.dumps(attributes, sort_keys=True)

    current_rows = _fetch_current_rows(conn, security_id)

    # Deduplicate exact matches (idempotency guard)
    for row in current_rows:
        if row["valid_from"] == event_iso and _row_payload(row) == attributes:
            return 0

    # Check for corrections at the same boundary (same valid_from)
    for row in current_rows:
        if row["valid_from"] == event_iso:
            _supersede_row(conn, row, knowledge_iso)
            new_valid_to = row["valid_to"]
            new_is_current = 1 if new_valid_to is None else 0
            return int(
                _insert_row(
                    conn,
                    security_id,
                    attrs_json,
                    event_iso,
                    new_valid_to,
                    knowledge_iso,
                    new_is_current,
                    event_iso,
                )
            )

    prev_row = _find_prev_row(current_rows, event_time)
    next_row = _find_next_row(current_rows, event_time)

    if prev_row and _overlaps(prev_row, event_time):
        inserted = 0
        # Close the previous knowledge record
        _supersede_row(conn, prev_row, knowledge_iso)
        prev_attrs = _row_payload(prev_row)
        prev_valid_from = prev_row["valid_from"]
        prev_valid_to = prev_row["valid_to"]

        # Reinstate the earlier segment with updated knowledge (if needed)
        if prev_valid_from != event_iso:
            inserted += int(
                _insert_row(
                    conn,
                    security_id,
                    json.dumps(prev_attrs, sort_keys=True),
                    prev_valid_from,
                    event_iso,
                    knowledge_iso,
                    0,
                    prev_valid_from,
                )
            )

        # Determine how far the new event is valid until
        new_valid_to = prev_valid_to
        if next_row:
            next_valid_from = next_row["valid_from"]
            if new_valid_to is None or _parse_ts(new_valid_to) > _parse_ts(next_valid_from):
                new_valid_to = next_valid_from

        new_is_current = 1 if new_valid_to is None else 0
        inserted += int(
            _insert_row(
                conn,
                security_id,
                attrs_json,
                event_iso,
                new_valid_to,
                knowledge_iso,
                new_is_current,
                event_iso,
            )
        )
        return inserted

    # New entity or earlier-than-known event
    new_valid_to = next_row["valid_from"] if next_row else None
    new_is_current = 1 if new_valid_to is None else 0
    return int(
        _insert_row(
            conn,
            security_id,
            attrs_json,
            event_iso,
            new_valid_to,
            knowledge_iso,
            new_is_current,
            event_iso,
        )
    )


def _fetch_current_rows(conn: sqlite3.Connection, security_id: str) -> List[sqlite3.Row]:
    cursor = conn.execute(
        "SELECT * FROM reference_data WHERE security_id = ? "
        "AND knowledge_to IS NULL ORDER BY valid_from",
        (security_id,),
    )
    return list(cursor.fetchall())


def _find_prev_row(
    rows: Sequence[sqlite3.Row], event_time: datetime
) -> Optional[sqlite3.Row]:
    prev: Optional[sqlite3.Row] = None
    for row in rows:
        if _parse_ts(row["valid_from"]) <= event_time:
            prev = row
        else:
            break
    return prev


def _find_next_row(
    rows: Sequence[sqlite3.Row], event_time: datetime
) -> Optional[sqlite3.Row]:
    for row in rows:
        if _parse_ts(row["valid_from"]) > event_time:
            return row
    return None


def _overlaps(row: sqlite3.Row, event_time: datetime) -> bool:
    start = _parse_ts(row["valid_from"])
    raw_end = row["valid_to"]
    end = _parse_ts(raw_end) if raw_end else None
    if end is None:
        return event_time >= start
    return start <= event_time < end


def _row_payload(row: sqlite3.Row) -> Dict[str, object]:
    return json.loads(row["attributes"])


def _supersede_row(
    conn: sqlite3.Connection, row: sqlite3.Row, knowledge_iso: str
) -> None:
    conn.execute(
        "UPDATE reference_data SET knowledge_to = ?, is_current = 0 "
        "WHERE id = ? AND knowledge_to IS NULL",
        (knowledge_iso, row["id"]),
    )


def _insert_row(
    conn: sqlite3.Connection,
    security_id: str,
    attrs_json: str,
    valid_from: str,
    valid_to: Optional[str],
    knowledge_from: str,
    is_current: int,
    event_time: str,
) -> bool:
    if _row_exists(conn, security_id, attrs_json, valid_from, valid_to):
        return False

    conn.execute(
        """
        INSERT INTO reference_data (
            security_id,
            attributes,
            event_time,
            valid_from,
            valid_to,
            knowledge_from,
            knowledge_to,
            is_current
        ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
        """,
        (
            security_id,
            attrs_json,
            event_time,
            valid_from,
            valid_to,
            knowledge_from,
            is_current,
        ),
    )
    return True


def _row_exists(
    conn: sqlite3.Connection,
    security_id: str,
    attrs_json: str,
    valid_from: str,
    valid_to: Optional[str],
) -> bool:
    if valid_to is None:
        query = (
            "SELECT 1 FROM reference_data WHERE security_id = ? "
            "AND attributes = ? AND valid_from = ? AND valid_to IS NULL "
            "AND knowledge_to IS NULL"
        )
        params: Tuple[object, ...] = (security_id, attrs_json, valid_from)
    else:
        query = (
            "SELECT 1 FROM reference_data WHERE security_id = ? "
            "AND attributes = ? AND valid_from = ? AND valid_to = ? "
            "AND knowledge_to IS NULL"
        )
        params = (security_id, attrs_json, valid_from, valid_to)

    cursor = conn.execute(query, params)
    return cursor.fetchone() is not None


def _parse_ts(value: object) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=DEFAULT_TZ)
    if not isinstance(value, str):
        raise TypeError(f"Unsupported timestamp type: {type(value)!r}")

    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)
    return _ensure_utc(dt)


def _format_ts(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    value = _ensure_utc(value)
    iso = value.isoformat(timespec="seconds")
    return iso.replace("+00:00", "Z")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=DEFAULT_TZ)
    return value.astimezone(DEFAULT_TZ)

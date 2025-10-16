import json
from datetime import datetime, timezone

from bitemporal import merge_logic


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def test_insert_new_entity_creates_current_row(conn):
    knowledge_time = datetime(2025, 1, 5, tzinfo=timezone.utc)
    record = {
        "security_id": "EQ1",
        "event_time": "2025-01-01T00:00:00Z",
        "name": "Alpha",
        "status": "ACTIVE",
    }

    summary = merge_logic.merge_records(conn, [record], knowledge_time=knowledge_time)

    assert summary.inserted_rows == 1
    rows = conn.execute("SELECT * FROM reference_data").fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["security_id"] == "EQ1"
    assert row["valid_to"] is None
    assert row["knowledge_from"] == _iso(knowledge_time)
    assert row["knowledge_to"] is None
    assert row["is_current"] == 1
    payload = json.loads(row["attributes"])
    assert payload["status"] == "ACTIVE"


def test_update_creates_history_and_marks_current(conn):
    record_initial = {
        "security_id": "EQ1",
        "event_time": "2025-01-01T00:00:00Z",
        "status": "ACTIVE",
    }
    record_update = {
        "security_id": "EQ1",
        "event_time": "2025-03-01T00:00:00Z",
        "status": "INACTIVE",
    }
    knowledge_initial = datetime(2025, 1, 5, tzinfo=timezone.utc)
    knowledge_update = datetime(2025, 3, 5, tzinfo=timezone.utc)

    merge_logic.merge_records(conn, [record_initial], knowledge_time=knowledge_initial)
    merge_logic.merge_records(conn, [record_update], knowledge_time=knowledge_update)

    rows = conn.execute(
        "SELECT * FROM reference_data ORDER BY id"
    ).fetchall()
    assert len(rows) == 3

    # Original record is now historical
    assert rows[0]["knowledge_to"] == _iso(knowledge_update)
    assert rows[0]["is_current"] == 0

    # Newly inserted segment representing historical period
    assert rows[1]["valid_to"] == record_update["event_time"]
    assert rows[1]["knowledge_from"] == _iso(knowledge_update)
    assert rows[1]["is_current"] == 0

    # Latest row is current with INACTIVE status
    payload_current = json.loads(rows[2]["attributes"])
    assert rows[2]["knowledge_from"] == _iso(knowledge_update)
    assert rows[2]["knowledge_to"] is None
    assert rows[2]["is_current"] == 1
    assert payload_current["status"] == "INACTIVE"


def test_backfill_splits_timeline(conn):
    record_initial = {
        "security_id": "EQ1",
        "event_time": "2025-01-01T00:00:00Z",
        "status": "ACTIVE",
    }
    record_latest = {
        "security_id": "EQ1",
        "event_time": "2025-03-01T00:00:00Z",
        "status": "INACTIVE",
    }
    record_backfill = {
        "security_id": "EQ1",
        "event_time": "2025-02-15T00:00:00Z",
        "status": "ON_HOLD",
    }

    merge_logic.merge_records(
        conn,
        [record_initial],
        knowledge_time=datetime(2025, 1, 5, tzinfo=timezone.utc),
    )
    merge_logic.merge_records(
        conn,
        [record_latest],
        knowledge_time=datetime(2025, 3, 5, tzinfo=timezone.utc),
    )
    merge_logic.merge_records(
        conn,
        [record_backfill],
        knowledge_time=datetime(2025, 4, 1, tzinfo=timezone.utc),
        allow_late_arrivals=True,
    )

    rows = conn.execute(
        "SELECT attributes, valid_from, valid_to, knowledge_from, knowledge_to, is_current "
        "FROM reference_data WHERE security_id = 'EQ1'"
    ).fetchall()

    # Expect at least one row covering the corrected historical segment
    assert any(
        row["valid_from"] == "2025-02-15T00:00:00Z"
        and row["valid_to"] == "2025-03-01T00:00:00Z"
        and json.loads(row["attributes"]) == {"status": "ON_HOLD"}
        for row in rows
    )

    # Original assumption retained with updated knowledge timeline
    assert any(
        row["valid_from"] == "2025-01-01T00:00:00Z"
        and row["valid_to"] == "2025-02-15T00:00:00Z"
        for row in rows
    )

    # Latest row remains the active knowledge
    assert any(row["is_current"] == 1 for row in rows)

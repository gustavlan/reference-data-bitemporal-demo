from datetime import datetime, timezone

from bitemporal import merge_logic


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_watermark_prevents_unintentional_reprocessing(conn):
    initial_record = {
        "security_id": "EQ1",
        "event_time": "2025-01-01T00:00:00Z",
        "status": "ACTIVE",
    }
    correction_record = {
        "security_id": "EQ1",
        "event_time": "2025-01-01T00:00:00Z",
        "status": "CORRECTED",
    }

    merge_logic.merge_records(
        conn,
        [initial_record],
        knowledge_time=_dt("2025-01-02T00:00:00Z"),
    )

    watermark = merge_logic.get_event_watermark(conn)
    assert watermark == _dt("2025-01-01T00:00:00Z")

    summary_skip = merge_logic.merge_records(
        conn,
        [correction_record],
        knowledge_time=_dt("2025-02-01T00:00:00Z"),
        allow_late_arrivals=False,
    )
    assert summary_skip.skipped_as_late == 1
    assert summary_skip.inserted_rows == 0

    rows_after_skip = conn.execute("SELECT COUNT(*) FROM reference_data").fetchone()[0]
    assert rows_after_skip == 1

    summary_apply = merge_logic.merge_records(
        conn,
        [correction_record],
        knowledge_time=_dt("2025-03-01T00:00:00Z"),
        allow_late_arrivals=True,
    )
    assert summary_apply.inserted_rows == 1

    rows = conn.execute(
        "SELECT attributes, knowledge_to FROM reference_data ORDER BY knowledge_from"
    ).fetchall()
    assert rows[0]["knowledge_to"] == "2025-03-01T00:00:00Z"
    assert rows[-1]["knowledge_to"] is None
    assert merge_logic.get_event_watermark(conn) == _dt("2025-01-01T00:00:00Z")

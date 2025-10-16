from datetime import datetime, timezone

from bitemporal import merge_logic


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_query_as_of_respects_knowledge_and_event_timelines(conn):
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
        knowledge_time=_dt("2025-01-05T00:00:00Z"),
    )
    merge_logic.merge_records(
        conn,
        [record_latest],
        knowledge_time=_dt("2025-03-05T00:00:00Z"),
    )
    merge_logic.merge_records(
        conn,
        [record_backfill],
        knowledge_time=_dt("2025-04-01T00:00:00Z"),
        allow_late_arrivals=True,
    )

    snapshot_before_update = merge_logic.query_as_of(
        conn,
        knowledge_time=_dt("2025-03-01T00:00:00Z"),
    )
    assert snapshot_before_update[0]["attributes"]["status"] == "ACTIVE"

    snapshot_after_update = merge_logic.query_as_of(
        conn,
        knowledge_time=_dt("2025-03-10T00:00:00Z"),
    )
    assert snapshot_after_update[0]["attributes"]["status"] == "INACTIVE"

    pre_correction_view = merge_logic.query_as_of(
        conn,
        knowledge_time=_dt("2025-03-10T00:00:00Z"),
        effective_time=_dt("2025-02-20T00:00:00Z"),
    )
    assert pre_correction_view[0]["attributes"]["status"] == "ACTIVE"

    post_correction_view = merge_logic.query_as_of(
        conn,
        knowledge_time=_dt("2025-04-10T00:00:00Z"),
        effective_time=_dt("2025-02-20T00:00:00Z"),
    )
    assert post_correction_view[0]["attributes"]["status"] == "ON_HOLD"

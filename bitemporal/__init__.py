"""Bi-temporal merge logic package."""

from .merge_logic import (
    initialize_db,
    merge_records,
    query_as_of,
    get_event_watermark,
    update_event_watermark,
)

__all__ = [
    "initialize_db",
    "merge_records",
    "query_as_of",
    "get_event_watermark",
    "update_event_watermark",
]

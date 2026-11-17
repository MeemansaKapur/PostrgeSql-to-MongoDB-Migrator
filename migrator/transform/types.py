"""PostgreSQL value -> BSON value. One function per type, used by bulk AND CDC.

Values arrive in two shapes:
  - from psycopg (bulk, verify): Decimal, datetime, date, int, str
  - from Debezium JSON (CDC):    numeric as str ("45210.50"), DATE as int days since
                                 epoch, TIMESTAMPTZ as ISO-8601 str with microseconds
Both must produce the identical BSON value, or verification would report drift.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from bson.decimal128 import Decimal128
from bson.int64 import Int64

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

NUMERIC = {"numeric", "decimal"}
TIMESTAMPS = {"timestamp with time zone", "timestamp without time zone"}


def to_decimal128(v) -> Decimal128:
    if isinstance(v, Decimal):
        return Decimal128(v)
    if isinstance(v, float):  # never expected; str() avoids binary float artefacts
        return Decimal128(Decimal(str(v)))
    return Decimal128(Decimal(str(v)))


def truncate_ms(dt: datetime) -> datetime:
    """MongoDB stores milliseconds; PostgreSQL keeps microseconds. Truncate (not round)."""
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def to_datetime(v) -> datetime:
    if isinstance(v, datetime):
        dt = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    elif isinstance(v, str):
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    elif isinstance(v, int):  # Debezium MicroTimestamp (timestamp without time zone)
        dt = EPOCH + timedelta(microseconds=v)
    else:
        raise TypeError(f"cannot convert {type(v).__name__} {v!r} to datetime")
    return truncate_ms(dt.astimezone(timezone.utc))


def date_to_datetime(v) -> datetime:
    """MongoDB has no date-only type: store 00:00 UTC."""
    if isinstance(v, datetime):
        v = v.date()
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
    if isinstance(v, int):  # Debezium io.debezium.time.Date = days since epoch
        return EPOCH + timedelta(days=v)
    if isinstance(v, str):
        d = date.fromisoformat(v)
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    raise TypeError(f"cannot convert {type(v).__name__} {v!r} to date")


def convert_value(v, pg_type: str | None):
    if v is None:
        return None
    if pg_type in NUMERIC:
        return to_decimal128(v)
    if pg_type in TIMESTAMPS:
        return to_datetime(v)
    if pg_type == "date":
        return date_to_datetime(v)
    if pg_type == "bigint":
        return Int64(v)
    if pg_type in ("integer", "smallint"):
        return int(v)
    return v  # text, varchar, char, boolean: already native


def convert_row(row: dict, columns: dict[str, str]) -> dict:
    """Convert every column. Columns not in the mapping (added mid-migration) pass through."""
    return {k: convert_value(v, columns.get(k)) for k, v in row.items()}

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from bson.decimal128 import Decimal128
from bson.int64 import Int64

from migrator.transform.types import convert_value, date_to_datetime, to_datetime

UTC = timezone.utc


@pytest.mark.parametrize("s", ["0.10", "-0.01", "45210.50", "9999999999999.99", "0.00"])
def test_numeric_string_is_exact(s):
    assert convert_value(s, "numeric") == Decimal128(s)
    assert str(convert_value(s, "numeric").to_decimal()) == s  # scale preserved


def test_numeric_from_psycopg_matches_debezium():
    assert convert_value(Decimal("45210.50"), "numeric") == convert_value("45210.50", "numeric")


def test_debezium_date_days_since_epoch():
    assert date_to_datetime(0) == datetime(1970, 1, 1, tzinfo=UTC)
    assert date_to_datetime(19782) == datetime(2024, 2, 29, tzinfo=UTC)   # leap day


def test_date_from_psycopg_matches_debezium():
    assert convert_value(date(2024, 2, 29), "date") == convert_value(19782, "date")


def test_timestamptz_truncates_to_ms_not_rounds():
    got = to_datetime("2024-01-09T10:22:31.512999Z")
    assert got == datetime(2024, 1, 9, 10, 22, 31, 512000, tzinfo=UTC)


def test_timestamptz_from_psycopg_matches_debezium():
    from zoneinfo import ZoneInfo
    pg = datetime(2024, 1, 9, 15, 52, 31, 512345, tzinfo=ZoneInfo("Asia/Kolkata"))
    assert convert_value(pg, "timestamp with time zone") == \
        convert_value("2024-01-09T10:22:31.512345Z", "timestamp with time zone")


def test_timestamptz_without_fraction():
    assert to_datetime("2024-01-09T10:22:31Z") == datetime(2024, 1, 9, 10, 22, 31, tzinfo=UTC)


def test_bigint_is_int64_and_nulls_pass():
    assert isinstance(convert_value(700001, "bigint"), Int64)
    for t in ("numeric", "date", "timestamp with time zone", "text", "bigint"):
        assert convert_value(None, t) is None


def test_unknown_type_passes_through():
    assert convert_value("$500 refund", "character varying") == "$500 refund"

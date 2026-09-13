from datetime import date

import pytest

from bikeshare.config import city, month_range, month_start


def test_month_range_crosses_year():
    assert month_range("2025-11", "2026-02") == ["2025-11", "2025-12", "2026-01", "2026-02"]


def test_month_range_single_month():
    assert month_range("2025-06", "2025-06") == ["2025-06"]


def test_month_range_rejects_reversed_range():
    with pytest.raises(ValueError):
        month_range("2025-06", "2025-01")


def test_month_start():
    assert month_start("2025-02") == date(2025, 2, 1)


def test_city_lookup_is_case_insensitive():
    assert city("jc").trip_file_prefix == "JC-"
    with pytest.raises(ValueError):
        city("LA")

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.materialized_views import days_to_ranges, extract_items, _range_match


def test_days_to_ranges_merges_consecutive_days():
    r = days_to_ranges(["2025-02-24", "2025-02-25", "2025-03-01"])
    assert r == [("2025-02-24", "2025-02-26"), ("2025-03-01", "2025-03-02")]


def test_days_to_ranges_ignores_garbage():
    assert days_to_ranges(["", "bad", "2025-01-31"]) == [("2025-01-31", "2025-02-01")]


def test_range_match_none_means_everything():
    assert _range_match("x", None) == {}
    assert "$or" in _range_match("x", [("a", "b"), ("c", "d")])


def test_extract_items_basic_and_missing_fields():
    items = extract_items('[{"sku": "A1", "qty": 2, "price": 10}, {"sku": "B2"}, {"foo": 1}]')
    assert [i["product"] for i in items] == ["A1", "B2"]
    assert items[0]["quantity"] == 2 and items[0]["revenue"] == 20
    assert items[1]["quantity"] == 1


def test_extract_items_bad_json_is_empty():
    assert extract_items("not-json") == []
    assert extract_items("") == []
    assert extract_items('{"items": [{"product_id": 7, "quantity": "3"}]}')[0]["quantity"] == 3

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import QuarantineErrorCodes as QEC, QualityStatus
from src.classification import classify_record


def _fake_raw_doc(record_raw: dict, id_run="run-test", number_row_source=1) -> dict:
    """Builds a minimal fake orders_raw document around a given record_raw."""
    return {
        "id_run": id_run,
        "file_source": "fake.csv",
        "number_row_source": number_row_source,
        "at_ingested": "2025-01-01T00:00:00+00:00",
        "engine_used": "python_batch",
        "record_raw": record_raw,
    }


def _base_record(**overrides) -> dict:
    """A fully clean, valid-by-default record_raw - individual fields are overridden per test."""
    record = {
        "order_id": "ORD-1",
        "order_date": "2025-02-24T21:29:00",
        "status": "confirmed",
        "customer_id": "CUST-1",
        "customer_name": "Test Customer",
        "customer_phone": "+967777123456",
        "customer_email": "test@example.com",
        "city": "Sana'a",
        "district": "Some District",
        "delivery_type": "home",
        "delivery_cost": "1000",
        "payment_method": "cash",
        "payment_status": "paid",
        "payment_amount": "5000",
        "currency": "YER",
        "total_amount": "5000",
        "items_json": '[{"sku": "A1", "qty": 1}]',
    }
    record.update(overrides)
    return record


def test_classify_fully_clean_record_is_valid():
    raw_doc = _fake_raw_doc(_base_record())
    result = classify_record(raw_doc)

    assert result["quality_status"] == QualityStatus.VALID
    assert result["validated_doc"] is not None
    assert result["quarantine_doc"] is None
    assert result["validated_doc"]["corrections"] == []
    assert result["validated_doc"]["id_order"] == "ORD-1"


def test_classify_record_needing_one_safe_correction_is_corrected():
    # Arabic digits in delivery_cost get safely converted -> "corrected", not "quarantined"
    raw_doc = _fake_raw_doc(_base_record(delivery_cost="١٠٠٠"))
    result = classify_record(raw_doc)

    assert result["quality_status"] == QualityStatus.CORRECTED
    assert result["validated_doc"] is not None
    assert len(result["validated_doc"]["corrections"]) >= 1
    assert result["quarantine_doc"] is None


def test_classify_record_with_single_error_is_quarantined():
    raw_doc = _fake_raw_doc(_base_record(order_id=""))
    result = classify_record(raw_doc)

    assert result["quality_status"] == QualityStatus.QUARANTINED
    assert result["validated_doc"] is None
    assert result["quarantine_doc"] is not None
    assert QEC.ID_ORDER_MISSING in result["quarantine_doc"]["codes_error"]
    assert QEC.ERRORS_CONFLICTING_MULTIPLE not in result["quarantine_doc"]["codes_error"]
    # The complete original raw record must never be dropped.
    assert result["quarantine_doc"]["record_raw"]["order_id"] == ""


def test_classify_record_with_multiple_conflicting_errors_flags_conflict():
    raw_doc = _fake_raw_doc(_base_record(order_id="", customer_id=""))
    result = classify_record(raw_doc)

    assert result["quality_status"] == QualityStatus.QUARANTINED
    codes = result["quarantine_doc"]["codes_error"]
    assert QEC.ID_ORDER_MISSING in codes
    assert QEC.ID_CUSTOMER_MISSING in codes
    assert QEC.ERRORS_CONFLICTING_MULTIPLE in codes
    # individual codes must be kept, not replaced by the conflict marker
    assert len(codes) == 3


def test_classify_quarantine_doc_id_order_is_none_when_order_id_itself_missing():
    raw_doc = _fake_raw_doc(_base_record(order_id=""))
    result = classify_record(raw_doc)

    assert result["quarantine_doc"]["id_order"] is None


def test_classify_details_error_is_human_readable_not_bare_codes():
    raw_doc = _fake_raw_doc(_base_record(items_json="not-json"))
    result = classify_record(raw_doc)

    details = result["quarantine_doc"]["details_error"]
    assert any(d["code"] == QEC.JSON_ITEMS_CORRUPTED for d in details)
    for entry in details:
        assert "detail" in entry and isinstance(entry["detail"], str) and len(entry["detail"]) > 0

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import QuarantineErrorCodes as QEC, QualityStatus
from src.quality_rules import apply_quality_rules


# Human-readable explanation per quarantine error code, used to build
# quarantine_orders.details_error (never just bare codes).
ERROR_DETAILS = {
    QEC.ID_ORDER_MISSING: "order_id is missing or blank.",
    QEC.ID_CUSTOMER_MISSING: "customer_id is missing or blank.",
    QEC.DATE_IMPOSSIBLE_INVALID: "order_date could not be parsed as a valid date/time in any recognized format.",
    QEC.JSON_ITEMS_CORRUPTED: "items_json is not valid, well-formed JSON.",
    QEC.ITEMS_EMPTY: "items_json is empty or contains no items.",
    QEC.PRICE_UNKNOWN: "total_amount is missing or could not be parsed as a number.",
    QEC.VALUE_NEGATIVE_AMBIGUOUS: "A numeric amount field (delivery_cost / payment_amount / total_amount) is "
                                   "negative; the correct value is ambiguous and was not guessed.",
    QEC.ID_ORDER_DUPLICATE: "order_id already exists in orders_validated (duplicate business key).",
    QEC.ERRORS_CONFLICTING_MULTIPLE: "More than one quarantine condition applies to this record at the same time.",
}


def _build_details_error(codes: list) -> list:
    """Turns a list of error codes into a human-readable details list, one entry per code."""
    return [
        {"code": code, "detail": ERROR_DETAILS.get(code, "No description available for this code.")}
        for code in codes
    ]


def classify_record(raw_doc: dict) -> dict:
   
    record_raw = raw_doc.get("record_raw", {})
    id_run = raw_doc.get("id_run")
    number_row_source = raw_doc.get("number_row_source")

    result = apply_quality_rules(record_raw)
    cleaned_record = result["cleaned_record"]
    corrections = result["corrections"]
    quarantine_errors = list(result["quarantine_errors"])  # copy - never mutate quality_rules' output

    
    distinct_codes = set(quarantine_errors)
    if len(distinct_codes) > 1:
        quarantine_errors.append(QEC.ERRORS_CONFLICTING_MULTIPLE)

    if quarantine_errors:
        quality_status = QualityStatus.QUARANTINED
    elif corrections:
        quality_status = QualityStatus.CORRECTED
    else:
        quality_status = QualityStatus.VALID

    validated_doc = None
    quarantine_doc = None

    if quality_status in (QualityStatus.VALID, QualityStatus.CORRECTED):
        validated_doc = {
            "id_order": cleaned_record.get("order_id"),
            "id_run": id_run,
            "number_row_source": number_row_source,
            "quality_status": quality_status,
            "record_cleaned": cleaned_record,
            "corrections": corrections,  # empty list if none, per spec
        }
    else:
        quarantine_doc = {
            "id_order": cleaned_record.get("order_id") or None,
            "id_run": id_run,
            "number_row_source": number_row_source,
            "codes_error": quarantine_errors,
            "details_error": _build_details_error(quarantine_errors),
            # Complete, untouched original raw record - never dropped.
            "record_raw": record_raw,
        }

    return {
        "quality_status": quality_status,
        "validated_doc": validated_doc,
        "quarantine_doc": quarantine_doc,
        "quarantine_errors": quarantine_errors,
    }

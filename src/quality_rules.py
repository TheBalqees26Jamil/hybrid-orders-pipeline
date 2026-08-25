import json # to handle items_json field
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import TARGET_CURRENCY, QuarantineErrorCodes as QEC

_ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
_LATIN_DIGITS = "0123456789"
_ARABIC_DECIMAL_SEP = "٫"     # U+066B ARABIC DECIMAL SEPARATOR
_ARABIC_THOUSANDS_SEP = "٬"   # U+066C ARABIC THOUSANDS SEPARATOR
_ARABIC_TO_LATIN = str.maketrans(
    _ARABIC_DIGITS + _ARABIC_DECIMAL_SEP + _ARABIC_THOUSANDS_SEP,
    _LATIN_DIGITS + "." + ",",
)
_ARABIC_CHARS_TO_DETECT = _ARABIC_DIGITS + _ARABIC_DECIMAL_SEP + _ARABIC_THOUSANDS_SEP


def rule_arabic_numerals(field: str, value):
    """Converts Arabic-Indic digits and Arabic decimal/thousands separators to Latin equivalents."""
    if value is None:
        return value, None, None

    text = str(value)
    if not any(ch in _ARABIC_CHARS_TO_DETECT for ch in text):
        return value, None, None  # nothing to convert

    converted = text.translate(_ARABIC_TO_LATIN)
    correction = {
        "field": field,
        "original_value": value,
        "corrected_value": converted,
        "rule_code": "ARABIC_NUMERALS",
    }
    return converted, correction, None


# ===================== Rule 2: Currency code normalization =====================

_CURRENCY_CODE_ALIASES = {
    "لاير": "YER", "لاير يمني": "YER",
    "ريال": "YER", "ريال يمني": "YER",
    "yer": "YER", "YER": "YER",
}


def rule_currency_code(field: str, value):
    """Normalizes known currency text/aliases to TARGET_CURRENCY. Unknown text is left untouched (never guessed)."""
    if value is None or str(value).strip() == "":
        return value, None, None

    text = str(value).strip()
    normalized = _CURRENCY_CODE_ALIASES.get(text) or _CURRENCY_CODE_ALIASES.get(text.upper())

    if normalized is None:
        return value, None, None  # unrecognized currency text -> leave as-is, do not guess

    if normalized == text:
        return value, None, None  # already normalized

    correction = {
        "field": field,
        "original_value": value,
        "corrected_value": normalized,
        "rule_code": "CURRENCY_NORMALIZED",
    }
    return normalized, correction, None


# ===================== Rule 3: Thousands separators =====================
def rule_thousands_separator(field: str, value):
    """Removes thousands separators (commas) and converts to a plain number string."""
    if value is None:
        return value, None, None

    text = str(value)
    if "," not in text:
        return value, None, None

    cleaned = text.replace(",", "")
    if not re.match(r"^-?\d+(\.\d+)?$", cleaned):
        return value, None, None  # not a clean number even after removing commas -> leave untouched

    correction = {
        "field": field,
        "original_value": value,
        "corrected_value": cleaned,
        "rule_code": "THOUSANDS_SEPARATOR_REMOVED",
    }
    return cleaned, correction, None


# ===================== Rule 4: Price written in words =====================

_KNOWN_PRICE_WORDS = {
    "ألف": 1000, "الف": 1000,
    "ألفان": 2000, "الفان": 2000,
    "ثلاثة آلاف": 3000, "ثلاثه الاف": 3000,
    "أربعة آلاف": 4000, "اربعه الاف": 4000,
    "خمسة آلاف": 5000, "خمسه الاف": 5000,
    "عشرة آلاف": 10000, "عشره الاف": 10000,
}


def rule_price_words(field: str, value):
    """Converts known Arabic word-numbers to numeric values. Unknown phrasing is left untouched."""
    if value is None:
        return value, None, None

    text = str(value).strip()
    if text not in _KNOWN_PRICE_WORDS:
        return value, None, None

    converted = _KNOWN_PRICE_WORDS[text]
    correction = {
        "field": field,
        "original_value": value,
        "corrected_value": converted,
        "rule_code": "PRICE_WORDS_CONVERTED",
    }
    return converted, correction, None


# ===================== Rule 5: Phone number =====================
def rule_phone(field: str, value):
    """Removes spaces/dashes and normalizes an unambiguous phone number format."""
    if value is None or str(value).strip() == "":
        return value, None, None

    text = str(value)
    cleaned = re.sub(r"[\s\-()]+", "", text)

    if cleaned == text:
        return value, None, None  # already clean, nothing to fix

    if not re.match(r"^\+?\d{7,15}$", cleaned):
        return value, None, None  # not unambiguously a phone number -> leave as-is

    correction = {
        "field": field,
        "original_value": value,
        "corrected_value": cleaned,
        "rule_code": "PHONE_NORMALIZED",
    }
    return cleaned, correction, None


# ===================== Rule 6: Email =====================
def rule_email(field: str, value):
   
    if value is None or str(value).strip() == "":
        return value, None, None

    text = str(value).strip()
    fixed = re.sub(r"@{2,}", "@", text)
    fixed = re.sub(r"\.{2,}", ".", fixed)

    email_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

    if fixed != text:
        if re.match(email_pattern, fixed):
            correction = {
                "field": field,
                "original_value": value,
                "corrected_value": fixed,
                "rule_code": "EMAIL_REPEATED_SYMBOLS",
            }
            return fixed, correction, None
        # still invalid even after the obvious fix -> cannot be safely corrected
        return value, None, None

    return value, None, None  # already valid, or invalid in a non-obvious way -> leave untouched

STANDARD_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
_DATE_FORMATS_WITH_TIME = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]
_DATE_FORMATS_DATE_ONLY = ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"]


def rule_date(field: str, value):
    """Converts a recognizably-formatted date/datetime to one standard ISO format (YYYY-MM-DDTHH:MM:SS)."""
    if value is None or str(value).strip() == "":
        return value, None, QEC.DATE_IMPOSSIBLE_INVALID

    text = str(value).strip()

    for fmt in _DATE_FORMATS_WITH_TIME:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        standardized = parsed.strftime(STANDARD_DATETIME_FORMAT)
        if standardized == text:
            return value, None, None  # already standard, nothing to fix
        correction = {
            "field": field,
            "original_value": value,
            "corrected_value": standardized,
            "rule_code": "DATE_STANDARDIZED",
        }
        return standardized, correction, None

    for fmt in _DATE_FORMATS_DATE_ONLY:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        standardized = parsed.strftime(STANDARD_DATETIME_FORMAT)  # midnight time appended explicitly
        correction = {
            "field": field,
            "original_value": value,
            "corrected_value": standardized,
            "rule_code": "DATE_STANDARDIZED",
        }
        return standardized, correction, None

    # Could not be parsed by any recognized format -> unresolvable, must be quarantined
    return value, None, QEC.DATE_IMPOSSIBLE_INVALID


# ===================== Rule 8: Whitespace & synonyms =====================
# Applies to the two status-like fields: "status" and "payment_status".
_SYNONYM_DICTIONARY = {
    "مؤكد": "confirmed", "تم التأكيد": "confirmed",
    "مدفوع": "paid", "تم الدفع": "paid",
    "ملغي": "cancelled", "ملغى": "cancelled",
    "قيد الانتظار": "pending", "معلق": "pending",
    "مسترد": "refunded",
}


def rule_whitespace_synonyms(field: str, value):
    """Trims whitespace and normalizes known status synonyms to a standard dictionary value."""
    if value is None:
        return value, None, None

    text = str(value)
    trimmed = text.strip()
    normalized = _SYNONYM_DICTIONARY.get(trimmed, trimmed)

    if normalized == text:
        return value, None, None

    correction = {
        "field": field,
        "original_value": value,
        "corrected_value": normalized,
        "rule_code": "WHITESPACE_SYNONYM_NORMALIZED",
    }
    return normalized, correction, None


# ===================== Rule 9: items_json validity =====================
def check_items_json(field: str, value):
    # check if json is valid and not empty, if not return the appropriate quarantine error code
    if value is None or str(value).strip() == "":
        return value, None, QEC.ITEMS_EMPTY

    text = str(value)
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return value, None, QEC.JSON_ITEMS_CORRUPTED

    if isinstance(parsed, list) and len(parsed) == 0:
        return value, None, QEC.ITEMS_EMPTY

    return value, None, None


# ===================== Rule 10: negative/ambiguous numeric values =====================
def rule_flag_negative_value(field: str, value):
    
    if value is None or str(value).strip() == "":
        return value, None, None

    try:
        numeric_value = float(str(value).strip())
    except ValueError:
        return value, None, None  # not parseable as a number here -> handled by other checks

    if numeric_value < 0:
        return value, None, QEC.VALUE_NEGATIVE_AMBIGUOUS

    return value, None, None


def rule_flag_price_unknown(field: str, value):
    
    if value is None or str(value).strip() == "":
        return value, None, QEC.PRICE_UNKNOWN

    try:
        float(str(value).strip())
    except ValueError:
        return value, None, QEC.PRICE_UNKNOWN

    return value, None, None


# ===================== Rule 11: required id fields =====================

def rule_check_order_id_missing(field: str, value):
    """Flags a missing/blank order_id. Never invents an id."""
    if value is None or str(value).strip() == "":
        return value, None, QEC.ID_ORDER_MISSING
    return value, None, None


def rule_check_customer_id_missing(field: str, value):
    """Flags a missing/blank customer_id. Never invents an id."""
    if value is None or str(value).strip() == "":
        return value, None, QEC.ID_CUSTOMER_MISSING
    return value, None, None


# ===================== Registry of all rules =====================

FIELD_RULES = {
    "order_id": [rule_check_order_id_missing],
    "customer_id": [rule_check_customer_id_missing],
    "customer_phone": [rule_arabic_numerals, rule_phone],
    "customer_email": [rule_email],
    "order_date": [rule_date],
    "status": [rule_whitespace_synonyms],
    "payment_status": [rule_whitespace_synonyms],
    "currency": [rule_currency_code],
    "delivery_cost": [rule_arabic_numerals, rule_thousands_separator, rule_price_words, rule_flag_negative_value],
    "payment_amount": [rule_arabic_numerals, rule_thousands_separator, rule_price_words, rule_flag_negative_value],
    "total_amount": [rule_arabic_numerals, rule_thousands_separator, rule_price_words,
                      rule_flag_price_unknown, rule_flag_negative_value],
    "items_json": [check_items_json],
}


def apply_quality_rules(record: dict) -> dict:
   
    cleaned_record = dict(record)
    corrections = []
    quarantine_errors = []

    for field, rules in FIELD_RULES.items():
        if field not in cleaned_record:
            continue
        current_value = cleaned_record[field]
        for rule_fn in rules:
            current_value, correction, error_code = rule_fn(field, current_value)
            if correction is not None:
                corrections.append(correction)
            if error_code is not None:
                quarantine_errors.append(error_code)
        cleaned_record[field] = current_value

    return {
        "cleaned_record": cleaned_record,
        "corrections": corrections,
        "quarantine_errors": quarantine_errors,
    }

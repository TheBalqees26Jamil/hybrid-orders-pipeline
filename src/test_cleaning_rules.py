import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import QuarantineErrorCodes as QEC
from src.quality_rules import (
    rule_arabic_numerals,
    rule_currency_code,
    rule_thousands_separator,
    rule_phone,
    rule_email,
    rule_date,
    rule_flag_negative_value,
    rule_check_order_id_missing,
    rule_check_customer_id_missing,
    STANDARD_DATETIME_FORMAT,
)


# ------------------------- Arabic numerals + decimal separator -------------------------

def test_rule_arabic_numerals_converts_digits_and_decimal_separator():
    # Verified real-data example from Phase 0: "٧٠٦٠٠٠٫٠" -> "706000.0"
    value, correction, error = rule_arabic_numerals("total_amount", "٧٠٦٠٠٠٫٠")
    assert value == "706000.0"
    assert correction is not None
    assert correction["rule_code"] == "ARABIC_NUMERALS"
    assert error is None


def test_rule_arabic_numerals_leaves_plain_latin_value_untouched():
    value, correction, error = rule_arabic_numerals("total_amount", "12345.6")
    assert value == "12345.6"
    assert correction is None
    assert error is None


# ------------------------- Currency normalization -------------------------

def test_rule_currency_code_normalizes_known_alias():
    value, correction, error = rule_currency_code("currency", "ريال")
    assert value == "YER"
    assert correction is not None
    assert correction["rule_code"] == "CURRENCY_NORMALIZED"


def test_rule_currency_code_leaves_unknown_text_untouched_never_guesses():
    value, correction, error = rule_currency_code("currency", "USD-ish-thing")
    assert value == "USD-ish-thing"
    assert correction is None
    assert error is None


# ------------------------- Thousands separator -------------------------

def test_rule_thousands_separator_removes_commas():
    value, correction, error = rule_thousands_separator("payment_amount", "12,345")
    assert value == "12345"
    assert correction["rule_code"] == "THOUSANDS_SEPARATOR_REMOVED"


def test_rule_thousands_separator_leaves_ambiguous_value_untouched():
    # Not a clean number even after removing commas -> must not guess
    value, correction, error = rule_thousands_separator("payment_amount", "12,345abc")
    assert value == "12,345abc"
    assert correction is None


# ------------------------- Phone -------------------------

def test_rule_phone_removes_spaces_and_dashes():
    value, correction, error = rule_phone("customer_phone", "+967 777-123-456")
    assert value == "+967777123456"
    assert correction["rule_code"] == "PHONE_NORMALIZED"


def test_rule_phone_leaves_already_clean_value_untouched():
    value, correction, error = rule_phone("customer_phone", "+967777123456")
    assert value == "+967777123456"
    assert correction is None


# ------------------------- Email -------------------------

def test_rule_email_fixes_repeated_at_symbol():
    value, correction, error = rule_email("customer_email", "user@@example.com")
    assert value == "user@example.com"
    assert correction["rule_code"] == "EMAIL_REPEATED_SYMBOLS"


def test_rule_email_leaves_non_obviously_fixable_value_untouched():
    value, correction, error = rule_email("customer_email", "not-an-email")
    assert value == "not-an-email"
    assert correction is None


# ------------------------- Date -------------------------

def test_rule_date_accepts_real_iso_format_unchanged():
    # Real order_date format verified during Phase 0: "2025-02-24T21:29:00"
    value, correction, error = rule_date("order_date", "2025-02-24T21:29:00")
    assert value == "2025-02-24T21:29:00"
    assert correction is None
    assert error is None


def test_rule_date_standardizes_recognized_alternate_format():
    value, correction, error = rule_date("order_date", "2025-02-24 21:29:00")
    assert value == "2025-02-24T21:29:00"
    assert correction["rule_code"] == "DATE_STANDARDIZED"
    assert error is None


def test_rule_date_flags_unparseable_value_for_quarantine():
    value, correction, error = rule_date("order_date", "not-a-date-at-all")
    assert correction is None
    assert error == QEC.DATE_IMPOSSIBLE_INVALID


# ------------------------- Negative value flag -------------------------

def test_rule_flag_negative_value_flags_negative_amount():
    value, correction, error = rule_flag_negative_value("total_amount", "-150")
    assert correction is None  # never "corrected" - sign is never flipped/guessed
    assert error == QEC.VALUE_NEGATIVE_AMBIGUOUS


def test_rule_flag_negative_value_leaves_positive_amount_untouched():
    value, correction, error = rule_flag_negative_value("total_amount", "150")
    assert correction is None
    assert error is None


# ------------------------- Missing required ids -------------------------

def test_rule_check_order_id_missing_flags_blank_value():
    value, correction, error = rule_check_order_id_missing("order_id", "")
    assert error == QEC.ID_ORDER_MISSING


def test_rule_check_order_id_missing_accepts_present_value():
    value, correction, error = rule_check_order_id_missing("order_id", "ORD-123")
    assert error is None


def test_rule_check_customer_id_missing_flags_blank_value():
    value, correction, error = rule_check_customer_id_missing("customer_id", None)
    assert error == QEC.ID_CUSTOMER_MISSING

from decimal import Decimal

from solution.preprocessing import (
    normalize_date,
    normalize_decimal,
    normalize_phone,
    normalize_row,
)


MOTOR_SCHEMA = {
    "workflow_id": "motor_renewal",
    "fields": [
        {"name": "customer_name", "type": "string", "required": True},
        {"name": "phone", "type": "phone", "required": True},
        {"name": "policy_number", "type": "string", "required": True},
        {"name": "policy_expiry", "type": "date", "required": False},
        {"name": "premium_amount", "type": "decimal", "required": False},
        {
            "name": "preferred_language",
            "type": "enum",
            "required": False,
            "values": ["en", "hi", "ta"],
            "default": "en",
        },
    ],
}


def test_phone_formats_are_canonicalized():
    assert normalize_phone("+91 79328 96340") == "+917932896340"
    assert normalize_phone("06892038806") == "+916892038806"
    assert normalize_phone("5546220212") is None


def test_dates_are_day_first_and_iso_formatted():
    assert normalize_date("21-11-2026") == "2026-11-21"
    assert normalize_date("02-Nov-2026") == "2026-11-02"
    assert normalize_date("31/02/2026") is None


def test_decimal_formats_are_two_decimal_places():
    assert normalize_decimal("INR 25,301.4") == Decimal("25301.40")
    assert normalize_decimal("twelve thousand") is None


def test_row_normalization_maps_aliases_defaults_and_warnings():
    result = normalize_row(
        {
            "Customer_Name": " Aarav Menon ",
            "Mobile No": "+91 79328 96340",
            "Policy No": "MOT/2026/100142",
            "Expiry": "21-11-2026",
            "Premium": "INR 25,301.4",
            "Language": "TA",
            "ignored_column": "ignored",
        },
        MOTOR_SCHEMA,
        batch_id="batch_001",
        row_number=1,
    )

    assert result.accepted
    assert result.values["phone"] == "+917932896340"
    assert result.values["policy_expiry"] == "2026-11-21"
    assert result.values["premium_amount"] == Decimal("25301.40")
    assert result.values["preferred_language"] == "ta"
    assert result.warnings == ()


def test_optional_invalid_values_become_null_and_warn_once():
    result = normalize_row(
        {
            "customer_name": "Nikhil Rao",
            "phone": "9932302346",
            "policy_number": "MOT/2026/100343",
            "premium_amount": "N/A",
            "preferred_language": "Hindi",
        },
        MOTOR_SCHEMA,
    )

    assert result.accepted
    assert result.values["premium_amount"] is None
    assert result.values["preferred_language"] == "en"
    assert result.warnings == ("invalid_premium_amount", "invalid_preferred_language")


def test_required_fields_and_phone_are_rejected():
    missing = normalize_row(
        {"customer_name": "A", "phone": "9876543210"}, MOTOR_SCHEMA
    )
    invalid_phone = normalize_row(
        {
            "customer_name": "A",
            "phone": "12345",
            "policy_number": "P1",
        },
        MOTOR_SCHEMA,
    )

    assert missing.rejection_reason == "missing_required:policy_number"
    assert invalid_phone.rejection_reason == "invalid_phone"

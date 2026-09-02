from functions.standardization import (
    standardize_address,
    standardize_email,
    standardize_name,
    standardize_phone,
)


def test_standardize_name_uppercases_and_normalizes_whitespace():
    assert standardize_name("  robert smith  ") == "ROBERT SMITH"


def test_standardize_email_lowercases():
    assert standardize_email("Rob@Gmail.Com") == "rob@gmail.com"


def test_standardize_phone_removes_formatting():
    assert standardize_phone("+91-9876543210") == "9876543210"


def test_standardize_address_title_cases():
    assert standardize_address("12 main street") == "12 Main Street"

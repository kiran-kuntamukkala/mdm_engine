from functions.classifier import classify_column


def test_classify_email_column():
    assert classify_column("work_email") == "EMAIL"


def test_classify_phone_column():
    assert classify_column("mobile_no") == "PHONE"


def test_unknown_column_is_returned():
    assert classify_column("custom_field_123") == "UNKNOWN"

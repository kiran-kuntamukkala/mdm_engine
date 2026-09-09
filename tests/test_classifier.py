from functions.classifier import classify_column, grouped_similar_columns


def test_classify_email_column():
    assert classify_column("work_email") == "EMAIL"


def test_classify_phone_column():
    assert classify_column("mobile_no") == "PHONE"


def test_near_duplicate_columns_are_grouped_as_the_same_field():
    grouped = grouped_similar_columns(["customer_name", "cust_name", "customer_nm", "full_name", "address", "customer_address"])

    assert grouped["NAME"]
    assert "customer_name" in grouped["NAME"]
    assert "cust_name" in grouped["NAME"]
    assert "full_name" in grouped["NAME"]
    assert grouped["ADDRESS"]
    assert "address" in grouped["ADDRESS"]


def test_unknown_column_is_returned():
    assert classify_column("custom_field_123") == "UNKNOWN"

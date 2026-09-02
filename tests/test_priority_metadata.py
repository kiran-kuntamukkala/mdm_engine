from functions.metadata import pick_prioritized_value, resolve_prioritized_metadata


def test_resolve_prioritized_metadata_uses_highest_priority_column_name_and_type():
    metadata_by_source = {
        "tb1": [
            {"column_name": "customer_name", "data_type": "string"},
            {"column_name": "email_id", "data_type": "string"},
        ],
        "tb2": [
            {"column_name": "cust_name", "data_type": "string"},
            {"column_name": "mail", "data_type": "string"},
        ],
        "tb3": [
            {"column_name": "customer_name", "data_type": "varchar"},
            {"column_name": "email", "data_type": "string"},
        ],
    }

    schema = resolve_prioritized_metadata(metadata_by_source, priority_order=["tb1", "tb2", "tb3"])

    assert schema[0]["column_name"] == "customer_name"
    assert schema[0]["data_type"] == "string"
    assert schema[1]["column_name"] == "email_id"
    assert schema[1]["data_type"] == "string"
    assert any(item["column_name"] == "cust_name" for item in schema) is False


def test_pick_prioritized_value_prefers_highest_priority_available_source():
    source_rows = {
        "tb1": {"customer_name": "Robert Smith", "email_id": "robert@gmail.com"},
        "tb2": {"cust_name": "ROBERT SMITH", "mail": "robert@ymail.com"},
        "tb3": {"customer_name": "Bob Smith", "email": "robert@other.com"},
    }

    selected = pick_prioritized_value(
        source_rows,
        priority_order=["tb1", "tb2", "tb3"],
    )

    assert selected["customer_name"] == "Robert Smith"
    assert selected["email_id"] == "robert@gmail.com"

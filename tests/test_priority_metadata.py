from functions.mdm_temp import build_mdm_temp
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


def test_build_mdm_temp_returns_one_canonical_row_per_source_record():
    rows = [{
        "record_id": "CRM001",
        "customer_name": "Robert Smith",
        "email_id": "robert@gmail.com",
        "mobile_no": "+91-9876543210",
        "address": "12 Main Street",
    }]

    canonical_rows = build_mdm_temp(rows, source_system="crm_customers", entity_type="CUSTOMER")

    assert len(canonical_rows) == 1
    assert canonical_rows[0]["record_id"] == "CRM001"
    assert canonical_rows[0]["customer_name"] == "ROBERT SMITH"
    assert canonical_rows[0]["email_id"] == "robert@gmail.com"
    assert canonical_rows[0]["mobile_no"] == "9876543210"
    assert "attribute_name" not in canonical_rows[0]
    assert "attribute_value" not in canonical_rows[0]

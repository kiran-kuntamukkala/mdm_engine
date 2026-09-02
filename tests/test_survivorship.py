from functions.survivorship import apply_survivorship, merge_records


def test_longest_value_survivorship():
    records = [
        {"record_id": "A", "NAME": "ROBERT", "load_timestamp": "2024-01-01T00:00:00"},
        {"record_id": "B", "NAME": "ROBERT SMITH", "load_timestamp": "2024-01-02T00:00:00"},
    ]
    result = apply_survivorship(records, "NAME", {"NAME": "LONGEST_VALUE"})
    assert result["attribute_value"] == "ROBERT SMITH"


def test_merge_records_consolidates_attributes():
    records = [
        {"record_id": "A", "EMAIL": "robert@gmail.com", "PHONE": "9876543210"},
        {"record_id": "B", "EMAIL": "robert@gmail.com", "PHONE": "9876543210"},
    ]
    merged = merge_records(records, {"EMAIL": "MOST_RECENT", "PHONE": "MOST_RECENT"})
    assert merged["EMAIL"] == "robert@gmail.com"
    assert merged["PHONE"] == "9876543210"

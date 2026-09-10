from functions.classifier import classify_column
from functions.mdm_review import build_match_review_queue, build_quality_summary, resolve_conflict_value
from new_mdm_engine.engine import _canonical_output_name


def test_canonical_output_name_preserves_identifier_fields():
    assert _canonical_output_name("customerID", "ID") == "customer_id"
    assert _canonical_output_name("accountId", "ID") == "account_id"
    assert _canonical_output_name("record_id", "ID") == "record_id"
    assert classify_column("customerID") == "ID"
    assert classify_column("accountId") == "ID"


def test_quality_summary_reports_key_metrics():
    summary = build_quality_summary(
        row_count=100,
        column_count=5,
        null_count=20,
        duplicate_ratio=0.08,
        completeness=0.8,
    )
    assert summary["row_count"] == 100
    assert summary["null_rate"] == 0.2
    assert summary["duplicate_rate"] == 0.08
    assert summary["completeness_score"] == 0.8


def test_conflict_resolution_prefers_high_priority_value():
    values = [
        {"source_priority": 1, "value": "ROBERT SMITH"},
        {"source_priority": 3, "value": "Robert Smith"},
    ]
    assert resolve_conflict_value(values, strategy="highest_priority") == "ROBERT SMITH"


def test_match_review_queue_uses_candidate_scores():
    review = build_match_review_queue([
        {
            "entity_id": "E-1",
            "source_count": 2,
            "match_score": 0.91,
            "conflict_fields": ["customer_name", "email"],
            "review_status": "pending",
        }
    ])
    assert review[0]["entity_id"] == "E-1"
    assert review[0]["match_score"] == 0.91
    assert review[0]["review_status"] == "pending"

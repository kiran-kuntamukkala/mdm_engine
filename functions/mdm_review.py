from __future__ import annotations

from typing import Any, Iterable


def build_match_review_queue(matches: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return a normalized review queue for candidate merges and ambiguous records."""
    review_rows: list[dict[str, Any]] = []
    for item in matches:
        review_rows.append({
            "entity_id": item.get("entity_id"),
            "source_count": int(item.get("source_count") or 0),
            "match_score": float(item.get("match_score") or 0.0),
            "conflict_fields": item.get("conflict_fields") or [],
            "review_status": item.get("review_status") or "pending",
            "reason": item.get("reason") or "Potential duplicate cluster requires review",
        })
    return sorted(review_rows, key=lambda row: (-row["match_score"], row["entity_id"] or ""))


def build_quality_summary(
    row_count: int,
    column_count: int,
    null_count: int,
    duplicate_ratio: float,
    completeness: float,
) -> dict[str, float | int]:
    """Build a compact data-quality summary for observability and governance dashboards."""
    row_count = int(row_count)
    column_count = int(column_count)
    null_count = int(null_count)
    null_rate = (null_count / row_count) if row_count else 0.0
    completeness_score = float(completeness)
    return {
        "row_count": row_count,
        "column_count": column_count,
        "null_count": null_count,
        "null_rate": round(null_rate, 4),
        "duplicate_rate": float(duplicate_ratio),
        "completeness_score": round(completeness_score, 4),
    }


def resolve_conflict_value(values: Iterable[dict[str, Any]], strategy: str = "highest_priority") -> Any:
    """Pick a winner among competing source values using a simple policy."""
    values = list(values)
    if not values:
        return None
    if strategy == "highest_priority":
        winner = min(values, key=lambda item: int(item.get("source_priority", 999)))
        return winner.get("value")
    if strategy == "most_recent":
        return max(values, key=lambda item: item.get("updated_at") or "").get("value")
    return values[0].get("value")

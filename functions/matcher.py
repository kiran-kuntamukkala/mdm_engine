from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from rapidfuzz import fuzz

from functions.utils import get_logger, load_json_config

logger = get_logger(__name__)


def exact_match(value_a: Any, value_b: Any) -> bool:
    """Compare values for exact equality after normalization."""
    try:
        if value_a is None or value_b is None:
            return False
        return str(value_a).strip() == str(value_b).strip()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Exact match failed for values '%s' and '%s': %s", value_a, value_b, exc)
        return False


def fuzzy_match(value_a: Any, value_b: Any, scorer: str = "ratio") -> float:
    """Return a normalized fuzzy similarity score between two strings."""
    try:
        if value_a is None or value_b is None:
            return 0.0

        left = str(value_a).strip()
        right = str(value_b).strip()
        if not left or not right:
            return 0.0

        strategy = getattr(fuzz, scorer, fuzz.ratio)
        return float(strategy(left, right))
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Fuzzy match failed for values '%s' and '%s': %s", value_a, value_b, exc)
        return 0.0


def calculate_match_score(field_type: str, value_a: Any, value_b: Any, match_rules: Dict[str, int] | None = None) -> float:
    """Calculate a weighted match score based on config-defined attribute weights."""
    try:
        if match_rules is None:
            match_rules = load_json_config("matching_rules.json")

        if field_type == "EMAIL":
            return 100.0 if exact_match(value_a, value_b) else 0.0
        if field_type == "PHONE":
            return 100.0 if exact_match(value_a, value_b) else 0.0
        if field_type in {"NAME", "ADDRESS"}:
            score = fuzzy_match(value_a, value_b)
            threshold = min(float(match_rules.get(field_type, match_rules.get("DEFAULT", 10))), 100.0)
            return float(score if score >= threshold else 0.0)

        match_score = fuzzy_match(value_a, value_b)
        threshold = min(float(match_rules.get("DEFAULT", 10)), 100.0)
        return float(match_score if match_score >= threshold else 0.0)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception(
            "Match score calculation failed for field '%s' with values '%s' and '%s': %s",
            field_type,
            value_a,
            value_b,
            exc,
        )
        return 0.0


def build_match_candidates(records: Iterable[Dict[str, Any]], match_rules: Dict[str, int] | None = None) -> List[Dict[str, Any]]:
    """Create candidate matches between records using metadata-driven similarity rules."""
    try:
        records = list(records)
        candidates: List[Dict[str, Any]] = []

        for idx, left in enumerate(records):
            for right in records[idx + 1 :]:
                score = 0.0
                matched_fields: List[str] = []

                for field_name in set(left.keys()) | set(right.keys()):
                    if field_name in {"record_id", "source_system", "entity_type", "load_timestamp"}:
                        continue

                    left_val = left.get(field_name)
                    right_val = right.get(field_name)
                    if left_val is None or right_val is None:
                        continue

                    field_type = str(field_name).upper()
                    field_score = calculate_match_score(field_type, left_val, right_val, match_rules=match_rules)
                    if field_score > 0:
                        matched_fields.append(field_name)
                        score += field_score

                status = "MATCH" if score > 0 else "NO_MATCH"
                if status == "MATCH":
                    candidates.append(
                        {
                            "record_id_1": left.get("record_id"),
                            "record_id_2": right.get("record_id"),
                            "match_score": round(score, 2),
                            "match_status": status,
                        }
                    )

        return candidates
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Candidate match generation failed: %s", exc)
        return []

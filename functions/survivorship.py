from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List

from functions.utils import get_logger, load_json_config

logger = get_logger(__name__)


def _normalize_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _resolve_strategy(attribute_name: str, rules: Dict[str, str] | None = None) -> str:
    if rules is None:
        rules = load_json_config("survivorship_rules.json")
    return rules.get(attribute_name.upper(), rules.get("DEFAULT", "MOST_RECENT"))


def apply_survivorship(records: Iterable[Dict[str, Any]], attribute_name: str, rules: Dict[str, str] | None = None) -> Dict[str, Any]:
    """Select a single best value for an attribute according to a survivorship strategy."""
    try:
        all_records = list(records)
        if not all_records:
            return {"attribute_name": attribute_name, "attribute_value": None}

        strategy = _resolve_strategy(attribute_name, rules=rules)
        values = [
            record.get(attribute_name)
            for record in all_records
            if record.get(attribute_name) is not None and _normalize_value(record.get(attribute_name))
        ]

        if not values:
            return {"attribute_name": attribute_name, "attribute_value": None}

        if strategy == "MOST_RECENT":
            latest_record = max(all_records, key=lambda record: str(record.get("load_timestamp", "1970-01-01T00:00:00")))
            return {
                "attribute_name": attribute_name,
                "attribute_value": latest_record.get(attribute_name),
                "source_record": latest_record.get("record_id"),
            }

        if strategy == "LONGEST_VALUE":
            winner = max(values, key=lambda value: len(_normalize_value(value)))
            return {"attribute_name": attribute_name, "attribute_value": winner}

        if strategy == "MOST_COMPLETES":
            winner = max(
                values,
                key=lambda value: (len(_normalize_value(value)), 1 if _normalize_value(value) else 0),
            )
            return {"attribute_name": attribute_name, "attribute_value": winner}

        return {"attribute_name": attribute_name, "attribute_value": values[0]}
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Survivorship failed for attribute '%s': %s", attribute_name, exc)
        return {"attribute_name": attribute_name, "attribute_value": None}


def merge_records(records: Iterable[Dict[str, Any]], rules: Dict[str, str] | None = None) -> Dict[str, Any]:
    """Consolidate duplicate record data into a single canonical record representation."""
    try:
        all_records = list(records)
        if not all_records:
            return {}

        attribute_names = set()
        for record in all_records:
            attribute_names.update(record.keys())

        merged: Dict[str, Any] = {"record_id": all_records[0].get("record_id")}
        for attribute_name in sorted(attribute_names - {"record_id", "source_system", "entity_type", "load_timestamp"}):
            merged[attribute_name] = apply_survivorship(all_records, attribute_name, rules=rules).get("attribute_value")

        return merged
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Record merge failed: %s", exc)
        return {}

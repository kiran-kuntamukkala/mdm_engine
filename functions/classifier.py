from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

from functions.utils import get_logger, load_json_config

logger = get_logger(__name__)


def _normalize_token(value: Any) -> str:
    """Normalize a column name so string comparisons are stable and pattern-based."""
    if value is None:
        return ""
    normalized = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    normalized = re.sub(r"[^a-z0-9_]+", "", normalized)
    return normalized


def classify_column(column_name: Any, config: Optional[Dict[str, List[str]]] = None) -> str:
    """Identify the semantic category of a source column from configuration metadata."""
    try:
        if column_name is None:
            return "UNKNOWN"

        if config is None:
            config = load_json_config("column_classification.json")

        normalized = _normalize_token(column_name)

        for category, aliases in config.items():
            for alias in aliases:
                if _normalize_token(alias) == normalized:
                    return category

        for category, aliases in config.items():
            for alias in aliases:
                alias_norm = _normalize_token(alias)
                if alias_norm in normalized or normalized in alias_norm:
                    return category

        return "UNKNOWN"
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Classification failed for column '%s': %s", column_name, exc)
        return "UNKNOWN"


def classify_columns(column_names: Iterable[Any], config: Optional[Dict[str, List[str]]] = None) -> Dict[str, str]:
    """Classify multiple columns in a single pass."""
    return {column: classify_column(column, config=config) for column in column_names}


def infer_entity_type(table_name: str, columns: Iterable[Any], config: Optional[Dict[str, Any]] = None) -> str:
    """Infer the entity type using config-driven pattern matching against table name and columns."""
    try:
        if config is None:
            config = load_json_config("entity_rules.json")

        tokens = set(_normalize_token(token) for token in [table_name, *columns])

        rules = config.get("ENTITY_RULES", [])
        for rule in rules:
            entity = rule.get("entity", "UNKNOWN")
            patterns = rule.get("patterns", [])
            if any(_normalize_token(pattern) in tokens for pattern in patterns):
                return entity

        return config.get("DEFAULT_ENTITY", "UNKNOWN")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Entity inference failed for table '%s': %s", table_name, exc)
        return config.get("DEFAULT_ENTITY", "UNKNOWN") if config else "UNKNOWN"

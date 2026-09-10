from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

from functions.utils import get_logger, load_json_config

logger = get_logger(__name__)


def _normalize_token(value: Any) -> str:
    """Normalize a column name so string comparisons are stable and pattern-based."""
    if value is None:
        return ""
    normalized = str(value).strip()
    normalized = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", normalized)
    normalized = normalized.lower().replace("-", "_").replace(" ", "_")
    normalized = re.sub(r"[^a-z0-9_]+", "", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized

def _token_variants(value: Any) -> set[str]:
    """Return a normalized token set that collapses common name, email, phone, and address aliases."""
    normalized = _normalize_token(value)
    if not normalized:
        return set()

    aliases = {
        "cust": "customer",
        "customer": "customer",
        "person": "name",
        "contact": "name",
        "full": "name",
        "name": "name",
        "nm": "name",
        "fname": "name",
        "fnm": "name",
        "first": "name",
        "lastname": "name",
        "mail": "email",
        "email": "email",
        "e": "email",
        "phone": "phone",
        "mobile": "phone",
        "cell": "phone",
        "tel": "phone",
        "telephone": "phone",
        "number": "phone",
        "num": "phone",
        "addr": "address",
        "address": "address",
        "street": "address",
        "residence": "address",
        "home": "address",
        "mailing": "address",
    }

    tokens = set()
    for part in re.split(r"[_]+", normalized):
        token = aliases.get(part, part)
        tokens.add(str(token))
    return tokens


def grouped_similar_columns(column_names: Iterable[Any], config: Optional[Dict[str, List[str]]] = None) -> Dict[str, List[str]]:
    """Group columns by logical field while collapsing near-identical aliases such as cust_name/customer_name."""
    grouped: Dict[str, List[str]] = {}
    for column_name in column_names:
        category = classify_column(column_name, config=config)
        if category == "UNKNOWN":
            continue
        grouped.setdefault(category, []).append(str(column_name))
    return {
        category: sorted(set(names))
        for category, names in grouped.items()
    }


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

        for category, aliases in config.items():
            category_tokens = set()
            for alias in aliases:
                category_tokens.update(_token_variants(alias))
            normalized_tokens = _token_variants(normalized)
            if category_tokens and normalized_tokens and (normalized_tokens & category_tokens):
                return category

        return "UNKNOWN"
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Classification failed for column '%s': %s", column_name, exc)
        return "UNKNOWN"


def classify_columns(column_names: Iterable[Any], config: Optional[Dict[str, List[str]]] = None) -> Dict[str, str]:
    """Classify multiple columns in a single pass."""
    return {column: classify_column(column, config=config) for column in column_names}

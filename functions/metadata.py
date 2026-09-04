from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

from functions.classifier import classify_column
from functions.utils import get_logger

logger = get_logger(__name__)


def _normalize_identifier(value: Any) -> str:
    """Normalize column names so equivalent aliases can be compared reliably."""
    if value is None:
        return ""
    cleaned = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    return re.sub(r"[^a-z0-9_]+", "", cleaned)


def _logical_field_key(column_name: Any) -> str:
    """Map a source column to a canonical logical field key using classification data."""
    if column_name is None:
        return ""

    classification = classify_column(column_name)
    if classification and classification != "UNKNOWN":
        return classification

    return _normalize_identifier(column_name)


def get_table_metadata(df: Any) -> List[Dict[str, str]]:
    """Return column metadata describing the structure of a source dataset.

    The function is intentionally schema-driven and works with both Spark and pandas
    DataFrames to keep the framework reusable across environments.
    """
    try:
        if hasattr(df, "dtypes") and hasattr(df.dtypes, "items"):
            return [
                {"column_name": column, "data_type": str(dtype)}
                for column, dtype in df.dtypes.items()
            ]

        if hasattr(df, "schema"):
            return [
                {"column_name": field.name, "data_type": field.dataType.simpleString()}
                for field in df.schema.fields
            ]

        raise TypeError("Unsupported DataFrame type for metadata extraction")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Unable to gather table metadata: %s", exc)
        raise


def resolve_prioritized_metadata(
    metadata_by_source: Dict[str, List[Dict[str, str]]],
    priority_order: Iterable[str] | None = None,
) -> List[Dict[str, str]]:
    """Choose the highest-priority available column and its dtype for each logical field.

    Example: tb1 -> tb2 -> tb3. If a field exists in tb1, it wins. If that field is missing
    in tb1, the next table in the priority chain is used instead and the type is aligned with
    that chosen source schema.
    """
    ordered_sources = list(priority_order) if priority_order is not None else list(metadata_by_source.keys())
    resolved: Dict[str, Dict[str, str]] = {}

    for source_name in ordered_sources:
        for field in metadata_by_source.get(source_name, []):
            column_name = field.get("column_name")
            if column_name is None:
                continue

            field_key = _logical_field_key(column_name)
            if not field_key or field_key in resolved:
                continue

            resolved[field_key] = {
                "column_name": column_name,
                "data_type": str(field.get("data_type", "string")),
                "source_name": source_name,
            }

    return [
        {
            "column_name": candidate["column_name"],
            "data_type": candidate["data_type"],
            "source_name": candidate["source_name"],
        }
        for candidate in resolved.values()
    ]


def pick_prioritized_value(
    source_rows: Dict[str, Dict[str, Any]],
    priority_order: Iterable[str] | None = None,
) -> Dict[str, Any]:
    """Select the highest-priority available value for each logical field across source rows."""
    ordered_sources = list(priority_order) if priority_order is not None else list(source_rows.keys())
    selected: Dict[str, Dict[str, Any]] = {}

    for source_name in ordered_sources:
        row = source_rows.get(source_name) or {}
        if not isinstance(row, dict):
            continue

        for column_name, value in row.items():
            if value is None:
                continue

            field_key = _logical_field_key(column_name)
            if not field_key or field_key in selected:
                continue

            selected[field_key] = {"column_name": column_name, "value": value}

    return {
        metadata["column_name"]: metadata["value"]
        for metadata in selected.values()
    }

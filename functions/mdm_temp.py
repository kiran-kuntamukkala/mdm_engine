from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List

from functions.classifier import classify_column
from functions.standardization import standardize_by_classification
from functions.utils import get_logger

logger = get_logger(__name__)


def normalize_record(record: Dict[str, Any], source_system: str, entity_type: str) -> List[Dict[str, Any]]:
    """Convert a source record into the bronze.mdm_temp canonical shape for attribute-level storage."""
    try:
        rows: List[Dict[str, Any]] = []

        for column_name, value in record.items():
            if column_name in {"source_system", "entity_type", "load_timestamp"}:
                continue

            classification = classify_column(column_name)
            normalized_value = standardize_by_classification(value, classification)
            if normalized_value is None:
                continue

            rows.append(
                {
                    "record_id": str(record.get("record_id") or record.get("id") or f"{source_system}_row"),
                    "source_system": source_system,
                    "entity_type": entity_type,
                    "attribute_name": column_name,
                    "attribute_value": normalized_value,
                    "load_timestamp": datetime.utcnow().isoformat(),
                }
            )

        return rows
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Normalization failed for record from source '%s': %s", source_system, exc)
        return []


def build_mdm_temp(df: Any, source_system: str, entity_type: str) -> List[Dict[str, Any]]:
    """Convert a whole dataset into canonical mdm_temp records."""
    try:
        rows: List[Dict[str, Any]] = []
        if hasattr(df, "to_dict"):
            source_rows = df.to_dict(orient="records")
        else:
            source_rows = list(df)

        for record in source_rows:
            rows.extend(normalize_record(record, source_system=source_system, entity_type=entity_type))

        return rows
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("mdm_temp preparation failed for source '%s': %s", source_system, exc)
        return []

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List

from functions.classifier import classify_column
from functions.standardization import standardize_by_classification
from functions.utils import get_logger

logger = get_logger(__name__)


def normalize_record(record: Dict[str, Any], source_system: str, entity_type: str) -> Dict[str, Any]:
    """Convert a source record into a single canonical MDM temp row."""
    try:
        normalized: Dict[str, Any] = {
            "record_id": str(record.get("record_id") or record.get("id") or f"{source_system}_row"),
            "source_system": source_system,
            "entity_type": entity_type,
            "load_timestamp": datetime.utcnow().isoformat(),
        }

        for column_name, value in record.items():
            if column_name in {"source_system", "entity_type", "load_timestamp"}:
                continue

            classification = classify_column(column_name)
            if classification == "UNKNOWN":
                continue

            standardized_value = standardize_by_classification(value, classification)
            if standardized_value is None:
                continue

            normalized[column_name] = standardized_value

        return normalized
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Normalization failed for record from source '%s': %s", source_system, exc)
        return {}


def build_mdm_temp(df: Any, source_system: str, entity_type: str) -> List[Dict[str, Any]]:
    """Convert a whole dataset into one canonical mdm_temp row per source record."""
    try:
        rows: List[Dict[str, Any]] = []
        if hasattr(df, "to_dict"):
            source_rows = df.to_dict(orient="records")
        else:
            source_rows = list(df)

        for record in source_rows:
            normalized_row = normalize_record(record, source_system=source_system, entity_type=entity_type)
            if normalized_row:
                rows.append(normalized_row)

        return rows
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("mdm_temp preparation failed for source '%s': %s", source_system, exc)
        return []

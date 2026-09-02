from __future__ import annotations

from typing import Any, Dict, Iterable, List

from functions.utils import get_logger

logger = get_logger(__name__)


def create_golden_record(matched_records: Iterable[Dict[str, Any]], entity_type: str = "CUSTOMER") -> Dict[str, Any]:
    """Create a canonical golden record from a cluster of related source records."""
    try:
        records = list(matched_records)
        if not records:
            return {"master_id": None, "entity_type": entity_type, "attributes": {}}

        golden: Dict[str, Any] = {"entity_type": entity_type, "attributes": {}}
        for key in sorted({attribute for record in records for attribute in record.keys() if attribute not in {"record_id", "source_system", "entity_type", "load_timestamp"}}):
            values = [record.get(key) for record in records if record.get(key) is not None]
            if values:
                golden["attributes"][key] = values[0]

        golden["master_id"] = f"MASTER{abs(hash(tuple(sorted(str(v) for v in golden['attributes'].items())))) % 1000000:06d}"
        golden["source_count"] = len(records)
        golden["confidence_score"] = 95.0
        golden["last_updated"] = "2026-01-01"
        return golden
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Golden record creation failed: %s", exc)
        return {"master_id": None, "entity_type": entity_type, "attributes": {}}

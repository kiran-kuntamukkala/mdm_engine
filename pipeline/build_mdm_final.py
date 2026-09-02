from __future__ import annotations

from typing import Any, Dict, Iterable, List

from pyspark.sql import DataFrame, SparkSession

from functions.golden_record import create_golden_record
from functions.utils import get_logger

logger = get_logger(__name__)


def build_golden_records(spark: SparkSession, matched_records: Iterable[Dict[str, Any]], target_table: str, entity_type: str = "CUSTOMER") -> DataFrame:
    """Create and persist bronze.mdm_final golden records from matched record groups."""
    try:
        clusters = []
        for record in matched_records:
            clusters.append(record)

        golden_record = create_golden_record(clusters, entity_type=entity_type)
        if not golden_record or golden_record.get("master_id") is None:
            logger.warning("No golden record created for entity type %s", entity_type)
            return spark.createDataFrame([], schema="master_id STRING, entity_type STRING, attribute_name STRING, attribute_value STRING, source_count INT, confidence_score DOUBLE, last_updated STRING")

        output_rows = []
        for attribute_name, attribute_value in golden_record["attributes"].items():
            output_rows.append(
                {
                    "master_id": golden_record["master_id"],
                    "entity_type": golden_record["entity_type"],
                    "attribute_name": attribute_name,
                    "attribute_value": attribute_value,
                    "source_count": golden_record.get("source_count", 0),
                    "confidence_score": golden_record.get("confidence_score", 0.0),
                    "last_updated": golden_record.get("last_updated", "2026-01-01"),
                }
            )

        golden_df = spark.createDataFrame(output_rows)
        golden_df.write.mode("overwrite").saveAsTable(target_table)
        logger.info("Wrote %s golden attributes to %s", golden_df.count(), target_table)
        return golden_df
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Golden record build failed: %s", exc)
        raise

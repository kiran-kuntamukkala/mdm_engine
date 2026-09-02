from __future__ import annotations

from typing import Any, Dict, Iterable, List

from pyspark.sql import DataFrame, SparkSession

from functions.mdm_temp import build_mdm_temp
from functions.utils import get_logger

logger = get_logger(__name__)


def build_mdm_temp_table(spark: SparkSession, source_table: str, target_table: str, source_system: str, entity_type: str) -> DataFrame:
    """Create the bronze.mdm_temp table from a raw source dataset."""
    try:
        df = spark.table(source_table)
        rows = build_mdm_temp(df, source_system=source_system, entity_type=entity_type)
        if not rows:
            logger.warning("No MDM temp records generated for %s", source_table)
            return spark.createDataFrame([], schema="record_id STRING, source_system STRING, entity_type STRING, attribute_name STRING, attribute_value STRING, load_timestamp STRING")

        out_df = spark.createDataFrame(rows)
        out_df.write.mode("overwrite").saveAsTable(target_table)
        logger.info("Wrote %s rows to %s", out_df.count(), target_table)
        return out_df
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Failed to build mdm_temp for %s: %s", source_table, exc)
        raise

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from functions.metadata import get_table_metadata
from functions.profiling import get_row_count
from functions.utils import get_logger

logger = get_logger(__name__)


def read_source_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a Bronze source table from Databricks catalog and schema."""
    try:
        logger.info("Reading table %s", table_name)
        return spark.table(table_name)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Failed to read source table '%s': %s", table_name, exc)
        raise


def profile_table(df: DataFrame) -> dict:
    """Return a structured profiling summary for a source table."""
    try:
        return {
            "row_count": get_row_count(df),
            "column_count": len(df.columns),
            "metadata": get_table_metadata(df),
        }
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Profiling failed for dataframe: %s", exc)
        raise

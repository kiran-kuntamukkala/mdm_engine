from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from functions.utils import get_logger

logger = get_logger(__name__)


def get_row_count(df: DataFrame) -> int:
    """Return total number of rows for a dataset."""
    try:
        return df.count()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Row count profiling failed: %s", exc)
        raise


def get_column_count(df: DataFrame) -> int:
    """Return number of columns in a dataset."""
    try:
        return len(df.columns)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Column count profiling failed: %s", exc)
        raise


def get_null_percentage(df: DataFrame, column_name: str) -> float:
    """Return percentage of null or empty values in a column."""
    try:
        total_rows = get_row_count(df)
        if total_rows == 0:
            return 0.0

        null_count = df.filter(
            F.col(column_name).isNull() | (F.col(column_name).cast("string") == "")
        ).count()

        return round((null_count / total_rows) * 100, 2)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Null percentage profiling failed for column '%s': %s", column_name, exc)
        raise


def get_distinct_count(df: DataFrame, column_name: str) -> int:
    """Return the number of distinct non-null values in a column."""
    try:
        return df.filter(F.col(column_name).isNotNull()).select(F.col(column_name)).distinct().count()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Distinct count profiling failed for column '%s': %s", column_name, exc)
        raise

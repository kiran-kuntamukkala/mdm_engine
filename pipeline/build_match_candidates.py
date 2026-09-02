from __future__ import annotations

from typing import Any, Dict, Iterable, List

from pyspark.sql import DataFrame, SparkSession

from functions.matcher import build_match_candidates
from functions.utils import get_logger

logger = get_logger(__name__)


def build_candidate_table(spark: SparkSession, source_records: Iterable[Dict[str, Any]], target_table: str) -> DataFrame:
    """Build bronze.match_candidates from a collection of canonical record dictionaries."""
    try:
        candidates = build_match_candidates(source_records)
        if not candidates:
            logger.warning("No candidate matches generated.")
            return spark.createDataFrame([], schema="record_id_1 STRING, record_id_2 STRING, match_score DOUBLE, match_status STRING")

        candidate_df = spark.createDataFrame(candidates)
        candidate_df.write.mode("overwrite").saveAsTable(target_table)
        logger.info("Wrote %s candidate pairs to %s", candidate_df.count(), target_table)
        return candidate_df
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Candidate build failed: %s", exc)
        raise

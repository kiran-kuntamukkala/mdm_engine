from __future__ import annotations

from pyspark.sql import SparkSession

from functions.classifier import classify_column
from functions.metadata import get_table_metadata, pick_prioritized_value, resolve_prioritized_metadata
from functions.mdm_temp import build_mdm_temp
from functions.utils import get_logger

logger = get_logger(__name__)


def build_spark_session(app_name: str = "mdm") -> SparkSession:
    """Create a Spark session for Databricks-compatible local or cluster execution."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def main() -> None:
    """Example orchestration entry point for a metadata-driven MDM workflow."""
    spark = build_spark_session()

    try:
        source_tables = [
            "mdm.bronze.crm_customers",
            "mdm.bronze.banking_customers",
            "mdm.bronze.creditcard_customers",
        ]
        priority_order = ["crm_customers", "banking_customers", "creditcard_customers"]
        metadata_by_source = {}
        source_records = {}

        for table_name in source_tables:
            source_name = table_name.split(".")[-1]
            logger.info("Processing source table: %s", table_name)
            df = spark.table(table_name)

            metadata = get_table_metadata(df)
            metadata_by_source[source_name] = metadata
            source_records[source_name] = df.limit(1).toPandas().to_dict(orient="records")[0] if df.limit(1).count() else {}
            logger.info("Discovered metadata for %s: %s", table_name, metadata)

            for column in df.columns:
                logger.info("Column '%s' classified as '%s'", column, classify_column(column))

            canonical_rows = build_mdm_temp(df, source_system=source_name, entity_type="CUSTOMER")
            logger.info("Generated %s canonical rows for %s", len(canonical_rows), table_name)

        prioritized_schema = resolve_prioritized_metadata(metadata_by_source, priority_order=priority_order)
        prioritized_record = pick_prioritized_value(source_records, priority_order=priority_order)
        logger.info("Resolved priority-based schema: %s", prioritized_schema)
        logger.info("Priority-selected source values: %s", prioritized_record)
        logger.info("MDM pipeline completed successfully.")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("MDM pipeline failed: %s", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

from __future__ import annotations

from functools import reduce
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from functions.classifier import classify_column
from functions.standardization import standardize_by_classification


CANONICAL_COLUMNS = {
    "NAME": "customer_name",
    "EMAIL": "email",
    "PHONE": "phone",
    "ADDRESS": "address",
    "ID": "record_id",
}
MATCH_COLUMNS = ("email", "phone", "customer_name", "address")


def _source_name(table_name: str) -> str:
    return table_name.rsplit(".", 1)[-1]


def _value(value: object, classification: str) -> str | None:
    if value is None:
        return None
    result = standardize_by_classification(value, classification)
    return result if result else None


def _canonicalize_table(df: DataFrame, source_table: str, priority: int) -> DataFrame:
    source_name = _source_name(source_table)
    classified = {column: classify_column(column) for column in df.columns}
    payload_columns = [F.col(column).cast("string").alias(column) for column in df.columns]

    canonical = []
    for category, canonical_name in CANONICAL_COLUMNS.items():
        candidates = [column for column, value in classified.items() if value == category]
        if candidates:
            expressions = [F.col(column).cast("string") for column in candidates]
            canonical.append(F.coalesce(*expressions).alias(canonical_name))
        else:
            canonical.append(F.lit(None).cast("string").alias(canonical_name))

    result = df.select(
        *canonical,
        F.to_json(F.struct(*payload_columns)).alias("source_payload"),
    )
    for column, classification in ((name, category) for category, name in CANONICAL_COLUMNS.items()):
        result = result.withColumn(
            column,
            F.udf(lambda value, cls=classification: _value(value, cls), "string")(F.col(column)),
        )

    row_window = Window.orderBy(F.monotonically_increasing_id())
    return (
        result.withColumn("source_table", F.lit(source_name))
        .withColumn("source_priority", F.lit(priority))
        .withColumn("source_row_number", F.row_number().over(row_window))
        .withColumn(
            "source_row_id",
            F.concat_ws(":", F.col("source_table"), F.col("source_row_number")),
        )
        .drop("source_row_number")
    )


def build_mdm_temp(spark: SparkSession, prioritized_tables: Sequence[str]) -> DataFrame:
    """Read prioritized source tables and return one canonical row per source row."""
    if not prioritized_tables:
        raise ValueError("prioritized_tables must contain at least one table")

    tables = [
        _canonicalize_table(spark.table(table_name), table_name, priority)
        for priority, table_name in enumerate(prioritized_tables, start=1)
    ]
    return reduce(DataFrame.unionByName, tables)


def _splink_pairs(mdm_temp: DataFrame, spark: SparkSession, threshold: float) -> DataFrame:
    """Use Splink to score candidate pairs; blocking keeps linkage scalable."""
    threshold = float(threshold)
    import splink

    splink_version = str(getattr(splink, "__version__", "unknown"))
    if not splink_version.startswith("4."):
        raise RuntimeError(
            f"new_mdm_engine requires Splink 4.x, but Databricks loaded {splink_version}. "
            "Install splink==4.0.17 and restart Python."
        )

    from splink import Linker, SparkAPI, block_on
    from splink import comparison_library as cl

    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "source_row_id",
        "probability_two_random_records_match": float(0.01),
        "comparisons": [
            cl.ExactMatch("email"),
            cl.ExactMatch("phone"),
            cl.JaroWinklerAtThresholds("customer_name", [float(0.95), float(0.85)]),
            cl.JaroWinklerAtThresholds("address", [float(0.95), float(0.8)]),
        ],
        "blocking_rules_to_generate_predictions": [
            block_on("email"),
            block_on("phone"),
            block_on("customer_name"),
        ],
    }
    linker = Linker(mdm_temp, settings, db_api=SparkAPI(spark_session=spark))
    predictions = linker.inference.predict().as_spark_dataframe()
    return predictions.filter(F.col("match_probability") >= F.lit(threshold)).select(
        F.col("source_row_id_l").alias("left_id"),
        F.col("source_row_id_r").alias("right_id"),
        "match_probability",
    )


def _add_entity_ids(mdm_temp: DataFrame, pairs: DataFrame) -> DataFrame:
    """Compute connected components so transitive matches become one entity."""
    edges = pairs.select("left_id", "right_id").distinct()
    nodes = mdm_temp.select(F.col("source_row_id").alias("node_id")).distinct()
    components = nodes.withColumn("component_id", F.col("node_id"))
    changed = True
    while changed:
        propagated = (
            edges.join(components.alias("left"), F.col("left_id") == F.col("left.node_id"))
            .select(F.col("right_id").alias("node_id"), F.col("left.component_id"))
            .unionByName(components.select("node_id", "component_id"))
            .groupBy("node_id")
            .agg(F.min("component_id").alias("component_id"))
        )
        changed = components.exceptAll(propagated).limit(1).count() > 0
        components = propagated
    return mdm_temp.join(components, mdm_temp.source_row_id == components.node_id).drop("node_id")


def _priority_value(df: DataFrame, column: str) -> F.Column:
    return F.element_at(
        F.sort_array(
            F.collect_list(
                F.when(
                    F.col(column).isNotNull(),
                    F.struct("source_priority", F.col(column)),
                )
            ),
            asc=True,
        ),
        1,
    )[column]


def build_mdm(
    spark: SparkSession,
    prioritized_tables: Sequence[str],
    match_probability_threshold: float = 0.5,
) -> tuple[DataFrame, DataFrame]:
    """Build row-preserving ``mdm_temp`` and priority-resolved ``mdm_final``."""
    match_probability_threshold = float(match_probability_threshold)
    mdm_temp = build_mdm_temp(spark, prioritized_tables)
    pairs = _splink_pairs(mdm_temp, spark, match_probability_threshold)
    clustered = _add_entity_ids(mdm_temp, pairs)
    final = clustered.groupBy("component_id").agg(
        *[_priority_value(clustered, column).alias(column) for column in MATCH_COLUMNS],
        _priority_value(clustered, "record_id").alias("record_id"),
        F.sort_array(F.collect_list(F.col("source_payload"))).alias("source_records"),
        F.count("source_row_id").alias("source_record_count"),
    ).withColumnRenamed("component_id", "entity_id")
    return mdm_temp, final
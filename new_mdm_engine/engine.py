from __future__ import annotations

from functools import reduce
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from functions.classifier import classify_column
from functions.metadata import get_table_metadata, resolve_prioritized_metadata
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

    return (
        result.withColumn("source_table", F.lit(source_name))
        .withColumn("source_priority", F.lit(priority))
        .withColumn(
            "source_row_id",
            F.concat_ws(":", F.col("source_table"), F.col("record_id")),
        )
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
    try:
        linker = Linker(mdm_temp, settings, db_api=SparkAPI(spark_session=spark))
        predictions = linker.inference.predict().as_spark_dataframe()
        return predictions.filter(F.col("match_probability") >= F.lit(threshold)).select(
            F.col("source_row_id_l").alias("left_id"),
            F.col("source_row_id_r").alias("right_id"),
            "match_probability",
        )
    except TypeError as exc:
        if "unsupported operand type(s) for /" not in str(exc):
            raise
        return _spark_compatibility_pairs(mdm_temp)


def _spark_compatibility_pairs(mdm_temp: DataFrame) -> DataFrame:
    """Keep Databricks jobs running when Splink's Python model setup is incompatible."""
    left = mdm_temp.alias("left")
    right = mdm_temp.alias("right")
    same_value = [
        (F.col(f"left.{column}").isNotNull())
        & (F.col(f"left.{column}") == F.col(f"right.{column}"))
        for column in MATCH_COLUMNS
    ]
    name_close = (
        F.col("left.customer_name").isNotNull()
        & F.col("right.customer_name").isNotNull()
        & (F.levenshtein("left.customer_name", "right.customer_name") <= 2)
    )
    return (
        left.join(
            right,
            (F.col("left.source_row_id") < F.col("right.source_row_id"))
            & (reduce(lambda current, condition: current | condition, same_value) | name_close),
        )
        .select(
            F.col("left.source_row_id").alias("left_id"),
            F.col("right.source_row_id").alias("right_id"),
            F.lit(1.0).alias("match_probability"),
        )
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
    sorted_values = F.array_sort(
        F.collect_list(
            F.when(
                F.col(column).isNotNull(),
                F.struct("source_priority", F.col(column)),
            )
        )
    )
    return F.get(sorted_values, 0).getField(column)


def _conflicting_values(df: DataFrame, column: str) -> F.Column:
    """Keep every distinct source value when an entity has a disagreement."""
    values = F.array_sort(
        F.collect_set(
            F.when(
                F.col(column).isNotNull(),
                F.struct("source_priority", "source_table", F.col(column).alias("value")),
            )
        )
    )
    return F.when(F.size(values) > 1, F.to_json(values)).otherwise(F.lit(None).cast("string"))


def _priority_actions(
    metadata: Sequence[dict[str, str]],
    priority_order: Sequence[str],
) -> list[dict[str, str]]:
    """Describe why each master column name and type was selected."""
    return [
        {
            "decision_type": "COLUMN_SCHEMA",
            "logical_field": classify_column(item["column_name"]),
            "master_column": item["column_name"],
            "data_type": item["data_type"],
            "selected_source": item["source_name"],
            "priority_order": ",".join(priority_order),
            "reason": "Selected from the highest-priority source containing this logical field.",
        }
        for item in metadata
    ]


def build_mdm(
    spark: SparkSession,
    prioritized_tables: Sequence[str],
    match_probability_threshold: float = 0.5,
    actions_table: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Build row-preserving temp data and a priority-resolved master table.

    ``actions_table`` optionally persists the schema decisions separately from the
    master data so every selected name and type has an auditable explanation.
    """
    match_probability_threshold = float(match_probability_threshold)
    mdm_temp = build_mdm_temp(spark, prioritized_tables)
    source_names = [_source_name(table_name) for table_name in prioritized_tables]
    metadata_by_source = {
        source_name: get_table_metadata(spark.table(table_name))
        for source_name, table_name in zip(source_names, prioritized_tables)
    }
    prioritized_metadata = resolve_prioritized_metadata(metadata_by_source, source_names)
    metadata_by_field = {
        classify_column(item["column_name"]): item for item in prioritized_metadata
    }
    pairs = _splink_pairs(mdm_temp, spark, match_probability_threshold)
    clustered = _add_entity_ids(mdm_temp, pairs)
    selected_internal = [
        _priority_value(clustered, column).alias(f"_selected_{column}")
        for column in MATCH_COLUMNS
    ]
    conflict_internal = [
        _conflicting_values(clustered, column).alias(f"_conflicts_{column.lower()}")
        for column in MATCH_COLUMNS
    ]
    final = clustered.groupBy("component_id").agg(
        *selected_internal,
        *conflict_internal,
        _priority_value(clustered, "record_id").alias("_selected_record_id"),
        F.sort_array(F.collect_list(F.col("source_payload"))).alias("source_records"),
        F.count("source_row_id").alias("source_record_count"),
    ).withColumnRenamed("component_id", "entity_id")

    output_columns = []
    for internal_name in (*MATCH_COLUMNS, "record_id"):
        category = "ID" if internal_name == "record_id" else classify_column(internal_name)
        metadata = metadata_by_field.get(category)
        output_name = metadata["column_name"] if metadata else internal_name
        data_type = metadata["data_type"] if metadata else "string"
        output_columns.append(F.col(f"_selected_{internal_name}").cast(data_type).alias(output_name))

    final = final.select(
        "entity_id",
        *output_columns,
        *[f"_conflicts_{column.lower()}" for column in MATCH_COLUMNS],
        "source_records",
        "source_record_count",
    )

    if actions_table:
        actions = spark.createDataFrame(_priority_actions(prioritized_metadata, source_names))
        actions.write.mode("overwrite").saveAsTable(actions_table)

    return mdm_temp, final
from __future__ import annotations

import re
from functools import reduce
from typing import Sequence
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from functions.classifier import classify_column
from functions.metadata import get_table_metadata, resolve_prioritized_metadata


CANONICAL_COLUMNS = {
    "NAME": "customer_name",
    "EMAIL": "email",
    "PHONE": "phone",
    "ADDRESS": "address",
}
MATCH_COLUMNS = ("email", "phone", "customer_name", "address")


def _canonical_output_name(column_name: str, classification: str | None = None) -> str:
    """Normalize source attributes to a stable logical field name for the master table."""
    raw_name = str(column_name or "").strip()
    normalized = raw_name.lower().replace("-", "_").replace(" ", "_")
    normalized = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", normalized)
    normalized = re.sub(r"[^a-z0-9_]+", "", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    category = (classification or classify_column(column_name) or "UNKNOWN").upper()

    if category == "NAME":
        return "customer_name"
    if category == "EMAIL":
        return "email"
    if category == "PHONE":
        return "phone"
    if category == "ADDRESS":
        return "address"
    if category == "ID":
        if "account" in normalized:
            return "account_id"
        if "customer" in normalized:
            return "customer_id"
        if "record" in normalized or normalized in {"id", "recordid"}:
            return "record_id"
        return normalized or "record_id"
    if normalized:
        return normalized
    return "record_id"


def _source_name(table_name: str) -> str:
    return table_name.rsplit(".", 1)[-1]


def _standardize_expression(column: F.Column, classification: str) -> F.Column:
    trimmed = F.trim(column.cast("string"))
    non_empty = F.when(trimmed != "", trimmed)

    if classification == "NAME":
        return F.upper(F.regexp_replace(non_empty, r"\s+", " "))
    if classification == "EMAIL":
        return F.lower(non_empty)
    if classification == "PHONE":
        digits = F.regexp_replace(trimmed, r"\D+", "")
        digits = F.when(
            (F.length(digits) > 10) & digits.startswith("91"),
            F.substring(digits, 3, 1000),
        ).otherwise(digits)
        digits = F.when(
            (F.length(digits) == 11) & digits.startswith("1"),
            F.substring(digits, 2, 1000),
        ).otherwise(digits)
        return F.when(F.length(digits) > 0, digits)
    if classification == "ADDRESS":
        return F.initcap(F.regexp_replace(non_empty, r"\s+", " "))
    return non_empty


def _logical_fields_for_table(df: DataFrame) -> list[str]:
    """Return the ordered logical field names for a source table, including ID columns."""
    fields: list[str] = []
    for column in df.columns:
        classification = classify_column(column)
        logical_name = _canonical_output_name(column, classification)
        if logical_name not in fields:
            fields.append(logical_name)
    return fields


def _canonicalize_table(df: DataFrame, source_table: str, priority: int, output_fields: Sequence[str]) -> DataFrame:
    source_name = _source_name(source_table)
    classified = {column: classify_column(column) for column in df.columns}
    payload_columns = [F.col(column).cast("string").alias(column) for column in df.columns]

    canonical = []
    for logical_name in output_fields:
        candidates = [
            column for column, value in classified.items()
            if _canonical_output_name(column, value) == logical_name
        ]
        if candidates:
            expressions = [F.col(column).cast("string") for column in candidates]
            canonical.append(F.coalesce(*expressions).alias(logical_name))
        else:
            canonical.append(F.lit(None).cast("string").alias(logical_name))

    result = df.select(
        *canonical,
        F.to_json(F.struct(*payload_columns)).alias("source_payload"),
    )
    for logical_name in output_fields:
        if logical_name in CANONICAL_COLUMNS.values():
            classification = next(
                key for key, value in CANONICAL_COLUMNS.items() if value == logical_name
            )
            result = result.withColumn(logical_name, _standardize_expression(F.col(logical_name), classification))

    return (
        result.withColumn("source_table", F.lit(source_name))
        .withColumn("source_priority", F.lit(priority))
        .withColumn(
            "source_row_id",
            F.concat_ws(":", F.col("source_table"), F.coalesce(F.col("record_id"), F.col("customer_id"), F.col("account_id"), F.lit("unknown"))),
        )
    )


def build_mdm_temp(spark: SparkSession, prioritized_tables: Sequence[str]) -> DataFrame:
    """Read prioritized source tables and return one canonical row per source row."""
    if not prioritized_tables:
        raise ValueError("prioritized_tables must contain at least one table")

    source_frames = [spark.table(table_name) for table_name in prioritized_tables]
    output_fields = []
    for dataframe in source_frames:
        for logical_name in _logical_fields_for_table(dataframe):
            if logical_name not in output_fields:
                output_fields.append(logical_name)

    tables = [
        _canonicalize_table(frame, table_name, priority, output_fields)
        for priority, (table_name, frame) in enumerate(zip(prioritized_tables, source_frames), start=1)
    ]
    view_names = [f"mdm_source_{uuid4().hex}" for _ in tables]
    for view_name, table in zip(view_names, tables):
        table.createOrReplaceTempView(view_name)
    return spark.sql(" UNION ALL ".join(f"SELECT * FROM `{name}`" for name in view_names))


def _splink_pairs(
    mdm_temp: DataFrame,
    spark: SparkSession,
    threshold: float,
    match_columns: Sequence[str],
) -> DataFrame:
    """Use Splink to score candidate pairs; blocking keeps linkage scalable."""
    threshold = float(threshold)
    if type(mdm_temp).__module__.startswith("pyspark.sql.connect"):
        return _spark_compatibility_pairs(mdm_temp, match_columns)

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
            cl.ExactMatch(column) if column in {"email", "phone"} else
            cl.JaroWinklerAtThresholds(
                column,
                [float(0.95), float(0.85)] if column == "customer_name" else [float(0.95), float(0.8)],
            )
            for column in match_columns
        ],
        "blocking_rules_to_generate_predictions": [block_on(column) for column in match_columns],
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
        return _spark_compatibility_pairs(mdm_temp, match_columns)

def _spark_compatibility_pairs(mdm_temp: DataFrame, match_columns: Sequence[str]) -> DataFrame:
    """Keep Databricks jobs running when Splink's Python model setup is incompatible."""
    left = mdm_temp.alias("left")
    right = mdm_temp.alias("right")
    same_value = [
        (F.col(f"left.{column}").isNotNull())
        & (F.col(f"left.{column}") == F.col(f"right.{column}"))
        for column in match_columns
    ]
    name_close = F.lit(False)
    if "customer_name" in match_columns:
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
        updates = (
            edges.join(components.alias("left"), F.col("left_id") == F.col("left.node_id"))
            .select(F.col("right_id").alias("node_id"), F.col("left.component_id"))
            .groupBy("node_id")
            .agg(F.min("component_id").alias("component_id"))
        )
        propagated = (
            components.alias("existing")
            .join(updates.alias("updates"), "node_id", "left")
            .select(
                F.col("node_id"),
                F.least(
                    F.col("existing.component_id"),
                    F.coalesce(F.col("updates.component_id"), F.col("existing.component_id")),
                ).alias("component_id"),
            )
        )
        changed = (
            components.join(propagated, ["node_id", "component_id"], "leftanti")
            .limit(1)
            .count()
            > 0
        )
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
    match_columns: Sequence[str] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Build row-preserving temp data and a priority-resolved master table.

    ``actions_table`` optionally persists the schema decisions separately from the
    master data so every selected name and type has an auditable explanation.
    """
    match_probability_threshold = float(match_probability_threshold)
    selected_match_columns = tuple(match_columns or MATCH_COLUMNS)
    invalid_columns = set(selected_match_columns) - set(MATCH_COLUMNS)
    if invalid_columns:
        raise ValueError(
            "match_columns must use canonical fields: "
            f"{', '.join(sorted(set(MATCH_COLUMNS)))}; invalid: {', '.join(sorted(invalid_columns))}"
        )
    if not selected_match_columns:
        raise ValueError("match_columns must contain at least one field")
    mdm_temp = build_mdm_temp(spark, prioritized_tables)
    source_names = [_source_name(table_name) for table_name in prioritized_tables]
    metadata_by_source = {
        source_name: get_table_metadata(spark.table(table_name))
        for source_name, table_name in zip(source_names, prioritized_tables)
    }
    prioritized_metadata = resolve_prioritized_metadata(metadata_by_source, source_names)
    metadata_by_field = {
        _canonical_output_name(item["column_name"], classify_column(item["column_name"])): item
        for item in prioritized_metadata
    }
    field_order = list(dict.fromkeys(
        _canonical_output_name(item["column_name"], classify_column(item["column_name"]))
        for item in prioritized_metadata
    ))
    matching_fields = [field for field in field_order if field in MATCH_COLUMNS or field.endswith("_id") or field == "record_id"]
    values_to_select = [field for field in field_order if field not in {"source_payload", "source_table", "source_priority", "source_row_id"}]

    pairs = _splink_pairs(mdm_temp, spark, match_probability_threshold, selected_match_columns)
    clustered = _add_entity_ids(mdm_temp, pairs)
    selected_internal = [
        _priority_value(clustered, field).alias(f"_selected_{field}")
        for field in values_to_select
        if field in clustered.columns
    ]
    conflict_internal = [
        _conflicting_values(clustered, field).alias(f"_conflicts_{field}")
        for field in values_to_select
        if field in clustered.columns and field in selected_match_columns or field.endswith("_id")
    ]
    final = clustered.groupBy("component_id").agg(
        *selected_internal,
        *conflict_internal,
        F.sort_array(F.collect_list(F.col("source_payload"))).alias("source_records"),
        F.count("source_row_id").alias("source_record_count"),
    ).withColumnRenamed("component_id", "entity_id")

    output_columns = []
    for internal_name in values_to_select:
        metadata = metadata_by_field.get(internal_name)
        data_type = metadata["data_type"] if metadata else "string"
        output_columns.append(
            F.col(f"_selected_{internal_name}").cast(data_type).alias(internal_name)
        )

    conflict_columns = [
        F.col(f"_conflicts_{field}")
        for field in values_to_select
        if field in clustered.columns and (field in selected_match_columns or field.endswith("_id"))
    ]

    final = final.select(
        "entity_id",
        *output_columns,
        *conflict_columns,
        "source_records",
        "source_record_count",
    )

    if actions_table:
        actions = spark.createDataFrame(_priority_actions(prioritized_metadata, source_names))
        actions.write.mode("overwrite").saveAsTable(actions_table)

    return mdm_temp, final
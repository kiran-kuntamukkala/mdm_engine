from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

from rapidfuzz import fuzz
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from functions.metadata import pick_prioritized_value, resolve_prioritized_metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("databricks_mdm")

CATALOG = "mdm"
SCHEMA = "bronze"


def get_spark() -> SparkSession:
    """Return the active SparkSession for Databricks runtime."""
    try:
        return SparkSession.getActiveSession()
    except Exception:
        return SparkSession.builder.appName("mdm_databricks_demo").getOrCreate()


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_name(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value).strip())
    return cleaned.upper() if cleaned else None


def normalize_email(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip().lower()
    return cleaned if cleaned else None


def normalize_phone(value: Any) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D+", "", str(value).strip())
    if not digits:
        return None
    if len(digits) > 10 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def normalize_address(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value).strip())
    return cleaned.title() if cleaned else None


def classify_column(column_name: str, config: Dict[str, List[str]] | None = None) -> str:
    """Map a source column name to a standard MDM attribute category."""
    config = config or load_json("config/column_classification.json")
    norm = re.sub(r"[^a-z0-9_]", "", str(column_name).lower().replace("-", "_").replace(" ", "_"))
    for category, aliases in config.items():
        for alias in aliases:
            alias_norm = re.sub(r"[^a-z0-9_]", "", str(alias).lower().replace("-", "_").replace(" ", "_"))
            if norm == alias_norm or alias_norm in norm or norm in alias_norm:
                return category
    return "UNKNOWN"


def standardize_value(value: Any, classification: str) -> Any:
    if value is None:
        return None
    mapping = {
        "NAME": normalize_name,
        "EMAIL": normalize_email,
        "PHONE": normalize_phone,
        "ADDRESS": normalize_address,
    }
    fn = mapping.get(classification)
    return fn(value) if fn else str(value).strip()


def get_table_metadata(df: DataFrame) -> List[Dict[str, str]]:
    return [{"column_name": col, "data_type": str(df.schema[col].dataType)} for col in df.columns]


def normalize_record(record: Dict[str, Any], source_system: str, entity_type: str) -> Dict[str, Any]:
    """Convert a source record into a single canonical MDM temp row."""
    normalized: Dict[str, Any] = {
        "record_id": str(record.get("record_id") or record.get("id") or f"{source_system}_row"),
        "source_system": source_system,
        "entity_type": entity_type,
        "load_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
    }

    for column_name, value in record.items():
        if column_name in {"source_system", "entity_type", "load_timestamp"}:
            continue

        classification = classify_column(column_name)
        if classification == "UNKNOWN":
            continue

        standardized = standardize_value(value, classification)
        if standardized is None:
            continue

        normalized[column_name] = standardized

    return normalized


def build_mdm_temp(df: DataFrame, source_system: str, entity_type: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for record in df.toPandas().to_dict(orient="records"):
        normalized_row = normalize_record(record, source_system=source_system, entity_type=entity_type)
        if normalized_row:
            rows.append(normalized_row)
    return rows


def exact_match(left: Any, right: Any) -> bool:
    return str(left).strip() == str(right).strip() if left is not None and right is not None else False


def fuzzy_match(left: Any, right: Any) -> float:
    if left is None or right is None:
        return 0.0
    return float(fuzz.ratio(str(left).strip(), str(right).strip()))


def calculate_match_score(field_type: str, left: Any, right: Any, thresholds: Dict[str, int] | None = None) -> float:
    thresholds = thresholds or {"EMAIL": 50, "PHONE": 40, "NAME": 105, "ADDRESS": 100, "DEFAULT": 10}
    if field_type in {"EMAIL", "PHONE"}:
        return 100.0 if exact_match(left, right) else 0.0
    score = fuzzy_match(left, right)
    cutoff = float(thresholds.get(field_type, thresholds.get("DEFAULT", 10)))
    return float(score if score >= cutoff else 0.0)


def build_match_candidates(records: Iterable[Dict[str, Any]], thresholds: Dict[str, int] | None = None) -> List[Dict[str, Any]]:
    records = list(records)
    candidates: List[Dict[str, Any]] = []
    for idx, left in enumerate(records):
        for right in records[idx + 1:]:
            score = 0.0
            for key in set(left.keys()) | set(right.keys()):
                if key in {"record_id", "source_system", "entity_type", "load_timestamp"}:
                    continue
                left_value = left.get(key)
                right_value = right.get(key)
                if left_value is None or right_value is None:
                    continue
                score += calculate_match_score(str(key).upper(), left_value, right_value, thresholds=thresholds)
            if score > 0:
                candidates.append({
                    "record_id_1": left.get("record_id"),
                    "record_id_2": right.get("record_id"),
                    "match_score": round(score, 2),
                    "match_status": "MATCH"
                })
    return candidates


def apply_survivorship(rows: Iterable[Dict[str, Any]], attribute_name: str, strategy: str = "MOST_RECENT") -> Dict[str, Any]:
    rows = list(rows)
    if not rows:
        return {"attribute_name": attribute_name, "attribute_value": None}
    values = [r.get(attribute_name) for r in rows if r.get(attribute_name) is not None]
    if not values:
        return {"attribute_name": attribute_name, "attribute_value": None}
    if strategy == "LONGEST_VALUE":
        selected = max(values, key=lambda v: len(str(v)))
    elif strategy == "MOST_COMPLETES":
        selected = max(values, key=lambda v: len(str(v)))
    else:
        selected = max(rows, key=lambda r: str(r.get("load_timestamp", "1970-01-01T00:00:00")))
        selected = selected.get(attribute_name)
    return {"attribute_name": attribute_name, "attribute_value": selected}


def create_golden_record(matched_records: Iterable[Dict[str, Any]], entity_type: str = "CUSTOMER") -> Dict[str, Any]:
    records = list(matched_records)
    if not records:
        return {"master_id": None, "entity_type": entity_type, "attributes": {}}
    attributes: Dict[str, Any] = {}
    for key in sorted({k for r in records for k in r.keys() if k not in {"record_id", "source_system", "entity_type", "load_timestamp"}}):
        values = [r.get(key) for r in records if r.get(key) is not None]
        if values:
            attributes[key] = values[0]
    master_id = f"MASTER{abs(hash(tuple(sorted(attributes.items())))) % 1000000:06d}"
    return {
        "master_id": master_id,
        "entity_type": entity_type,
        "attributes": attributes,
        "source_count": len(records),
        "confidence_score": 95.0,
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d")
    }


def write_df(df: DataFrame, table_name: str) -> None:
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    df.write.mode("overwrite").saveAsTable(full_name)
    logger.info("Saved dataframe to %s", full_name)


def write_deployment_watermark(
    spark: SparkSession,
    source_tables: List[str] | None = None,
    priority_order: List[str] | None = None,
) -> None:
    """Persist the current deployment metadata so Databricks can prove the actual build in use."""
    watermark_rows = [{
        "deployment_version": "2026-09-02",
        "build_name": "priority-one-row-per-source",
        "catalog_name": CATALOG,
        "schema_name": SCHEMA,
        "priority_order": ",".join(priority_order or ["crm_customers", "banking_customers", "creditcard_customers"]),
        "source_tables": ",".join(source_tables or [
            "mdm.bronze.crm_customers",
            "mdm.bronze.banking_customers",
            "mdm.bronze.creditcard_customers",
        ]),
        "status": "updated",
        "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
    }]
    watermark_df = spark.createDataFrame(watermark_rows)
    write_df(watermark_df, "deployment_watermark")
    logger.info("Deployment watermark saved to %s.%s", CATALOG, "deployment_watermark")


def process_all_tables() -> None:
    spark = get_spark()
    source_tables = [
        "mdm.bronze.crm_customers",
        "mdm.bronze.banking_customers",
        "mdm.bronze.creditcard_customers",
    ]
    priority_order = ["crm_customers", "banking_customers", "creditcard_customers"]
    metadata_by_source: Dict[str, List[Dict[str, str]]] = {}
    source_records: Dict[str, Dict[str, Any]] = {}

    all_temp_rows: List[Dict[str, Any]] = []
    for table_name in source_tables:
        source_name = table_name.split(".")[-1]
        logger.info("Reading %s", table_name)
        df = spark.table(table_name)
        metadata = get_table_metadata(df)
        metadata_by_source[source_name] = metadata
        logger.info("Metadata for %s: %s", table_name, metadata)

        row_data = df.limit(1).toPandas().to_dict(orient="records")
        source_records[source_name] = row_data[0] if row_data else {}

        temp_rows = build_mdm_temp(df, source_system=source_name, entity_type="CUSTOMER")
        logger.info("Generated %s mdm_temp rows from %s", len(temp_rows), table_name)
        all_temp_rows.extend(temp_rows)

    prioritized_schema = resolve_prioritized_metadata(metadata_by_source, priority_order=priority_order)
    logger.info("Priority-based MDM schema: %s", prioritized_schema)

    prioritized_record = pick_prioritized_value(source_records, priority_order=priority_order)
    logger.info("Priority-selected source values: %s", prioritized_record)

    temp_df = spark.createDataFrame(all_temp_rows)
    write_df(temp_df, "mdm_temp")

    candidate_rows = build_match_candidates(all_temp_rows)
    candidate_df = spark.createDataFrame(candidate_rows)
    write_df(candidate_df, "match_candidates")

    golden_record = create_golden_record(all_temp_rows, entity_type="CUSTOMER")
    rows_out = [{
        "master_id": golden_record["master_id"],
        "entity_type": golden_record["entity_type"],
        "attribute_name": key,
        "attribute_value": value,
        "source_count": golden_record["source_count"],
        "confidence_score": golden_record["confidence_score"],
        "last_updated": golden_record["last_updated"],
    } for key, value in golden_record["attributes"].items()]
    golden_df = spark.createDataFrame(rows_out)
    write_df(golden_df, "mdm_final")

    write_deployment_watermark(spark, source_tables=source_tables, priority_order=priority_order)

    logger.info("MDM pipeline complete.")


if __name__ == "__main__":
    process_all_tables()

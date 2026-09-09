from __future__ import annotations

from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from databricks_mdm import process_all_tables


def build_spark() -> SparkSession:
    """Return the active Databricks Spark session."""
    return SparkSession.getActiveSession()


def get_effective_catalog(spark: SparkSession, default_catalog: str = "mdm") -> str:
    """Return the current or fallback catalog that the caller is allowed to use."""
    try:
        current = str(spark.catalog.currentCatalog()).strip()
        if current:
            return current
    except Exception:
        pass
    return default_catalog


def _is_permission_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        "INSUFFICIENT_PERMISSIONS" in message
        or "CREATE on CATALOG" in message
        or "CREATE on SCHEMA" in message
        or "permission" in message.lower()
    )


def ensure_catalog_and_schema(spark: SparkSession, catalog: str = "mdm", schema: str = "bronze") -> Tuple[str, str]:
    """Create the catalog/schema only when permitted; otherwise use the current accessible catalog."""
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    except Exception as exc:  # pragma: no cover - Databricks permission behavior is environment-specific
        if not _is_permission_error(exc):
            raise
        fallback_catalog = get_effective_catalog(spark, default_catalog="main")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fallback_catalog}.{schema}")
        return fallback_catalog, schema

    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    except Exception as exc:  # pragma: no cover - Databricks permission behavior is environment-specific
        if not _is_permission_error(exc):
            raise
        fallback_catalog = get_effective_catalog(spark, default_catalog="main")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fallback_catalog}.{schema}")
        return fallback_catalog, schema

    return catalog, schema


def create_catalog_and_schema(spark: SparkSession) -> None:
    """Backward-compatible wrapper for the legacy setup helper."""
    ensure_catalog_and_schema(spark)


def crm_customers_rows() -> List[dict]:
    return [
        {
            "record_id": "CRM001",
            "customer_name": "Robert Smith",
            "email_id": "robert@gmail.com",
            "mobile_no": "+91-9876543210",
            "address": "12 Main Street, New York, NY"
        },
        {
            "record_id": "CRM002",
            "customer_name": "Rob Smith",
            "email_id": "robert@gmail.com",
            "mobile_no": "9876543210",
            "address": "12 Main Street, New York, NY"
        },
        {
            "record_id": "CRM003",
            "customer_name": "Alice Johnson",
            "email_id": "alice.johnson@gmail.com",
            "mobile_no": "+1-415-555-0111",
            "address": "88 Park Avenue, San Francisco, CA"
        },
    ]


def banking_customers_rows() -> List[dict]:
    return [
        {
            "record_id": "BANK100",
            "cust_name": "ROBERT SMITH",
            "mail": "robert@gmail.com",
            "phone": "9876543210",
            "addr": "12 Main St, New York, NY"
        },
        {
            "record_id": "BANK101",
            "cust_name": "Robert Smtih",
            "mail": "robert@ymail.com",
            "phone": "9876543210",
            "addr": "12 Main Street, New York, NY"
        },
        {
            "record_id": "BANK102",
            "cust_name": "Peter Jones",
            "mail": "peter.jones@bank.com",
            "phone": "5551234567",
            "addr": "10 Market Street, Boston, MA"
        },
    ]


def creditcard_customers_rows() -> List[dict]:
    return [
        {
            "record_id": "CC001",
            "customer_name": "Robert Smith",
            "email": "robert.smith@gmail.com",
            "mobile": "+91-9876543210",
            "street_address": "12 Main Street, New York, NY"
        },
        {
            "record_id": "CC002",
            "customer_name": "Robert Smyth",
            "email": "robert@gmail.com",
            "mobile": "9876543210",
            "street_address": "12 Main St, New York, NY"
        },
        {
            "record_id": "CC003",
            "customer_name": "Maria Garcia",
            "email": "maria.garcia@gmail.com",
            "mobile": "+1-202-555-0199",
            "street_address": "500 Lake Shore Dr, Chicago, IL"
        },
    ]


def create_demo_tables(spark: SparkSession) -> None:
    """Create the demo source tables with realistic variation and duplicates."""
    catalog, schema = ensure_catalog_and_schema(spark)
    table_prefix = f"{catalog}.{schema}"

    crm_schema = StructType([
        StructField("record_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email_id", StringType(), True),
        StructField("mobile_no", StringType(), True),
        StructField("address", StringType(), True),
    ])

    banking_schema = StructType([
        StructField("record_id", StringType(), True),
        StructField("cust_name", StringType(), True),
        StructField("mail", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("addr", StringType(), True),
    ])

    credit_schema = StructType([
        StructField("record_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("mobile", StringType(), True),
        StructField("street_address", StringType(), True),
    ])

    spark.createDataFrame(crm_customers_rows(), schema=crm_schema).write.mode("overwrite").saveAsTable(f"{table_prefix}.crm_customers")
    spark.createDataFrame(banking_customers_rows(), schema=banking_schema).write.mode("overwrite").saveAsTable(f"{table_prefix}.banking_customers")
    spark.createDataFrame(creditcard_customers_rows(), schema=credit_schema).write.mode("overwrite").saveAsTable(f"{table_prefix}.creditcard_customers")

    print(f"Created and loaded demo data into {table_prefix} source tables.")


def main() -> None:
    spark = build_spark()
    catalog, schema = ensure_catalog_and_schema(spark)
    print(f"Using catalog/schema {catalog}.{schema} for demo MDM tables.")
    create_demo_tables(spark)
    process_all_tables()
    print("Demo MDM pipeline completed successfully.")


if __name__ == "__main__":
    main()

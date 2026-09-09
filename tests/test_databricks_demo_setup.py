from types import SimpleNamespace

from databricks_demo_setup import ensure_catalog_and_schema


class FakeSpark:
    def __init__(self):
        self.catalog = SimpleNamespace(currentCatalog=lambda: "main")
        self.calls = []

    def sql(self, statement):
        self.calls.append(statement)
        if statement.startswith("CREATE CATALOG"):
            raise RuntimeError("INSUFFICIENT_PERMISSIONS: User does not have permission CREATE on CATALOG")
        return None


def test_ensure_catalog_and_schema_falls_back_when_catalog_creation_is_forbidden():
    spark = FakeSpark()

    ensure_catalog_and_schema(spark, catalog="mdm", schema="bronze")

    assert "CREATE CATALOG IF NOT EXISTS mdm" in spark.calls
    assert "CREATE SCHEMA IF NOT EXISTS main.bronze" in spark.calls

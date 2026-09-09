from pyspark.sql import SparkSession

from new_mdm_engine import build_mdm


def test_aliases_priority_and_unmatched_records():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("new-mdm-engine-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        spark.createDataFrame(
            [("CRM1", "Robert Smith", "robert@gmail.com"), ("CRM2", "Maria Garcia", "maria@gmail.com")],
            ["record_id", "fname", "email_id"],
        ).createOrReplaceTempView("crm")
        spark.createDataFrame(
            [("BANK1", "ROBERT SMITH", None), ("BANK2", "Peter Jones", "peter@gmail.com")],
            ["record_id", "cust_name", "mail"],
        ).createOrReplaceTempView("bank")

        mdm_temp, mdm_final = build_mdm(spark, ["crm", "bank"])

        assert mdm_temp.count() == 4
        assert mdm_temp.where("customer_name = 'ROBERT SMITH'").count() == 2
        assert mdm_final.where("customer_name = 'ROBERT SMITH'").count() == 1
        robert = mdm_final.where("customer_name = 'ROBERT SMITH'").first()
        assert robert.record_id == "CRM1"
        assert mdm_final.count() == 3
    finally:
        spark.stop()


def test_large_dataset_with_selected_ambiguity_fields():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("new-mdm-engine-large-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        crm_rows = [
            (f"CRM{i:04d}", f"Customer {i}", f"customer{i}@example.com", f"555{i:07d}")
            for i in range(1, 101)
        ]
        bank_rows = [
            (f"BANK{i:04d}", f"CUSTOMER {i}", f"customer{i}@example.com" if i <= 80 else f"bank{i}@example.com", f"555{i:07d}")
            for i in range(1, 101)
        ]
        spark.createDataFrame(crm_rows, ["record_id", "customer_name", "email", "mobile"]).createOrReplaceTempView("crm_large")
        spark.createDataFrame(bank_rows, ["record_id", "cust_name", "mail", "phone"]).createOrReplaceTempView("bank_large")

        mdm_temp, mdm_final = build_mdm(
            spark,
            ["crm_large", "bank_large"],
            match_columns=["email", "phone"],
        )

        assert mdm_temp.count() == 200
        assert mdm_final.count() == 100
        assert mdm_final.where("customer_name = 'CUSTOMER 1'").first().record_id == "CRM0001"
        assert mdm_final.where("customer_name = 'CUSTOMER 100'").first().record_id == "CRM0100"
    finally:
        spark.stop()
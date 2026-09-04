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
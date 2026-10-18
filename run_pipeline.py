import sys, os, time
from pyspark.sql import functions as F
from src.session import get_spark_session, DATA, OUT, AS_OF
from src.read_clean import (
    read_and_clean_customers, read_and_clean_accounts,
    clean_transactions, read_and_clean_merchants
)
from src.schemas import TXN_SCHEMA, DAY2_SCHEMA
from src.validate import validate_customers, validate_accounts, validate_transactions
from src.transforms import route, dq_summary, latest_by_key
from src.business_gold import enrich_fx, apply_business_rules, compute_analytics

def run_all():
    print("="*60)
    print("Starting NovaPay Bank PySpark Pipeline (All 60 Scenarios)")
    print("="*60)
    
    t_start = time.time()
    
    # 1. SparkSession (#1)
    spark = get_spark_session()
    print(f"[#1] Spark initialized. Version: {spark.version}, TimeZone: {spark.conf.get('spark.sql.session.timeZone')}")
    
    # 2. Customers Ingestion & Cleaning (#2, #3, #8, #10, #11, #12, #13, #15, #16, #17, #23, #24, #26, #27, #28)
    cust_raw, cust_ok, cust_l = read_and_clean_customers(spark)
    n_cust_raw = cust_raw.count()
    n_cust_corrupt = cust_raw.filter("_corrupt_record IS NOT NULL").count()
    print(f"[#2, #3] Customers read: {n_cust_raw}, corrupt: {n_cust_corrupt}")
    
    # 3. Accounts Ingestion & Cleaning (#4, #8, #16, #18, #19, #24, #27, #28)
    acc_raw, acc_ok, acc_l = read_and_clean_accounts(spark)
    n_acc_raw = acc_raw.count()
    n_acc_corrupt = acc_raw.filter("_corrupt_record IS NOT NULL").count()
    print(f"[#4] Accounts read: {n_acc_raw}, corrupt: {n_acc_corrupt}")
    
    # 4. Merchants Ingestion & Cleaning (#6, #25, #28)
    merch_raw, merch = read_and_clean_merchants(spark)
    print(f"[#6, #25] Merchants raw: {merch_raw.count()}, after clean: {merch.count()}")
    
    # 5. Transactions Ingestion & Cleaning (#5, #8, #14, #15, #19, #20, #21, #22, #23, #24, #28)
    txn_raw = (
        spark.read.schema(TXN_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(f"{DATA}/transactions/transactions_2025H1.json")
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .cache()
    )
    n_txn_raw = txn_raw.count()
    n_txn_corrupt = txn_raw.filter("_corrupt_record IS NOT NULL").count()
    print(f"[#5] Transactions read: {n_txn_raw}, corrupt: {n_txn_corrupt}")
    
    # Save corrupt transactions to quarantine
    txn_corrupt = (
        txn_raw.filter("_corrupt_record IS NOT NULL")
        .select(
            F.lit("transactions").alias("entity"),
            "_corrupt_record",
            F.current_timestamp().alias("quarantined_at")
        )
    )
    txn_corrupt.write.mode("overwrite").json(f"{OUT}/quarantine/corrupt_transactions")
    
    silver_txn = clean_transactions(txn_raw).cache()
    print(f"[#24, #28] Clean Silver Transactions: {silver_txn.count()}")
    
    # Write Silver Parquet tables (#28)
    (
        silver_txn.withColumn("txn_month", F.date_format("txn_date_ist", "yyyy-MM"))
        .write.mode("overwrite").partitionBy("txn_month")
        .parquet(f"{OUT}/silver/transactions")
    )
    cust_l.write.mode("overwrite").parquet(f"{OUT}/silver/customers")
    acc_l.write.mode("overwrite").parquet(f"{OUT}/silver/accounts")
    merch.write.mode("overwrite").parquet(f"{OUT}/silver/merchants")
    print("[#28] Silver tables written to output/silver/ successfully.")
    
    # 6. Validation & Quarantine (#29, #30, #31, #32, #33, #34, #35, #36)
    cust_dq = validate_customers(cust_l)
    acc_dq = validate_accounts(acc_l, cust_l)
    txn_dq = validate_transactions(silver_txn, acc_l)
    
    cust_valid = route(cust_dq, "customers", OUT).cache()
    acc_valid = route(acc_dq, "accounts", OUT).cache()
    txn_valid = route(txn_dq, "transactions", OUT).cache()
    print(f"[#34] Valid rows -> Customers: {cust_valid.count()}, Accounts: {acc_valid.count()}, Transactions: {txn_valid.count()}")
    
    # DQ Summary Report (#35)
    dq_rep_c = dq_summary(cust_dq, "customers")
    dq_rep_a = dq_summary(acc_dq, "accounts")
    dq_rep_t = dq_summary(txn_dq, "transactions")
    dq_report = dq_rep_c.unionByName(dq_rep_a).unionByName(dq_rep_t).orderBy("entity", F.desc("failed_rows"))
    dq_report.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/reports/dq")
    print("[#35] DQ report written to output/reports/dq")
    
    # Reconciliation (#36)
    n_parsed = n_txn_raw - n_txn_corrupt
    n_after_exact = silver_txn.count()
    n_valid = txn_valid.count()
    n_quarantined = spark.read.parquet(f"{OUT}/quarantine/transactions").count()
    n_dups = n_parsed - n_after_exact
    print(f"[#36] Reconciliation: read={n_txn_raw} corrupt={n_txn_corrupt} duplicates/old_versions={n_dups} quarantined={n_quarantined} valid={n_valid}")
    assert n_txn_raw == n_txn_corrupt + n_dups + n_quarantined + n_valid, "Reconciliation failed! Rows went missing!"
    print("[#36] Reconciliation assert passed perfectly!")
    
    # 7. Business Rules: FX, Balances, Fraud & AML (#37 - #46)
    fx_daily, txn_fx = enrich_fx(spark, txn_valid, DATA)
    txn_fx, daily_position, mcc_ref = apply_business_rules(spark, txn_fx, DATA)
    txn_fx = txn_fx.cache()
    print(f"[#37-#46] Business rules and risk scoring applied. Cached txn_fx count: {txn_fx.count()}")
    
    # 8. Analytics (#47 - #54)
    ok, customer_360, monthly, mom, rfm, top_merchants, summary = compute_analytics(spark, txn_fx, cust_valid)
    customer_360 = customer_360.cache()
    rfm = rfm.cache()
    print(f"[#47] Customer 360 count: {customer_360.count()}")
    
    # #54: Explode, collect and structs in reverse
    cust_accounts = (
        acc_valid.groupBy("customer_id")
        .agg(
            F.collect_list(F.struct("account_id", "account_type", "status")).alias("accounts"),
            F.count("*").alias("n_accounts")
        )
    )
    back = cust_accounts.select("customer_id", F.explode("accounts").alias("a")).select("customer_id", "a.*")
    assert back.count() == acc_valid.count(), "Scenario #54 reverse explode count mismatch!"
    print("[#54] Explode and collect structs check passed!")
    
    # 9. Next-Day Data: Schema Drift, Late Data, Replays & Merge (#7, #55)
    day2_path = f"{DATA}/transactions/transactions_2025-07-01.json"
    day2_raw = (
        spark.read.schema(DAY2_SCHEMA).option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(day2_path)
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .cache()
    )
    day2 = clean_transactions(day2_raw)
    merged = latest_by_key(
        silver_txn.unionByName(day2, allowMissingColumns=True),
        ["txn_id"], ["updated_at"]
    )
    late = day2.filter("txn_date_ist < DATE'2025-07-01'")
    replays = day2.join(silver_txn.select("txn_id"), "txn_id", "left_semi")
    print(f"[#7, #55] Day 2 merge: silver_txn={silver_txn.count()}, day2={day2.count()}, merged={merged.count()}, late={late.count()}, replays={replays.count()}")
    
    # 10. Gold Layer (#56, #57)
    fact_cols = [
        "txn_id", "account_id", "customer_id", "txn_ts", "txn_date_ist", "txn_type",
        "channel", "status", "currency", "amount", "amount_inr", "amount_usd",
        "signed_amount_inr", "category", "merchant_name", "merchant_location_city",
        "is_international", "risk_score", "risk_band"
    ]
    (
        txn_fx.select(*fact_cols).withColumn("txn_month", F.date_format("txn_date_ist", "yyyy-MM"))
        .repartition("txn_month")
        .write.mode("overwrite").partitionBy("txn_month").parquet(f"{OUT}/gold/fact_transactions")
    )
    
    (
        customer_360.join(rfm.select("customer_id", "segment"), "customer_id", "left")
        .coalesce(1).write.mode("overwrite").parquet(f"{OUT}/gold/customer_360")
    )
    
    daily_kpis = (
        ok.groupBy("txn_date_ist").agg(
            F.count("*").alias("txns"),
            F.round(F.sum("amount_inr"), 2).alias("volume_inr"),
            F.countDistinct("customer_id").alias("active_customers"),
            F.sum(F.col("is_international").cast("int")).alias("intl_txns")
        )
    )
    daily_kpis.coalesce(1).write.mode("overwrite").parquet(f"{OUT}/gold/daily_kpis")
    print("[#56] Gold fact_transactions, customer_360, daily_kpis written.")
    
    # #57: One alerts table from many rules (unionByName)
    RULE_FLAGS = [
        "flag_velocity", "flag_structuring", "flag_high_value",
        "flag_high_risk_mcc", "flag_dormant_activity"
    ]
    alerts = None
    for flag in RULE_FLAGS:
        part = (
            txn_fx.filter(F.col(flag))
            .select(
                "txn_id", "account_id", "customer_id", "txn_ts", "amount_inr",
                F.lit(flag.replace("flag_", "")).alias("rule"), "risk_score"
            )
        )
        alerts = part if alerts is None else alerts.unionByName(part)
    alerts.write.mode("overwrite").parquet(f"{OUT}/gold/alerts")
    print(f"[#57] Gold alerts written. Total alerts count: {alerts.count()}")
    
    # 11. Performance & Catalyst Inspection (#58)
    txn_fx.join(F.broadcast(mcc_ref.select("mcc", "description")), "mcc", "left").explain()
    txn_fx.unpersist()
    print("[#58] Plan inspection complete.")
    
    # 12. Python UDF vs Built-in Comparison (#59)
    from pyspark.sql.types import StringType
    @F.udf(StringType())
    def mask_pan_udf(pan):
        return None if pan is None else pan[:2] + "XXXXXX" + pan[-2:]
    mask_builtin = F.concat(F.substring("pan", 1, 2), F.lit("XXXXXX"), F.substring("pan", -2, 2))
    big = cust_valid.crossJoin(spark.range(20)).select("pan")
    for name, col in [("udf", mask_pan_udf("pan")), ("builtin", mask_builtin)]:
        t = time.time()
        big.select(col.alias("m")).agg(F.count("m")).collect()
        print(f"[#59] {name:8} {time.time() - t:.2f}s")
    
    print("="*60)
    print(f"Pipeline executed successfully in {time.time() - t_start:.2f} seconds!")
    print("="*60)
    spark.stop()

if __name__ == "__main__":
    run_all()

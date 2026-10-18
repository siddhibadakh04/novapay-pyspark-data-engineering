from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.session import get_spark_session, DATA, OUT, AS_OF
from src.schemas import CUST_SCHEMA, ACC_SCHEMA, TXN_SCHEMA, MERCH_SCHEMA
from src.transforms import (
    add_lineage, normalise_nulls, map_values, parse_date, parse_txn_ts,
    parse_amount, parse_percent, flatten, latest_by_key,
    GENDER, COUNTRY, TXN_TYPE, ACC_TYPE, CURRENCY, CHANNEL, BOOL
)

def read_and_clean_customers(spark, data_dir=DATA):
    """Scenarios #2, #3, #10, #11, #12, #13, #15, #16, #17, #23, #24, #26, #27"""
    cust_raw = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(CUST_SCHEMA)
        .csv(f"{data_dir}/customers.csv")
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .cache()
    )
    
    # #3: Split corrupt rows into quarantine
    cust_corrupt = (
        cust_raw.filter("_corrupt_record IS NOT NULL")
        .select(
            F.lit("customers").alias("entity"),
            "_corrupt_record",
            F.current_timestamp().alias("quarantined_at")
        )
    )
    cust_corrupt.write.mode("overwrite").json(f"{OUT}/quarantine/corrupt_customers")
    
    cust_ok = cust_raw.filter("_corrupt_record IS NULL").drop("_corrupt_record")
    cust_ok = add_lineage(cust_ok, "customers_csv")
    
    # #10: Normalise null tokens
    cust_n = normalise_nulls(cust_ok)
    
    # #11: Trim and fix casing
    cust_c = (
        cust_n
        .withColumn("first_name", F.initcap(F.trim("first_name")))
        .withColumn("last_name", F.initcap(F.trim("last_name")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("pan", F.upper(F.trim("pan")))
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("customer_id", F.upper(F.trim("customer_id")))
    )
    
    # #12: Standardise categories with mapping tables
    cust_c = (
        cust_c
        .withColumn("gender", map_values("gender", GENDER, "U"))
        .withColumn("country", map_values("country", COUNTRY, "UNKNOWN"))
        .withColumn("kyc_status", F.upper(F.trim("kyc_status")))
        .withColumn("risk_rating", F.upper(F.trim("risk_rating")))
    )
    
    # #13: Parse dates written in four formats
    cust_c = (
        cust_c
        .withColumn("dob_raw", F.col("dob"))
        .withColumn("dob", parse_date("dob"))
        .withColumn("customer_since", parse_date("customer_since"))
        .withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    )
    
    # #15: Parse money
    cust_c = (
        cust_c
        .withColumn("income_raw", F.col("annual_income"))
        .withColumn("annual_income", parse_amount("annual_income"))
    )
    
    # #16: Normalise booleans
    cust_c = cust_c.withColumn("is_active", map_values("is_active", BOOL))
    
    # #17: Clean phone numbers
    digits = F.regexp_replace("phone", r"\D", "")
    cust_c = (
        cust_c
        .withColumn("phone_raw", F.col("phone"))
        .withColumn(
            "phone",
            F.nullif(F.regexp_extract(digits, r"^(?:91|0)?([6-9]\d{9})$", 1), F.lit(""))
        )
    )
    
    # #23 & #24: Remove exact duplicates & Keep latest version per customer_id
    cust_l = latest_by_key(cust_c.dropDuplicates(), ["customer_id"], ["updated_at"])
    
    # #26: Derive new columns
    cust_l = (
        cust_l
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("age", F.floor(F.months_between(F.lit(AS_OF), "dob") / 12).cast("int"))
        .withColumn(
            "tenure_years",
            F.round(F.months_between(F.lit(AS_OF), "customer_since") / 12, 1)
        )
        .withColumn("email_domain", F.regexp_extract("email", r"@(.+)$", 1))
        .withColumn(
            "pan_holder_type",
            F.when(F.substring("pan", 4, 1) == "P", "INDIVIDUAL")
            .when(F.substring("pan", 4, 1) == "C", "COMPANY")
            .otherwise("OTHER")
        )
    )
    
    # #27: Business segments with CASE logic
    cust_l = cust_l.withColumn(
        "income_band",
        F.when(F.col("annual_income").isNull(), "UNKNOWN")
        .when(F.col("annual_income") < 500000, "MASS")
        .when(F.col("annual_income") < 1500000, "AFFLUENT")
        .when(F.col("annual_income") < 5000000, "HNI")
        .otherwise("ULTRA_HNI")
    )
    
    return cust_raw, cust_ok, cust_l

def read_and_clean_accounts(spark, data_dir=DATA):
    """Scenarios #4, #16, #18, #19, #24, #27"""
    acc_raw = (
        spark.read.option("header", True).option("sep", "|")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(ACC_SCHEMA)
        .csv(f"{data_dir}/accounts.csv")
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .cache()
    )
    acc_ok = acc_raw.filter("_corrupt_record IS NULL").drop("_corrupt_record")
    acc_ok = add_lineage(acc_ok, "accounts_csv")
    
    # #16: Parse percent
    acc_c = normalise_nulls(acc_ok).withColumn("interest_rate", parse_percent("interest_rate"))
    
    # #18: Validate and mask card numbers (Luhn) without a UDF
    card = F.regexp_replace("card_number", r"[\s-]", "")
    rev = F.reverse(F.split(card, ""))
    doubled = F.transform(
        rev,
        lambda d, i: F.when(
            i % 2 == 1,
            F.when(d.cast("int") * 2 > 9, d.cast("int") * 2 - 9).otherwise(d.cast("int") * 2)
        ).otherwise(d.cast("int"))
    )
    luhn_ok = F.aggregate(doubled, F.lit(0), lambda acc, x: acc + x) % 10 == 0
    
    acc_c = (
        acc_c
        .withColumn(
            "card_valid",
            F.when(F.col("card_number").isNull(), None)
            .otherwise(card.rlike(r"^\d{13,19}$") & luhn_ok)
        )
        .withColumn(
            "card_last4",
            F.when(F.col("card_number").isNotNull(), F.substring(card, -4, 4))
        )
        .drop("card_number")
    )
    
    # #19: Standardise accounts
    acc_c = (
        acc_c
        .withColumn("account_type", map_values("account_type", ACC_TYPE, "UNKNOWN"))
        .withColumn("currency", map_values("currency", CURRENCY, "UNKNOWN"))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("ifsc_code", F.upper(F.trim("ifsc_code")))
        .withColumn("open_date", parse_date("open_date"))
        .withColumn("closed_date", parse_date("closed_date"))
        .withColumn("credit_limit", parse_amount("credit_limit"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    
    # #24: Latest per account_id
    acc_l = latest_by_key(acc_c.dropDuplicates(), ["account_id"], ["updated_at"])
    
    # #27: Account age
    acc_l = acc_l.withColumn(
        "account_age_years",
        F.round(F.months_between(F.lit(AS_OF), "open_date") / 12, 1)
    )
    
    return acc_raw, acc_ok, acc_l

def clean_transactions(raw):
    """Scenarios #28: Clean transaction DataFrame"""
    df = raw.filter("_corrupt_record IS NULL").drop("_corrupt_record")
    df = add_lineage(df, "transactions_json")
    df = (
        df.withColumn("txn_ts_raw", F.col("txn_ts"))
        .withColumn("txn_ts", parse_txn_ts("txn_ts"))
        .withColumn("updated_at", parse_txn_ts("updated_at"))
        .withColumn(
            "txn_date_ist",
            F.to_date(F.from_utc_timestamp("txn_ts", "Asia/Kolkata"))
        )
        .withColumn("amount_raw", F.col("amount"))
        .withColumn("amount", parse_amount("amount"))
    )
    df = (
        normalise_nulls(df)
        .withColumn("txn_type", map_values("txn_type", TXN_TYPE, "UNKNOWN"))
        .withColumn("currency", map_values("currency", CURRENCY, "UNKNOWN"))
        .withColumn("channel", map_values("channel", CHANNEL, "UNKNOWN"))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("account_id", F.upper(F.trim("account_id")))
    )
    df = flatten(df).withColumnRenamed("merchant_name", "merchant_name_raw")
    df = (
        df.withColumn(
            "is_international",
            F.array_contains("tags", "international") | (F.col("currency") != "INR")
        )
        .withColumn("tag_count", F.size(F.coalesce("tags", F.array())))
        .withColumn("tags_text", F.array_join("tags", ","))
        .withColumn(
            "merchant_name",
            F.initcap(F.regexp_replace(F.trim("merchant_name_raw"), r"\s+", " "))
        )
        .withColumn(
            "mcc",
            F.when(F.col("merchant_mcc").rlike(r"^\d{4}$"), F.col("merchant_mcc"))
        )
        .withColumn(
            "mcc_invalid",
            F.col("merchant_mcc").isNotNull() & F.col("mcc").isNull()
        )
    )
    data_cols = [c for c in df.columns if not c.startswith("_")]
    return latest_by_key(df.dropDuplicates(data_cols), ["txn_id"], ["updated_at"])

def read_and_clean_merchants(spark, data_dir=DATA):
    """Scenarios #6, #25: Read multi-line JSON array and clean merchants"""
    merch_raw = (
        spark.read.option("multiLine", True)
        .schema(MERCH_SCHEMA)
        .json(f"{data_dir}/merchants.json")
    )
    merch = (
        merch_raw
        .withColumn("mcc", F.lpad(F.trim("category.mcc"), 4, "0"))
        .withColumn("merchant_name", F.trim("merchant_name"))
        .withColumn("city", F.col("address.city"))
        .withColumn("is_old", F.col("merchant_name").endswith("(old)"))
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("merchant_id").orderBy("is_old", "merchant_name")
            )
        )
        .filter("_rn = 1")
        .drop("_rn", "is_old")
    )
    return merch_raw, merch

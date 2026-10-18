from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.session import AS_OF, DATA, OUT

def enrich_fx(spark, txn_valid, data_dir=DATA):
    """Scenarios #37 & #38: FX clean, forward fill, currency conversion to INR and USD"""
    fx_raw = spark.read.option("header", True).csv(f"{data_dir}/fx_rates.csv")
    fx = (
        fx_raw.withColumn("rate_date", F.to_date("rate_date"))
        .withColumn("rate", F.col("rate").cast("decimal(18,6)"))
        .withColumn("rate", F.when(F.col("rate") > 0, F.col("rate"))) # 0, -1, N/A -> NULL
    )
    cal = (
        fx.agg(F.min("rate_date").alias("lo"), F.lit(AS_OF).cast("date").alias("hi"))
        .select(F.explode(F.sequence("lo", "hi")).alias("rate_date"))
    )
    w = Window.partitionBy("from_ccy").orderBy("rate_date").rowsBetween(Window.unboundedPreceding, 0)
    fx_daily = (
        cal.crossJoin(fx.select("from_ccy").distinct())
        .join(fx.select("rate_date", "from_ccy", "rate"), ["rate_date", "from_ccy"], "left")
        .withColumn("is_filled", F.col("rate").isNull())
        .withColumn("rate", F.last("rate", ignorenulls=True).over(w))
        .unionByName(
            cal.select(
                "rate_date",
                F.lit("INR").alias("from_ccy"),
                F.lit(1).cast("decimal(18,6)").alias("rate"),
                F.lit(False).alias("is_filled")
            )
        )
    )
    
    usd = fx_daily.filter("from_ccy = 'USD'").select("rate_date", F.col("rate").alias("usd_inr"))
    txn_fx = (
        txn_valid
        .join(
            F.broadcast(
                fx_daily.withColumnRenamed("from_ccy", "currency")
                .withColumnRenamed("rate_date", "txn_date_ist")
            ),
            ["txn_date_ist", "currency"],
            "left"
        )
        .join(
            F.broadcast(usd.withColumnRenamed("rate_date", "txn_date_ist")),
            "txn_date_ist",
            "left"
        )
        .withColumn("amount_inr", F.round(F.col("amount") * F.col("rate"), 2).cast("decimal(18,2)"))
        .withColumn(
            "amount_usd",
            F.round(F.col("amount_inr") / F.col("usd_inr"), 2).cast("decimal(18,2)")
        )
        .drop("rate", "is_filled")
    )
    return fx_daily, txn_fx

def apply_business_rules(spark, txn_fx, data_dir=DATA):
    """Scenarios #39 to #46: signed amounts, MCC broadcast, balances, fraud & AML rules, risk scoring"""
    # #39: Signed amounts and direction
    txn_fx = (
        txn_fx
        .withColumn("direction", F.when(F.col("txn_type") == "DEBIT", "OUT").otherwise("IN"))
        .withColumn(
            "signed_amount_inr",
            F.when(F.col("status") != "SUCCESS", F.lit(0).cast("decimal(18,2)"))
            .when(F.col("txn_type") == "DEBIT", -F.col("amount_inr"))
            .otherwise(F.col("amount_inr"))
        )
    )
    
    # #40: Enrich with MCC reference (broadcast join)
    mcc_ref = (
        spark.read.option("header", True).csv(f"{data_dir}/mcc_codes.csv")
        .withColumn("mcc_high_risk", F.col("is_high_risk") == "Y")
        .drop("is_high_risk")
    )
    txn_fx = (
        txn_fx.join(F.broadcast(mcc_ref), "mcc", "left")
        .withColumn(
            "category",
            F.coalesce(
                "category",
                F.when(F.col("channel") == "ATM", "Cash Withdrawal")
                .when(F.col("channel").isin("UPI", "NEFT", "IMPS"), "Transfers")
                .otherwise("Uncategorised")
            )
        )
        .withColumn("mcc_high_risk", F.coalesce("mcc_high_risk", F.lit(False)))
    )
    
    # #41: Running balance and daily closing position
    w_run = (
        Window.partitionBy("account_id").orderBy("txn_ts", "txn_id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    txn_fx = txn_fx.withColumn("running_net_inr", F.sum("signed_amount_inr").over(w_run))
    w_last = (
        Window.partitionBy("account_id", "txn_date_ist")
        .orderBy(F.desc("txn_ts"), F.desc("txn_id"))
    )
    daily_position = (
        txn_fx.withColumn("_rn", F.row_number().over(w_last))
        .filter("_rn = 1")
        .select("account_id", "txn_date_ist", F.col("running_net_inr").alias("closing_net_inr"))
    )
    
    # #42: Fraud rule 1 - velocity
    w_10m = (
        Window.partitionBy("account_id")
        .orderBy(F.col("txn_ts").cast("long"))
        .rangeBetween(-600, 0)
    )
    txn_fx = (
        txn_fx
        .withColumn(
            "txn_count_10m",
            F.when(F.col("card_last4").isNotNull(), F.count("*").over(w_10m))
        )
        .withColumn("flag_velocity", F.coalesce(F.col("txn_count_10m") >= 5, F.lit(False)))
    )
    
    # #43: AML rule 2 - structuring
    near_limit = (
        (F.col("txn_type") == "DEBIT")
        & F.col("channel").isin("IMPS", "NEFT", "UPI")
        & (F.col("amount_inr") >= 45000)
        & (F.col("amount_inr") < 50000)
    )
    w_24h = (
        Window.partitionBy("account_id").orderBy(F.col("txn_ts").cast("long"))
        .rangeBetween(-86400, 0)
    )
    txn_fx = (
        txn_fx.withColumn("_near", near_limit.cast("int"))
        .withColumn("near_limit_24h", F.sum("_near").over(w_24h))
        .withColumn("flag_structuring", near_limit & (F.col("near_limit_24h") >= 3))
        .drop("_near")
    )
    
    # #44: AML rule 3 - high value, high-risk merchants and odd hours
    hour_ist = F.hour(F.from_utc_timestamp("txn_ts", "Asia/Kolkata"))
    txn_fx = (
        txn_fx
        .withColumn("flag_high_value", F.col("amount_inr") >= 200000)
        .withColumn("flag_high_risk_mcc", F.col("mcc_high_risk") & (F.col("txn_type") == "DEBIT"))
        .withColumn(
            "flag_round_amount",
            (F.col("amount_inr") >= 10000) & (F.col("amount_inr") % 10000 == 0)
        )
        .withColumn("flag_odd_hour", hour_ist < 5)
    )
    
    # #45: Dormant accounts that suddenly move money
    txn_fx = txn_fx.withColumn(
        "flag_dormant_activity",
        (F.col("account_status") == "DORMANT")
        & (F.col("txn_type") == "DEBIT")
        & (F.col("status") == "SUCCESS")
    )
    
    # #46: Combine flags into a risk score
    WEIGHTS = {
        "flag_velocity": 40, "flag_structuring": 50, "flag_high_value": 20,
        "flag_high_risk_mcc": 25, "flag_dormant_activity": 35, "flag_odd_hour": 5,
        "flag_round_amount": 5
    }
    score = sum(F.when(F.col(c), w).otherwise(0) for c, w in WEIGHTS.items())
    txn_fx = (
        txn_fx.withColumn("risk_score", score)
        .withColumn(
            "risk_band",
            F.when(F.col("risk_score") >= 50, "HIGH")
            .when(F.col("risk_score") >= 25, "MEDIUM")
            .otherwise("LOW")
        )
    )
    
    return txn_fx, daily_position, mcc_ref

def compute_analytics(spark, txn_fx, cust_valid):
    """Scenarios #47 to #54: customer 360, favourite category, monthly spend matrix, RFM, top merchants, rollup, reverse struct"""
    ok = txn_fx.filter("status = 'SUCCESS'")
    
    # #47: Customer 360: aggregates per customer
    cust_metrics = ok.groupBy("customer_id").agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount_inr"))).alias("spend_inr"),
        F.sum(F.when(F.col("txn_type") != "DEBIT", F.col("amount_inr"))).alias("inflow_inr"),
        F.count("*").alias("txn_count"),
        F.round(F.avg("amount_inr"), 2).alias("avg_ticket_inr"),
        F.min("txn_ts").alias("first_txn_ts"),
        F.max("txn_ts").alias("last_txn_ts"),
        F.countDistinct("merchant_merchant_id").alias("distinct_merchants"),
        F.sum(F.col("risk_band").isin("HIGH", "MEDIUM").cast("int")).alias("alerts")
    )
    customer_360 = (
        cust_valid.select(
            "customer_id", "full_name", "city", "age", "income_band",
            "kyc_status", "risk_rating"
        )
        .join(cust_metrics, "customer_id", "left")
        .fillna({"txn_count": 0, "alerts": 0})
    )
    
    # #48: Favourite category per customer
    cat_spend = (
        ok.filter("txn_type = 'DEBIT'")
        .groupBy("customer_id", "category")
        .agg(F.sum("amount_inr").alias("cat_spend_inr"))
    )
    w_cat = Window.partitionBy("customer_id").orderBy(F.desc("cat_spend_inr"), "category")
    fav = (
        cat_spend.withColumn("rk", F.row_number().over(w_cat))
        .filter("rk = 1")
        .select("customer_id", F.col("category").alias("favourite_category"))
    )
    customer_360 = customer_360.join(fav, "customer_id", "left")
    
    # #49: Monthly spend matrix (pivot)
    MONTHS = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]
    monthly = (
        ok.filter("txn_type = 'DEBIT'")
        .withColumn("month", F.date_format("txn_date_ist", "yyyy-MM"))
        .groupBy("customer_id").pivot("month", MONTHS)
        .agg(F.round(F.sum("amount_inr"), 2))
        .fillna(0)
    )
    
    # #50: Unpivot back to rows (stack) and month-over-month change (lag)
    expr = "stack(6, " + ", ".join(f"'{m}', `{m}`" for m in MONTHS) + ") AS (month, spend_inr)"
    long_df = monthly.select("customer_id", F.expr(expr))
    w_m = Window.partitionBy("customer_id").orderBy("month")
    mom = (
        long_df.withColumn("prev_spend", F.lag("spend_inr").over(w_m))
        .withColumn(
            "mom_change_pct",
            F.round(
                (F.col("spend_inr") - F.col("prev_spend"))
                / F.when(F.col("prev_spend") != 0, F.col("prev_spend")) * 100,
                1
            )
        )
    )
    
    # #51: RFM segmentation with ntile
    rfm = (
        customer_360.filter("txn_count > 0")
        .withColumn("recency_days", F.datediff(F.lit(AS_OF), F.to_date("last_txn_ts")))
        .withColumn("r", 6 - F.ntile(5).over(Window.orderBy("recency_days")))
        .withColumn("f", F.ntile(5).over(Window.orderBy("txn_count")))
        .withColumn("m", F.ntile(5).over(Window.orderBy(F.col("spend_inr").asc_nulls_first())))
        .withColumn(
            "segment",
            F.when((F.col("r") >= 4) & (F.col("f") >= 4), "CHAMPION")
            .when(F.col("r") <= 2, "AT_RISK")
            .when(F.col("m") >= 4, "BIG_SPENDER")
            .otherwise("REGULAR")
        )
    )
    
    # #52: Top 3 merchants per city (dense_rank)
    merchant_spend = (
        ok.filter("merchant_merchant_id IS NOT NULL AND txn_type = 'DEBIT'")
        .groupBy("merchant_location_city", "merchant_name")
        .agg(
            F.round(F.sum("amount_inr"), 0).alias("spend_inr"),
            F.count("*").alias("txns")
        )
    )
    w_city = Window.partitionBy("merchant_location_city").orderBy(F.desc("spend_inr"))
    top_merchants = (
        merchant_spend.withColumn("rank", F.dense_rank().over(w_city))
        .filter("rank <= 3")
        .orderBy("merchant_location_city", "rank")
    )
    
    # #53: Totals and subtotals with rollup
    summary = (
        ok.filter("txn_type = 'DEBIT'")
        .rollup("channel", "category")
        .agg(
            F.round(F.sum("amount_inr") / 1e7, 2).alias("spend_crore"),
            F.count("*").alias("txns")
        )
        .withColumn(
            "level",
            F.when(F.col("channel").isNull(), "GRAND TOTAL")
            .when(F.col("category").isNull(), "CHANNEL TOTAL")
            .otherwise("DETAIL")
        )
        .orderBy(F.col("channel").asc_nulls_last(), F.col("category").asc_nulls_last())
    )
    
    return ok, customer_360, monthly, mom, rfm, top_merchants, summary

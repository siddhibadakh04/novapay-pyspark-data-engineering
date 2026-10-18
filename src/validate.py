from pyspark.sql import functions as F
from src.transforms import apply_rules

CUSTOMER_RULES = [
    {"name": "customer_id_format", "severity": "ERROR", "expr": "customer_id RLIKE '^CUST[0-9]{5}$'"},
    {"name": "dob_valid", "severity": "ERROR", "expr": "dob IS NOT NULL AND dob <= DATE'2025-07-01'"},
    {"name": "age_18_plus", "severity": "ERROR", "expr": "age >= 18"},
    {"name": "email_format", "severity": "WARN", "expr": "email RLIKE '^[a-z0-9._%+-]+@[a-z0-9-]+([.][a-z0-9-]+)*[.][a-z]{2,}$'"},
    {"name": "phone_valid", "severity": "WARN", "expr": "phone IS NOT NULL"},
    {"name": "pan_format", "severity": "WARN", "expr": "pan RLIKE '^[A-Z]{5}[0-9]{4}[A-Z]$'"},
    {"name": "pincode_format", "severity": "WARN", "expr": "pincode RLIKE '^[1-9][0-9]{5}$'"},
    {"name": "kyc_status_allowed", "severity": "WARN", "expr": "kyc_status IN ('VERIFIED', 'PENDING', 'REJECTED', 'EXPIRED')"},
    {"name": "income_valid", "severity": "WARN", "expr": "annual_income >= 0"},
]

ACCOUNT_RULES = [
    {"name": "account_type_known", "severity": "ERROR", "expr": "account_type <> 'UNKNOWN'"},
    {"name": "open_date_valid", "severity": "ERROR", "expr": "open_date IS NOT NULL"},
    {"name": "closed_after_open", "severity": "ERROR", "expr": "closed_date IS NULL OR closed_date >= open_date"},
    {"name": "closed_has_date", "severity": "WARN", "expr": "status <> 'CLOSED' OR closed_date IS NOT NULL"},
    {"name": "ifsc_format", "severity": "WARN", "expr": "ifsc_code RLIKE '^[A-Z]{4}0[A-Z0-9]{6}$'"},
    {"name": "limit_only_for_cards", "severity": "WARN", "expr": "(account_type = 'CREDIT_CARD') = (credit_limit IS NOT NULL)"},
    {"name": "card_luhn_valid", "severity": "ERROR", "expr": "account_type <> 'CREDIT_CARD' OR card_valid"},
    {"name": "currency_inr", "severity": "WARN", "expr": "currency = 'INR'"},
]

TXN_RULES = [
    {"name": "account_id_present", "severity": "ERROR", "expr": "account_id IS NOT NULL"},
    {"name": "account_exists", "severity": "ERROR", "expr": "account_id IS NULL OR account_status IS NOT NULL"},
    {"name": "txn_ts_valid", "severity": "ERROR", "expr": "txn_ts IS NOT NULL"},
    {"name": "txn_not_in_future", "severity": "ERROR", "expr": "txn_ts IS NULL OR txn_ts < TIMESTAMP'2025-07-01 00:00:00+05:30'"},
    {"name": "amount_parsed", "severity": "ERROR", "expr": "amount IS NOT NULL"},
    {"name": "amount_positive", "severity": "ERROR", "expr": "amount IS NULL OR amount > 0"},
    {"name": "currency_known", "severity": "ERROR", "expr": "currency <> 'UNKNOWN'"},
    {"name": "txn_type_known", "severity": "ERROR", "expr": "txn_type <> 'UNKNOWN'"},
    {"name": "channel_known", "severity": "WARN", "expr": "channel <> 'UNKNOWN'"},
    {"name": "status_known", "severity": "ERROR", "expr": "status IN ('SUCCESS', 'FAILED', 'PENDING')"},
    {"name": "not_after_account_closed", "severity": "ERROR", "expr": "closed_date IS NULL OR txn_date_ist <= closed_date"},
    {"name": "not_before_account_open", "severity": "WARN", "expr": "open_date IS NULL OR txn_date_ist >= open_date"},
    {"name": "mcc_valid", "severity": "WARN", "expr": "NOT coalesce(mcc_invalid, false)"},
]

def validate_customers(cust_l):
    """Scenario #29: Customer data quality rules"""
    return apply_rules(cust_l, CUSTOMER_RULES)

def validate_accounts(acc_l, cust_l):
    """Scenario #30 & #31: Account rules and customer referential integrity"""
    acc_dq = apply_rules(acc_l, ACCOUNT_RULES)
    known_cust = cust_l.select("customer_id", F.lit(True).alias("customer_exists"))
    acc_dq = (
        acc_dq.join(F.broadcast(known_cust), "customer_id", "left")
        .withColumn(
            "dq_issues",
            F.when(
                F.col("customer_exists").isNull(),
                F.array_union("dq_issues", F.array(F.lit("ERROR:customer_exists")))
            ).otherwise(F.col("dq_issues"))
        )
    )
    return acc_dq

def validate_transactions(silver_txn, acc_l):
    """Scenario #32 & #33: Transaction rules, account join, and refund validation"""
    acc_attr = acc_l.select(
        "account_id", "customer_id", "account_type",
        F.col("status").alias("account_status"), "open_date", "closed_date", "credit_limit"
    )
    txn_j = silver_txn.join(F.broadcast(acc_attr), "account_id", "left")
    txn_dq = apply_rules(txn_j, TXN_RULES)
    
    # #33: Validate refunds against original transaction (self-join)
    orig = silver_txn.select(
        F.col("txn_id").alias("original_txn_id"),
        F.col("account_id").alias("orig_account_id"),
        F.col("amount").alias("orig_amount"),
        F.col("currency").alias("orig_currency")
    )
    refund_check = (
        txn_dq.filter("txn_type = 'REFUND'")
        .join(orig, "original_txn_id", "left")
        .select(
            "txn_id",
            F.when(F.col("orig_amount").isNull(), "ERROR:refund_original_missing")
            .when(F.col("orig_account_id") != F.col("account_id"), "ERROR:refund_account_mismatch")
            .when(F.col("amount") > F.col("orig_amount"), "ERROR:refund_exceeds_original")
            .alias("refund_issue")
        )
        .filter("refund_issue IS NOT NULL")
    )
    txn_dq = (
        txn_dq.join(refund_check, "txn_id", "left")
        .withColumn(
            "dq_issues",
            F.when(
                F.col("refund_issue").isNotNull(),
                F.array_union("dq_issues", F.array("refund_issue"))
            ).otherwise(F.col("dq_issues"))
        )
        .drop("refund_issue")
    )
    return txn_dq

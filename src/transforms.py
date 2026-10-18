from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType

# --- Mapping constants ---
GENDER = {"M": "M", "MALE": "M", "F": "F", "FEMALE": "F", "O": "O", "OTHER": "O", "X": "O"}
COUNTRY = {"IN": "IN", "INDIA": "IN", "IND": "IN"}
TXN_TYPE = {
    "DEBIT": "DEBIT", "DR": "DEBIT", "D": "DEBIT",
    "CREDIT": "CREDIT", "CR": "CREDIT", "C": "CREDIT",
    "REFUND": "REFUND", "RFND": "REFUND"
}
ACC_TYPE = {
    "SAVINGS": "SAVINGS", "SB": "SAVINGS",
    "CURRENT": "CURRENT", "CA": "CURRENT",
    "CREDIT_CARD": "CREDIT_CARD", "CREDIT CARD": "CREDIT_CARD", "CC": "CREDIT_CARD",
    "FIXED_DEPOSIT": "FIXED_DEPOSIT", "FD": "FIXED_DEPOSIT", "FIXED DEPOSIT": "FIXED_DEPOSIT"
}
CURRENCY = {
    "INR": "INR", "RS": "INR", "₹": "INR",
    "USD": "USD", "$": "USD", "US$": "USD",
    "EUR": "EUR", "GBP": "GBP", "AED": "AED", "SGD": "SGD"
}
CHANNEL = {c: c for c in ["POS", "ECOM", "ATM", "UPI", "NEFT", "IMPS"]}
BOOL = {
    "Y": True, "YES": True, "1": True, "TRUE": True,
    "N": False, "NO": False, "0": False, "FALSE": False
}
NULL_TOKENS = ["", "null", "n/a", "na", "-", "none"]
DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MMM-yyyy", "yyyy/MM/dd"]
MONTHS = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]

# Scenario #8: Lineage helper
def add_lineage(df, source):
    return (
        df.withColumn("_source", F.lit(source))
        .withColumn("_loaded_at", F.current_timestamp())
    )

# Scenario #10: Normalise null tokens in every text column
def normalise_nulls(df):
    cols = []
    for c, t in df.dtypes:
        if t == "string" and not c.startswith("_"):
            v = F.trim(F.col(c))
            cols.append(F.when(F.lower(v).isin(NULL_TOKENS), None).otherwise(F.col(c)).alias(c))
        else:
            cols.append(F.col(c))
    return df.select(*cols)

# Scenario #12: Standardise categories with mapping tables
def map_values(col, mapping, default=None):
    m = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    v = m[F.upper(F.trim(F.col(col)))]
    return F.coalesce(v, F.lit(default)) if default is not None else v

# Scenario #13: Parse dates written in four formats
def parse_date(col):
    s = F.trim(F.col(col))
    return F.coalesce(*[F.to_date(s, f) for f in DATE_FORMATS])

# Scenario #14: Normalise timestamps from four styles to UTC
def parse_txn_ts(col):
    s = F.trim(F.col(col))
    return F.coalesce(
        F.when(s.rlike(r"^\d{13}$"), F.expr(f"timestamp_millis(cast(trim({col}) AS BIGINT))")),
        F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ssXXX"), # ISO + offset
        F.to_timestamp(F.concat(s, F.lit("+05:30")), "yyyy-MM-dd HH:mm:ssXXX"), # naive IST
        F.to_timestamp(F.concat(s, F.lit("+05:30")), "dd/MM/yyyy HH:mmXXX")
    )

# Scenario #15: Parse money: commas, Indian grouping, symbols and negatives
def parse_amount(col, scale="DECIMAL(18,2)"):
    s = F.trim(F.col(col))
    negative = s.rlike(r"^\(.*\)$") # (500.00) means -500.00
    digits = F.regexp_replace(s, r"[^0-9.\-]", "") # drop ₹, spaces letters ( )
    value = F.when(digits.rlike(r"^-?\d+(\.\d+)?$"), digits.cast(scale))
    return F.when(negative, -value).otherwise(value)

# Scenario #16: Normalise percentages
def parse_percent(col):
    s = F.trim(F.col(col))
    v = F.regexp_replace(s, "%", "").cast("double")
    return F.when(s.endswith("%") | (v >= 1), v).otherwise(F.round(v * 100, 4))

# Scenario #20: Flatten nested structs
def flatten(df, sep="_"):
    """Recursively turn struct fields into top-level columns: merchant.location.city -> merchant_location_city."""
    while True:
        structs = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        if not structs:
            return df
        cols = []
        for f in df.schema.fields:
            if f.name in structs:
                cols += [
                    F.col(f"`{f.name}`.`{s.name}`").alias(f"{f.name}{sep}{s.name}")
                    for s in f.dataType.fields
                ]
            else:
                cols.append(F.col(f"`{f.name}`"))
        df = df.select(*cols)

# Scenario #24: Keep the latest version per business key
def latest_by_key(df, keys, order_cols):
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# Scenario #29: Reusable data-quality rule engine
def apply_rules(df, rules):
    """
    Adds dq_issues: array like ['ERROR:age_18_plus', 'WARN:email_format'].
    A rule PASSES when its SQL expression is true; NULL counts as a failure.
    """
    checks = [
        F.when(~F.coalesce(F.expr(r["expr"]), F.lit(False)), F.lit(f'{r["severity"]}:{r["name"]}'))
        for r in rules
    ]
    return df.withColumn("dq_issues", F.filter(F.array(*checks), lambda x: x.isNotNull()))

# Scenario #34: Route rows by severity: quarantine vs Silver/Valid
def route(df, entity, out_dir="output"):
    has_error = F.exists("dq_issues", lambda x: x.startswith("ERROR"))
    good = df.filter(~has_error)
    bad = (
        df.filter(has_error)
        .select(
            F.lit(entity).alias("entity"),
            F.array_join("dq_issues", "; ").alias("reasons"),
            F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("payload"),
            F.current_timestamp().alias("quarantined_at")
        )
    )
    bad.write.mode("overwrite").parquet(f"{out_dir}/quarantine/{entity}")
    return good

# Scenario #35: Data-quality summary report generator
def dq_summary(df, entity):
    total = df.count()
    if total == 0:
        return None
    return (
        df.select(F.explode("dq_issues").alias("issue"))
        .groupBy("issue").count()
        .select(
            F.lit(entity).alias("entity"),
            F.split("issue", ":")[0].alias("severity"),
            F.split("issue", ":")[1].alias("rule"),
            F.col("count").alias("failed_rows"),
            F.round(F.col("count") / total * 100, 2).alias("failed_pct")
        )
    )

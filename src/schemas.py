# Schemas for NovaPay Data Sources
CUST_COLS = [
    "customer_id", "first_name", "last_name", "dob", "gender", "email", "phone", "pan",
    "address", "city", "state", "country", "pincode", "kyc_status", "risk_rating",
    "annual_income", "customer_since", "is_active", "updated_at"
]
CUST_SCHEMA = ", ".join(f"{c} STRING" for c in CUST_COLS) + ", _corrupt_record STRING"

ACC_COLS = [
    "account_id", "customer_id", "account_type", "account_number", "card_number",
    "ifsc_code", "branch_code", "currency", "open_date", "closed_date", "status",
    "credit_limit", "interest_rate", "updated_at"
]
ACC_SCHEMA = ", ".join(f"{c} STRING" for c in ACC_COLS) + ", _corrupt_record STRING"

TXN_SCHEMA = """
 txn_id STRING, account_id STRING, txn_ts STRING, amount STRING, currency STRING,
 txn_type STRING, channel STRING, status STRING,
 merchant STRUCT<merchant_id: STRING, name: STRING, mcc: STRING,
 location: STRUCT<city: STRING, country: STRING>>,
 card STRUCT<last4: STRING, present: BOOLEAN, network: STRING>,
 counterparty STRUCT<name: STRING, vpa: STRING, bank_ifsc: STRING>,
 tags ARRAY<STRING>, device STRUCT<ip: STRING, os: STRING>,
 original_txn_id STRING, description STRING, updated_at STRING, _corrupt_record STRING"""

DAY2_SCHEMA = TXN_SCHEMA + ", risk_score DOUBLE, fee STRUCT<amount: DOUBLE, currency: STRING>"

MERCH_SCHEMA = """merchant_id STRING, merchant_name STRING,
 category STRUCT<mcc: STRING, description: STRING>, aliases ARRAY<STRING>,
 address STRUCT<city: STRING, country: STRING, geo: STRUCT<lat: DOUBLE, lon: DOUBLE>>,
 risk_level STRING, onboarded_on STRING, is_active BOOLEAN"""

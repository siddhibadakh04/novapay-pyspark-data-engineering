import pytest
from decimal import Decimal
from pyspark.sql import SparkSession
from src.transforms import parse_amount, parse_date, parse_percent, normalise_nulls

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("novapay-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )

@pytest.mark.parametrize("raw, expected", [
    ("1,234.50", Decimal("1234.50")),
    ("8,83,000", Decimal("883000.00")),
    ("₹ 5,00,000", Decimal("500000.00")),
    ("(512.40)", Decimal("-512.40")),
    ("-50000", Decimal("-50000.00")),
    ("abc", None),
    (None, None)
])
def test_parse_amount(spark, raw, expected):
    """Scenario #60: Unit test parse_amount with exact parameters from PDF."""
    df = spark.createDataFrame([(raw,)], "a STRING")
    assert df.select(parse_amount("a").alias("v")).first().v == expected

def test_parse_percent(spark):
    df = spark.createDataFrame([("3.5%",), ("3.5",), ("0.0350",)], ["r"])
    results = [row.v for row in df.select(parse_percent("r").alias("v")).collect()]
    assert results == [3.5, 3.5, 3.5]

def test_parse_date(spark):
    df = spark.createDataFrame([
        ("2025-01-31",), ("31/01/2025",), ("31-Jan-2025",), ("2025/01/31",), ("31/02/1990",)
    ], ["d"])
    results = [str(row.v) for row in df.select(parse_date("d").alias("v")).collect()]
    assert results == ["2025-01-31", "2025-01-31", "2025-01-31", "2025-01-31", "None"]

def test_normalise_nulls(spark):
    df = spark.createDataFrame([("N/A", "valid"), ("none", "  "), ("-", "test")], ["c1", "c2"])
    cleaned = normalise_nulls(df)
    assert cleaned.filter("c1 IS NOT NULL").count() == 0
    assert cleaned.filter("c2 IS NULL").count() == 1

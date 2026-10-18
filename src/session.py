from pyspark.sql import SparkSession, Window, functions as F

def get_spark_session(app_name="novapay-weekend-2"):
    """
    Scenario #1: SparkSession for this project.
    - TimeZone: UTC
    - Shuffle partitions: 8 (optimized for local machine)
    - Adaptive query execution: enabled
    - Time parser policy: CORRECTED (strict, modern parser)
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )

DATA = "data"
OUT = "output"
AS_OF = "2025-07-01"

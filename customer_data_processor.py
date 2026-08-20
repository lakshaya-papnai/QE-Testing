import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CustomerDemographicsETL")

# ---------------------------------------------------------------------------
# SparkSession — local mode with fast failure on deterministic errors
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CustomerDemographicsETL")
    .config("spark.task.maxFailures", "1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def run_pipeline() -> None:
    """
    Execute the Customer Demographics ETL pipeline.

    Flow
    ----
    Bronze  ->  Silver  ->  Gold

    Bronze : Two customer DataFrames created with explicit schemas.
    Silver : Records with age < 0 are removed.
    Gold   : Both Silver DataFrames are combined with unionByName().
    """

    # ------------------------------------------------------------------
    # Initialise
    # ------------------------------------------------------------------
    logger.info("Initializing Customer Demographics ETL Job...")
    logger.info("Defining explicit schemas...")

    # ------------------------------------------------------------------
    # Schema definitions
    # 'name' MUST be StringType — the previous LongType/BIGINT declaration
    # caused SparkNumberFormatException (CAST_INVALID_INPUT, SQLSTATE 22018)
    # when Spark tried to cast string values like 'Alice' to BIGINT.
    # ------------------------------------------------------------------
    schema1 = StructType([
        StructField("id",   LongType(),   True),
        StructField("name", StringType(), True),   # FIX: was LongType() / BIGINT
        StructField("age",  IntegerType(), True),
    ])

    schema2 = StructType([
        StructField("id",   LongType(),   True),
        StructField("age",  IntegerType(), True),
        StructField("name", StringType(), True),   # FIX: was LongType() / BIGINT
        # Note: different column ORDER from schema1 — handled by .select() in Silver
    ])

    # ------------------------------------------------------------------
    # Bronze layer
    # ------------------------------------------------------------------
    logger.info("Extracting data into Bronze layer...")

    data1 = [
        (1, "Alice",      25),
        (2, "TestUser",   -1),
        (3, "Charlie",    30),
    ]

    data2 = [
        (4, 22,  "Dave"),
        (5, -5,  "BotAccount"),
        (6, 28,  "Eve"),
    ]

    df1_bronze = spark.createDataFrame(data1, schema=schema1)
    df2_bronze = spark.createDataFrame(data2, schema=schema2)

    # ------------------------------------------------------------------
    # Schema assertions — surface type errors immediately at definition
    # time rather than during distributed task execution.
    # ------------------------------------------------------------------
    dtypes1 = dict(df1_bronze.dtypes)
    dtypes2 = dict(df2_bronze.dtypes)

    assert dtypes1["name"] == "string", (
        f"df1_bronze: 'name' column must be StringType, got {dtypes1['name']}"
    )
    assert dtypes2["name"] == "string", (
        f"df2_bronze: 'name' column must be StringType, got {dtypes2['name']}"
    )

    # ------------------------------------------------------------------
    # Silver layer — filter negative ages and normalise column order
    # so that unionByName() can match columns by name regardless of the
    # different column orders in the two Bronze schemas.
    # ------------------------------------------------------------------
    logger.info("Filtering noisy data for Silver layer...")

    df1_silver = df1_bronze.filter("age >= 0").select("id", "name", "age")
    df2_silver = df2_bronze.filter("age >= 0").select("id", "name", "age")

    # ------------------------------------------------------------------
    # Gold layer
    # ------------------------------------------------------------------
    logger.info("Integrating Silver tables into Gold layer...")

    df_gold = df1_silver.unionByName(df2_silver)

    # ------------------------------------------------------------------
    # Row-count smoke-test
    # ------------------------------------------------------------------
    gold_count = df_gold.count()
    assert gold_count == 4, (
        f"Gold layer expected 4 records (ids 1, 3, 4, 6), got {gold_count}"
    )

    logger.info("Pipeline completed successfully.")

    # ------------------------------------------------------------------
    # Display results (Databricks-compatible; falls back to show() locally)
    # ------------------------------------------------------------------
    try:
        display(df_gold)  # noqa: F821  — available in Databricks notebooks
    except NameError:
        df_gold.show()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        run_pipeline()
    except AssertionError as exc:
        logger.error("Pipeline failed schema/count validation: %s", exc)
        spark.stop()
        raise
    except Exception as exc:
        logger.error("Pipeline failed during execution. Error details: %s", exc)
        spark.stop()
        raise
    finally:
        spark.stop()

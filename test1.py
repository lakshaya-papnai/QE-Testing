import logging
import sys
import os
from pyspark.sql.functions import col, lit, sum as _sum, avg, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# ==========================================
# 0. DATABRICKS-SAFE LOGGING SETUP
# ==========================================
logger = logging.getLogger("ChaosMasterPipeline")
logger.setLevel(logging.INFO)

if not logger.handlers:
    stream_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

logger.propagate = False

# ==========================================
# 1. CONFIGURE ERROR MODE HERE
# ==========================================
# 1 = Path Not Found [PATH_NOT_FOUND]
# 2 = Compute Crash (Massive Cross Join) [Killed / OOM]
# 3 = Positional Schema Mismatch [CAST_INVALID_INPUT]
# 4 = Schema Drift / Missing Column [UNRESOLVED_COLUMN]
#
# ERROR_MODE is sourced from a Databricks widget (EXECUTION_MODE) at runtime,
# falling back to the EXECUTION_MODE environment variable, then defaulting to 1.
# Register 'EXECUTION_MODE' as a job parameter in the Databricks Jobs UI
# with valid values 1-4 before scheduling this pipeline.
try:
    ERROR_MODE = int(dbutils.widgets.get("EXECUTION_MODE"))
except Exception:
    ERROR_MODE = int(os.environ.get("EXECUTION_MODE", "1"))

logger.info(f"Initializing Unified Chaos Pipeline (Running ERROR_MODE: {ERROR_MODE})...")

# ==========================================
# PRE-FLIGHT: Define required reference paths per mode
# ==========================================
TAX_FILE_PATH = "dbfs:/mnt/reference_data/regional_tax_rates_2026.csv"

def assert_dbfs_path_exists(path: str) -> None:
    """Raise a descriptive FileNotFoundError if the given DBFS path does not exist.
    Uses dbutils.fs.ls() which is available on all Databricks clusters.
    """
    try:
        dbutils.fs.ls(path)
    except Exception:
        raise FileNotFoundError(
            f"Required reference file not found at '{path}'. "
            "Ensure the file has been uploaded to the DBFS mount by the "
            "Data Engineering team before re-running this pipeline mode."
        )

try:
    # ==========================================
    # 2. BRONZE LAYER (Shared Mock Data)
    # ==========================================
    logger.info("Extracting core dimensions and facts...")
    
    cust_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("region", StringType(), True)
    ])
    customers_df = spark.createDataFrame([
        (101, "Alice Technologies", "NA"), 
        (102, "Bob Analytics", "EMEA"),
        (103, "Charlie Corp", "APAC")
    ], schema=cust_schema)

    prod_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True)
    ])
    products_df = spark.createDataFrame([
        (1, "Software", 1500.00), 
        (2, "Hardware", 850.50),
        (3, "Consulting", 3000.00)
    ], schema=prod_schema)

    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("qty", IntegerType(), True)
    ])
    orders_df = spark.createDataFrame([
        (5001, 101, 1, 5), 
        (5002, 101, 3, 1),
        (5003, 102, 2, 10),
        (5004, 103, 1, 2)
    ], schema=orders_schema)

    # ==========================================
    # 3. ERROR BRANCHING LOGIC (Silver/Gold Layers)
    # ==========================================
    
    if ERROR_MODE == 1:
        logger.info("Triggering Mode 1: Storage Trap (Missing Lookup File)...")
        
        try:
            # Valid ETL logic
            enriched_orders = orders_df.join(customers_df, "customer_id", "inner") \
                                       .join(products_df, "product_id", "inner") \
                                       .withColumn("total_revenue", col("qty") * col("price")) \
                                       .filter(col("total_revenue") > 1000)
                                       
            # Pre-flight check: verify the tax rates file exists on DBFS before
            # attempting to read it, so we get a clear actionable error rather
            # than a raw AnalysisException (SQLSTATE 42K03) mid-execution.
            logger.info("Verifying regional tax lookup table exists at: %s", TAX_FILE_PATH)
            assert_dbfs_path_exists(TAX_FILE_PATH)

            logger.info("Loading regional tax lookup table...")
            tax_rates_df = spark.read.option("header", "true").csv(TAX_FILE_PATH)
                                
            final_df = enriched_orders.join(tax_rates_df, "region", "left")
            display(final_df)

        except FileNotFoundError as e:
            logger.error("Mode 1 aborted — missing reference data: %s", str(e))
            raise


    elif ERROR_MODE == 2:
        logger.info("Triggering Mode 2: Compute Trap (Explosive Join)...")
        
        # Valid ETL logic up front
        customer_spend = orders_df.join(products_df, "product_id") \
                                  .withColumn("spend", col("qty") * col("price")) \
                                  .groupBy("customer_id").agg(_sum("spend").alias("total_spend"))
                                  
        active_customers = customers_df.join(customer_spend, "customer_id").filter(col("total_spend") > 0)
        
        # Trap: A developer tries to generate a "customer combination matrix" for a recommendation engine
        # but accidentally creates a Cartesian product (Cross Join) on two massive synthetic datasets
        logger.info("Generating customer similarity matrix...")
        synthetic_users_1 = spark.range(2000000).withColumnRenamed("id", "user_a")
        synthetic_users_2 = spark.range(2000000).withColumnRenamed("id", "user_b")
        
        explosion_matrix = synthetic_users_1.crossJoin(synthetic_users_2)
        
        # Triggers the execution plan and crashes the executors
        explosion_matrix.count()


    elif ERROR_MODE == 3:
        logger.info("Triggering Mode 3: Union Schema Mismatch Trap...")
        
        # Complex transformations to generate current month summary
        current_summary_df = orders_df.join(customers_df, "customer_id") \
                                      .join(products_df, "product_id") \
                                      .withColumn("revenue", col("qty") * col("price")) \
                                      .groupBy("region", "category") \
                                      .agg(_sum("revenue").alias("total_revenue"), avg("revenue").alias("avg_order_value")) \
                                      .select("region", "category", "total_revenue", "avg_order_value")
        
        # Trap: Legacy system provides historical data with the same names, but different physical order
        legacy_schema = StructType([
            StructField("category", StringType(), True),
            StructField("region", StringType(), True),
            StructField("avg_order_value", DoubleType(), True),
            StructField("total_revenue", DoubleType(), True)
        ])
        legacy_summary_df = spark.createDataFrame([
            ("Software", "NA", 1200.0, 50000.0),
            ("Hardware", "EMEA", 800.0, 25000.0)
        ], schema=legacy_schema)
        
        logger.info("Consolidating current and historical summaries...")
        # Crashes here: Tries to cast 'Software' (String) into 'region' (String is fine, but logical mismatch) 
        # or Double into String depending on exact Spark version strictness. 
        # But specifically tries to union physically instead of by name.
        consolidated_df = current_summary_df.union(legacy_summary_df)
        display(consolidated_df)


    elif ERROR_MODE == 4:
        logger.info("Triggering Mode 4: Schema Drift Trap...")
        
        # Trap Setup: Upstream silently renames 'qty' to 'order_quantity'
        drifted_orders_df = orders_df.withColumnRenamed("qty", "order_quantity")
        
        # Lengthy downstream logic that rigidly expects the old 'qty' column
        logger.info("Enriching orders with customer and product hierarchies...")
        silver_df = drifted_orders_df.join(customers_df, "customer_id", "inner") \
                                     .join(products_df, "product_id", "inner") \
                                     .filter(col("region").isin("NA", "EMEA"))
                                     
        logger.info("Calculating final financial metrics...")
        # Crashes here: [UNRESOLVED_COLUMN] Cannot resolve 'qty'
        gold_df = silver_df.withColumn("gross_margin", (col("qty") * col("price")) * lit(0.85)) \
                           .groupBy("customer_id", "name") \
                           .agg(_sum("gross_margin").alias("total_margin"))
                           
        display(gold_df)

    else:
        logger.info("No valid ERROR_MODE selected. Bypassing traps.")
        display(customers_df)

except Exception as e:
    logger.error("Pipeline failed during execution. Error details: %s", str(e))
    raise

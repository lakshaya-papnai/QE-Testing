from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_customer_data(spark: SparkSession):
    # Load current month's active customers
    df_current = spark.createDataFrame([
        (1, "Alice", "alice@example.com", 1500.50),
        (2, "Bob", "bob@example.com", 200.00)
    ], ["customer_id", "name", "email", "lifetime_value"])

    # Load legacy archived customers (Schema differs: lifetime_value is missing, phone is added)
    df_archived = spark.createDataFrame([
        (3, "Charlie", "charlie@example.com", "555-1234"),
        (4, "Diana", "diana@example.com", "555-5678")
    ], ["customer_id", "name", "email", "phone"])

    # Combine current and archived customers for full reporting
    # BUG: using union() instead of unionByName(allowMissingColumns=True)
    # This will cause an AnalysisException because schemas don't match exactly.
    df_combined = df_current.union(df_archived)

    # Calculate some metrics
    df_summary = df_combined.groupBy("customer_id").count()
    
    return df_summary

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CustomerDataProcessor").getOrCreate()
    try:
        result = process_customer_data(spark)
        result.show()
    except Exception as e:
        print(f"Error processing data: {e}")
    finally:
        spark.stop()

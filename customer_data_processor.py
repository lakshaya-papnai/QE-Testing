from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType


risk_schema = StructType([
    StructField("risk_label", StringType(), True),
    StructField("risk_score", FloatType(), True),
])
@udf(returnType=risk_schema)
def classify_risk(order_value):
    if order_value is None:
        return None
    if order_value > 1000:
        return ("HIGH", 0.9)   
    elif order_value > 300:
        return ("MEDIUM", 0.5)
    else:
        return ("LOW", 0.1)


def run_risk_classification(spark: SparkSession):
    df = spark.createDataFrame([
        (1, "order-A", 1500.0),
        (2, "order-B", 450.0),
        (3, "order-C", 80.0),
    ], ["customer_id", "order_id", "order_value"])

    df_classified = df.withColumn("risk", classify_risk(col("order_value")))
    df_final = df_classified.select(
        "customer_id", "order_id", "order_value",
        col("risk.risk_label").alias("risk_label"),
        col("risk.risk_score").alias("risk_score"),
    )

    return df_final


if __name__ == "__main__":
    spark = SparkSession.builder.appName("RiskClassificationPipeline").getOrCreate()
    try:
        result = run_risk_classification(spark)
        result.show()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

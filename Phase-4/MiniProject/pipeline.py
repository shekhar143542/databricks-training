"""
pipeline.py

Phase 4 Mini Project: Business Pipeline & Analytics
End-to-end PySpark pipeline: clean -> transform -> aggregate -> report.

Run:
    python pipeline.py

Input:
    data/sales.csv   (generate with data/generate_sample_data.py)

Output:
    output/report/   (final report, CSV, written by Task 7)
    Console printouts for Tasks 1-6 along the way.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT_PATH = "data/sales.csv"
OUTPUT_PATH = "output/report"


def get_spark():
    return (
        SparkSession.builder
        .appName("Phase4-Business-Pipeline")
        .master("local[*]")
        .getOrCreate()
    )


def load_raw(spark, path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def clean_data(df):
    """
    Data cleaning, applied before any joins/aggregations:
      - drop rows with null keys (customer_id)
      - drop duplicate rows
      - filter out invalid values (negative amounts)
      - enforce expected column types
    """
    cleaned = (
        df
        .dropDuplicates()
        .filter(F.col("customer_id").isNotNull())
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_date", F.to_date("order_date"))
        .filter(F.col("amount") > 0)
        .filter(F.col("customer_id").isNotNull())  # catches rows that failed the cast above
    )
    return cleaned


# ---------------------------------------------------------------------------
# Task 1: Daily Sales
# ---------------------------------------------------------------------------
def daily_sales(df):
    return (
        df.groupBy("order_date")
        .agg(F.sum("amount").alias("total_sales"))
        .orderBy("order_date")
    )


# ---------------------------------------------------------------------------
# Task 2: City-wise Revenue
# ---------------------------------------------------------------------------
def city_revenue(df):
    return (
        df.groupBy("city")
        .agg(F.sum("amount").alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
    )


# ---------------------------------------------------------------------------
# Task 3: Top 5 Customers
# ---------------------------------------------------------------------------
def top_customers(df, n=5):
    return (
        df.groupBy("customer_name")
        .agg(F.sum("amount").alias("total_spend"))
        .orderBy(F.desc("total_spend"))
        .limit(n)
    )


# ---------------------------------------------------------------------------
# Task 4: Repeat Customers (more than 1 order)
# ---------------------------------------------------------------------------
def repeat_customers(df):
    return (
        df.groupBy("customer_id")
        .agg(F.count("order_id").alias("order_count"))
        .filter(F.col("order_count") > 1)
        .orderBy(F.desc("order_count"))
    )


# ---------------------------------------------------------------------------
# Task 5: Customer Segmentation
#   total_spend > 10000        -> Gold
#   5000 <= total_spend <=10000 -> Silver
#   total_spend < 5000          -> Bronze
# ---------------------------------------------------------------------------
def segment_customers(df):
    spend = df.groupBy("customer_name").agg(F.sum("amount").alias("total_spend"))
    return spend.withColumn(
        "segment",
        F.when(F.col("total_spend") > 10000, "Gold")
         .when((F.col("total_spend") >= 5000) & (F.col("total_spend") <= 10000), "Silver")
         .otherwise("Bronze"),
    ).orderBy(F.desc("total_spend"))


# ---------------------------------------------------------------------------
# Task 6: Final Reporting Table
#   customer_name, city, total_spend, order_count, segment
# ---------------------------------------------------------------------------
def final_report(df):
    spend_and_segment = segment_customers(df)

    orders = df.groupBy("customer_id", "customer_name").agg(
        F.count("order_id").alias("order_count")
    )

    # A customer can order from multiple cities; take their most frequent city
    # (highest order count for that customer+city combo) so each customer
    # appears once in the final table.
    city_counts = df.groupBy("customer_id", "customer_name", "city").agg(
        F.count("order_id").alias("city_order_count")
    )
    w = Window.partitionBy("customer_id").orderBy(F.desc("city_order_count"))
    primary_city = (
        city_counts
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") == 1)
        .select("customer_id", "customer_name", "city")
    )

    report = (
        primary_city
        .join(orders, on=["customer_id", "customer_name"], how="inner")
        .join(spend_and_segment, on="customer_name", how="inner")
        .select("customer_name", "city", "total_spend", "order_count", "segment")
        .orderBy(F.desc("total_spend"))
    )
    return report


# ---------------------------------------------------------------------------
# Task 7: Save Output
# ---------------------------------------------------------------------------
def save_output(df, path):
    df.write.mode("overwrite").option("header", True).csv(path)


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    raw = load_raw(spark, INPUT_PATH)
    print(f"Raw row count: {raw.count()}")

    clean = clean_data(raw)
    print(f"Clean row count (after removing null keys, dupes, invalid amounts): {clean.count()}")
    clean.cache()

    print("\n--- Task 1: Daily Sales ---")
    daily_sales(clean).show(10, truncate=False)

    print("\n--- Task 2: City-wise Revenue ---")
    city_revenue(clean).show(truncate=False)

    print("\n--- Task 3: Top 5 Customers ---")
    top_customers(clean).show(truncate=False)

    print("\n--- Task 4: Repeat Customers ---")
    repeat_customers(clean).show(truncate=False)

    print("\n--- Task 5: Customer Segmentation ---")
    segment_customers(clean).show(truncate=False)

    print("\n--- Task 6: Final Reporting Table ---")
    report = final_report(clean)
    report.show(truncate=False)

    print(f"\n--- Task 7: Saving final report to {OUTPUT_PATH} ---")
    save_output(report, OUTPUT_PATH)
    print("Done.")

    spark.stop()


if __name__ == "__main__":
    main()

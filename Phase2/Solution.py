"""
solution.py

SQL to PySpark – Phase 2 Bridge Pack

For each exercise: the SQL version runs first (via spark.sql on temp views),
then the equivalent PySpark DataFrame version runs right after it, so you
can compare the two side by side.

Run:
    python solution.py

Input:
    customers.csv  (customer_id, customer_name, city)
    orders.csv     (order_id, customer_id, amount)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CUSTOMERS_PATH = "customers.csv"
ORDERS_PATH = "orders.csv"


def get_spark():
    return (
        SparkSession.builder
        .appName("SQL-to-PySpark-Bridge-Pack")
        .master("local[*]")
        .getOrCreate()
    )


def load_data(spark):
    customers = spark.read.option("header", "true").option("inferSchema", "true").csv(CUSTOMERS_PATH)
    orders = spark.read.option("header", "true").option("inferSchema", "true").csv(ORDERS_PATH)
    return customers, orders


def clean_data(customers, orders):
    """Mini cleaning task: remove rows with missing customer_id from both datasets."""
    customers = customers.dropna(subset=["customer_id"])
    orders = orders.dropna(subset=["customer_id"])
    return customers, orders


def banner(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    customers, orders = load_data(spark)

    print("--- Raw data ---")
    customers.show()
    orders.show()
    customers.printSchema()
    orders.printSchema()

    customers, orders = clean_data(customers, orders)

    # Register temp views so we can run plain SQL against them
    customers.createOrReplaceTempView("customers")
    orders.createOrReplaceTempView("orders")

    # -----------------------------------------------------------------
    # Exercise 1: Total order amount for each customer
    # -----------------------------------------------------------------
    banner("Exercise 1: Total order amount for each customer")

    print("[SQL]")
    spark.sql("""
        SELECT c.customer_id, c.customer_name, SUM(o.amount) AS total_spend
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name
        ORDER BY c.customer_id
    """).show()

    print("[PySpark]")
    ex1 = (
        customers.join(orders, on="customer_id", how="inner")
        .groupBy("customer_id", "customer_name")
        .agg(F.sum("amount").alias("total_spend"))
        .orderBy("customer_id")
    )
    ex1.show()

    # -----------------------------------------------------------------
    # Exercise 2: Top 3 customers by total spend
    # -----------------------------------------------------------------
    banner("Exercise 2: Top 3 customers by total spend")

    print("[SQL]")
    spark.sql("""
        SELECT c.customer_id, c.customer_name, SUM(o.amount) AS total_spend
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name
        ORDER BY total_spend DESC
        LIMIT 3
    """).show()

    print("[PySpark]")
    ex2 = (
        customers.join(orders, on="customer_id", how="inner")
        .groupBy("customer_id", "customer_name")
        .agg(F.sum("amount").alias("total_spend"))
        .orderBy(F.desc("total_spend"))
        .limit(3)
    )
    ex2.show()

    # -----------------------------------------------------------------
    # Exercise 3: Customers with no orders
    # -----------------------------------------------------------------
    banner("Exercise 3: Customers with no orders")

    print("[SQL]")
    spark.sql("""
        SELECT c.customer_id, c.customer_name
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        WHERE o.order_id IS NULL
    """).show()

    print("[PySpark]")
    ex3 = (
        customers.join(orders, on="customer_id", how="left")
        .filter(F.col("order_id").isNull())
        .select("customer_id", "customer_name")
    )
    ex3.show()

    # -----------------------------------------------------------------
    # Exercise 4: City-wise total revenue
    # -----------------------------------------------------------------
    banner("Exercise 4: City-wise total revenue")

    print("[SQL]")
    spark.sql("""
        SELECT c.city, SUM(o.amount) AS total_revenue
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.city
        ORDER BY total_revenue DESC
    """).show()

    print("[PySpark]")
    ex4 = (
        customers.join(orders, on="customer_id", how="inner")
        .groupBy("city")
        .agg(F.sum("amount").alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
    )
    ex4.show()

    # -----------------------------------------------------------------
    # Exercise 5: Average order amount per customer
    # -----------------------------------------------------------------
    banner("Exercise 5: Average order amount per customer")

    print("[SQL]")
    spark.sql("""
        SELECT c.customer_id, c.customer_name, AVG(o.amount) AS avg_order_amount
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name
        ORDER BY c.customer_id
    """).show()

    print("[PySpark]")
    ex5 = (
        customers.join(orders, on="customer_id", how="inner")
        .groupBy("customer_id", "customer_name")
        .agg(F.avg("amount").alias("avg_order_amount"))
        .orderBy("customer_id")
    )
    ex5.show()

    # -----------------------------------------------------------------
    # Exercise 6: Customers with more than one order
    # -----------------------------------------------------------------
    banner("Exercise 6: Customers with more than one order")

    print("[SQL]")
    spark.sql("""
        SELECT c.customer_id, c.customer_name, COUNT(o.order_id) AS order_count
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name
        HAVING COUNT(o.order_id) > 1
        ORDER BY order_count DESC
    """).show()

    print("[PySpark]")
    ex6 = (
        customers.join(orders, on="customer_id", how="inner")
        .groupBy("customer_id", "customer_name")
        .agg(F.count("order_id").alias("order_count"))
        .filter(F.col("order_count") > 1)
        .orderBy(F.desc("order_count"))
    )
    ex6.show()

    # -----------------------------------------------------------------
    # Exercise 7: Sort customers by total spend descending
    #   (includes customers with zero orders, unlike Exercise 2's inner join)
    # -----------------------------------------------------------------
    banner("Exercise 7: Sort customers by total spend descending")

    print("[SQL]")
    spark.sql("""
        SELECT c.customer_id, c.customer_name,
               COALESCE(SUM(o.amount), 0) AS total_spend
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name
        ORDER BY total_spend DESC
    """).show()

    print("[PySpark]")
    ex7 = (
        customers.join(orders, on="customer_id", how="left")
        .groupBy("customer_id", "customer_name")
        .agg(F.coalesce(F.sum("amount"), F.lit(0)).alias("total_spend"))
        .orderBy(F.desc("total_spend"))
    )
    ex7.show()

    spark.stop()


if __name__ == "__main__":
    main()

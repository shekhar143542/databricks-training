# SQL to PySpark — Phase 2 Bridge Pack

A hands-on bridge project to help understand how common SQL queries translate into equivalent **PySpark DataFrame** operations. Every exercise is implemented in both **SQL** and **PySpark**, making it easy to compare the syntax and understand the concepts side by side.

---

## Overview

This project contains sample datasets along with a runnable solution for **7 guided exercises**, where each solution is shown in both **SQL** and **PySpark**.

The script:

- Loads CSV files into Spark DataFrames.
- Cleans the data by removing rows with missing `customer_id`.
- Creates temporary SQL views.
- Executes each exercise using SQL.
- Solves the same exercise using the PySpark DataFrame API.
- Prints both outputs for easy comparison.

---

## Project Structure

```text
.
├── customers.csv        # customer_id, customer_name, city
│                        # (contains one row with a blank customer_id)
├── orders.csv           # order_id, customer_id, amount
│                        # (contains one row with a blank customer_id)
├── solution.py          # Loads data, cleans it, and solves all exercises
├── requirements.txt
└── README.md
```

---

## Sample Dataset

The datasets are intentionally designed so every exercise produces meaningful results.

- Customers **7** and **8** have **no orders** (Exercise 3).
- Customer **1** has **three orders** (Exercise 6).
- Both CSV files contain **one row with a missing `customer_id`**, allowing the data-cleaning step to demonstrate filtering invalid records.

---

## Features

- SQL and PySpark solutions shown side by side.
- Data cleaning before analysis.
- Uses Spark SQL with temporary views.
- Equivalent DataFrame API implementation for every query.
- Beginner-friendly examples for learning PySpark transformations.
- Easy to extend with additional SQL-to-PySpark exercises.

---

## Setup

Create and activate a virtual environment.

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
```

Install the required dependencies.

```bash
pip install -r requirements.txt
```

---

## Run

Execute the solution script.

```bash
python solution.py
```

For each exercise, the script prints:

1. SQL result (executed using `spark.sql()` against temporary views)
2. Equivalent PySpark DataFrame result

This makes it easy to compare both implementations and understand how SQL concepts map to PySpark.

---

## Exercises Covered

1. Total order amount for each customer
2. Top 3 customers by total spend
3. Customers with no orders
4. City-wise total revenue
5. Average order amount per customer
6. Customers with more than one order
7. Sort customers by total spend descending (including zero-order customers)

---

## SQL ↔ PySpark Mapping

| SQL | PySpark |
|------|----------|
| `JOIN ... ON` | `.join(other, on="col", how="inner")` |
| `LEFT JOIN` | `.join(other, on="col", how="left")` |
| `WHERE` | `.filter(...)` |
| `GROUP BY` | `.groupBy(...)` |
| `SUM()`, `AVG()`, `COUNT()` | `F.sum()`, `F.avg()`, `F.count()` inside `.agg()` |
| `HAVING` | `.filter(...)` applied *after* `.agg()` |
| `ORDER BY ... DESC` | `.orderBy(F.desc("col"))` |
| `COALESCE()` | `F.coalesce()` |

---

## Important Note

Exercise **3** and Exercise **7** require a **LEFT JOIN** instead of an **INNER JOIN** because they need to include customers who have **no matching records** in the `orders` table.

Using an inner join would exclude those customers entirely, making the results incorrect for these scenarios.

---

## Learning Objectives

After completing this project, you will understand how to:

- Read CSV files into Spark DataFrames.
- Clean and filter data before processing.
- Create temporary SQL views in Spark.
- Perform joins using SQL and PySpark.
- Use aggregation functions like `SUM`, `AVG`, and `COUNT`.
- Apply grouping, filtering, and sorting operations.
- Translate common SQL queries into equivalent PySpark DataFrame transformations.

---

## Technologies Used

- Python
- Apache Spark (PySpark)
- Spark SQL
- DataFrame API
- CSV datasets

---

## Author

**Shekhar**

A beginner-friendly learning project demonstrating SQL-to-PySpark query translation using Apache Spark.

# 🚀 Databricks Phase 3 – PySpark ETL Pipeline

A hands-on **PySpark ETL** project developed in **Databricks** to understand the complete **Extract, Transform, Load (ETL)** workflow used in modern data engineering. This project demonstrates reading data from CSV files, performing data cleaning and transformations, applying business logic, and loading the processed data into **Databricks Unity Catalog Volumes**.

---

## 📖 Project Overview

This project follows the standard **ETL (Extract, Transform, Load)** process commonly used in data engineering pipelines.

### 📥 Extract
- Read CSV datasets
- Inspect schema
- Explore data
- Validate input records

### 🔄 Transform
- Handle missing values
- Remove invalid records
- Filter data
- Perform aggregations
- Apply business logic
- Generate analytical reports

### 📤 Load
- Save processed data to Unity Catalog Volume
- Generate final reporting dataset
- Store transformed data for downstream analytics

---

## ✨ Features

- End-to-end ETL pipeline implementation
- Data cleaning and preprocessing
- Business-oriented data transformations
- Aggregation and reporting using PySpark
- Unity Catalog integration
- Beginner-friendly PySpark examples
- Databricks notebook implementation

---

## 🛠️ Technologies Used

- Python
- PySpark
- Apache Spark
- Databricks
- Unity Catalog
- CSV Files

---

## 📂 Project Structure

```text
Databricks-Phase-3/
│
├── data-bricks-phase-3.ipynb
├── db_customer.csv
├── db_sales.csv
├── README.md
```

---

## 📊 Dataset

### Customer Dataset

Contains customer information such as:

- Customer ID
- Customer Name
- Age
- City

### Sales Dataset

Contains sales transaction details including:

- Order ID
- Customer ID
- Order Date
- City
- Sales Amount

---

# 🔄 ETL Workflow

```text
          CSV Files
              │
              ▼
         EXTRACT
 Read Customer & Sales Data
              │
              ▼
        TRANSFORM
 • Clean Missing Values
 • Filter Invalid Records
 • Aggregate Sales
 • Apply Business Logic
 • Generate Reports
              │
              ▼
            LOAD
 Save Final Report to
 Unity Catalog Volume
```

---

# 📋 Tasks Performed

## 1. Read CSV Files

- Read customer dataset
- Read sales dataset
- Infer schema
- Display records

---

## 2. Inspect Data

- View DataFrame
- Print schema
- Validate columns

---

## 3. Data Cleaning

- Remove null values
- Fill missing values
- Remove duplicates
- Filter invalid ages
- Filter negative sales

---

## 4. Data Transformation

- Group By
- Aggregation
- Sorting
- Filtering
- Column Selection

---

## 5. Business Analysis

- Daily Sales Report
- City-wise Revenue
- Repeat Customers
- Highest Spending Customer
- Customer-wise Total Sales
- Order Count Report

---

## 6. Load Data

Processed data is written into a Unity Catalog Volume.

```python
report.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", True) \
      .csv("/Volumes/workspace/default/assignment_data/final_report")
```

---

# 🧠 PySpark Concepts Covered

- SparkSession
- DataFrame
- show()
- printSchema()
- select()
- filter()
- where()
- dropna()
- fillna()
- groupBy()
- agg()
- count()
- sum()
- alias()
- orderBy()
- coalesce()
- write()

---

# 📈 Business Problems Solved

- Customer Data Cleaning
- Sales Data Cleaning
- Daily Sales Calculation
- City-wise Revenue Analysis
- Repeat Customer Identification
- Customer Spending Analysis
- Final Reporting Table Generation

---

# 📊 Sample Output

| Customer ID | City | Total Spend | Order Count |
|-------------|------|------------:|------------:|
| 101 | Hyderabad | 4500 | 3 |
| 102 | Bangalore | 3200 | 2 |
| 103 | Chennai | 1800 | 1 |

---

# 🎯 Learning Outcomes

This project helped me understand:

- ETL Pipeline Development
- Data Engineering Fundamentals
- PySpark DataFrame API
- Data Cleaning Techniques
- Business Transformations
- Aggregations
- Databricks File Management
- Unity Catalog Volumes
- End-to-End ETL Workflow

---

## 🚀 Prerequisites

Before running this project, ensure you have:

- A Databricks Workspace
- Apache Spark Runtime
- Unity Catalog enabled
- Customer and Sales CSV datasets

---

# ▶️ How to Run

1. Open the Databricks notebook.
2. Upload the CSV datasets.
3. Create or use an existing Spark session.
4. Execute each ETL step in sequence.
5. Verify the generated reports.
6. Save the final report to the Unity Catalog Volume.

---

## 📸 Screenshots

You can add screenshots of:

- Dataset Upload
- Data Preview
- Schema Inspection
- Data Cleaning
- Business Reports
- Final Output
- Unity Catalog Volume

---

## 📌 Future Improvements

- Read data from JSON and Parquet files
- Connect to cloud storage (AWS S3 / Azure Data Lake)
- Automate ETL using Databricks Workflows
- Add Delta Lake support
- Schedule pipelines
- Integrate data quality validation
- Add incremental data processing

---

## 👨‍💻 Author

**Shekhar**

B.Tech – Artificial Intelligence & Data Science

Passionate about **Data Engineering | PySpark | Apache Spark | Databricks | ETL Pipelines**

---

⭐ If you found this project helpful, consider giving it a **Star** on GitHub!

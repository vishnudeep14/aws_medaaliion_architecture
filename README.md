# aws_medaliion_architecture
# 🛒 AWS Retail Data Lakehouse using Medallion Architecture

A complete data lakehouse pipeline built on **AWS Free Tier**, demonstrating **Bronze → Silver → Gold** layers using PySpark, S3, Glue, and Athena. Ideal for dashboards, analytics, and real-world retail data pipelines.

---

## 🚀 Architecture Overview

This project follows the **Medallion Architecture**:
- **Bronze Layer**: Raw CSV data ingested from synthetic retail transactions (Faker)
- **Silver Layer**: Cleaned, structured Parquet format using PySpark
- **Gold Layer**: Aggregated KPIs (e.g., total sales by category), ready for analytics

![Architecture](architecture.png)

---

## 📦 Tech Stack

| Layer     | Tools Used |
|-----------|------------|
| Bronze    | Python, Faker, AWS S3, Glue Crawler |
| Silver    | PySpark, Glue Job, Parquet           |
| Gold      | PySpark Aggregation, Athena          |
| Analytics | Amazon Athena / QuickSight           |

---

## 📂 Data Schema

**Raw Dataset (Bronze Layer)**:
| Column         | Description           |
|----------------|------------------------|
| Store_ID       | Store identifier       |
| Product_ID     | Product SKU            |
| Category       | Product category       |
| Date           | Transaction date       |
| Quantity_Sold  | Units sold             |
| Price          | Unit price in INR      |

---

## 🧪 Pipeline Steps

### 🔹 1. Bronze Layer (Raw Ingestion)
- Synthetic data generated using `faker` in Python.
- Stored in `s3://myretaildatawarehouse-123/retail-data/bronze/sales_data.csv`
- Glue Crawler creates table: `bronze_sales`

### 🔸 2. Silver Layer (Cleansing & Structuring)
```python
# PySpark Transformation
df = spark.read.csv(bronze_path, header=True, inferSchema=True)
df.write.mode("overwrite").parquet(silver_path)

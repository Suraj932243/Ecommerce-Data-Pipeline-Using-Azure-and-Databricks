# End-to-End E-Commerce Data Engineering Pipeline using Azure Data Factory, Azure Databricks and Spark Declarative Pipelines

## Project Overview

This project demonstrates an end-to-end Data Engineering solution built on Microsoft Azure using Azure Data Factory, Azure Databricks, Spark Declarative Pipelines (SDP), Unity Catalog, Delta Lake, and Azure Data Lake Storage Gen2.

The project follows the Medallion Architecture (Bronze → Silver → Gold) to ingest, clean, transform, and analyze e-commerce datasets.

---

# Architecture

```text
GitHub Repository
        ↓
Azure Data Factory
        ↓
Lookup Activity
        ↓
ForEach Activity
        ↓
Copy Data Activity
        ↓
ADLS Gen2 Bronze Container
        ↓
Bronze Tables
        ↓
Spark Declarative Pipelines
        ↓
Silver Layer
(Data Cleaning + Expectations)
        ↓
Gold Layer
(Business KPIs)
        ↓
Export to ADLS Gold Container
        ↓
Power BI
```

---

# Technologies Used

* Azure Data Factory
* Azure Databricks
* PySpark
* Spark Declarative Pipelines (SDP)
* Delta Lake
* Unity Catalog
* Azure Data Lake Storage Gen2
* Managed Identity
* External Locations
* Storage Credentials
* Azure SQL
* GitHub
* Python

---

# Dataset Source

The datasets were obtained from a public GitHub repository.

Datasets used:

* customers.csv
* orders.csv
* payments.csv
* products.csv

---

# Azure Data Factory Pipeline

Azure Data Factory is used to orchestrate the ingestion process.

Pipeline Flow:

```text
Lookup Files
      ↓
ForEach
      ↓
Copy Data Activity
      ↓
Databricks Activity
```

---

# Medallion Architecture

The project follows a Medallion Architecture:

```text
Bronze Layer
      ↓
Silver Layer
      ↓
Gold Layer
```

---

# Bronze Layer

Raw datasets are ingested into the Bronze container in Azure Data Lake Storage Gen2.

### Bronze Tables

* customers_bronze
* orders_bronze
* payments_bronze
* products_bronze

### Bronze Container

---

# Silver Layer

The Silver Layer performs data cleaning and quality checks.

### Transformations Applied

### Customers

* Duplicate removal
* Null validation
* Standardization of customer_city
* Capitalization using initcap()

### Orders

* Duplicate removal
* Timestamp conversion
* Delivery days calculation
* Null handling

### Payments

* Duplicate removal
* Data type casting
* String normalization
* Payment value conversion

### Products

* Duplicate removal
* Null handling
* Product category standardization
* Integer type casting

---

# Data Quality Expectations

Spark Declarative Pipelines expectations were used for validating records.

Examples:

### Customer

```python
@dp.expect_or_drop(
    "customer_id_not_null",
    "customer_id IS NOT NULL"
)
```

### Orders

```python
@dp.expect_or_drop(
    "order_id_not_null",
    "order_id IS NOT NULL"
)
```

### Payments

```python
@dp.expect_or_drop(
    "payment_positive",
    "payment_value >= 0"
)
```

### Products

```python
@dp.expect_or_drop(
    "product_id_not_null",
    "product_id IS NOT NULL"
)
```

Invalid records are automatically removed from the pipeline.

---

# Gold Layer

Business-level aggregations and KPIs are created in the Gold layer.

### Revenue Analysis

Table:

* revenue_by_city_gold

Metrics:

* Total revenue by city
* Average order value
* Total orders

---

### Delivery Performance Analysis

Table:

* delivery_performance_gold

Metrics:

* Average delivery days
* Late deliveries
* Late delivery percentage

---

### Payment Analysis

Table:

* payment_analysis_gold

Metrics:

* Revenue by payment type
* Average installments
* Average order value

---

### Product Analysis

Table:

* product_analysis_gold

Metrics:

* Product count by category
* Average dimensions
* Average weight
* Average number of photos

---

### Customer Segmentation (RFM)

Table:

* customer_segments_gold

Segments:

* VIP Customers
* Loyal Customers
* At Risk Customers
* New Customers

---

# Spark Declarative Pipeline DAG

The dependency graph is automatically managed by Databricks.

---

# Silver Container

Exported Silver Delta Tables:

* customers
* orders
* payments
* products

---

# Gold Container

Exported Gold Delta Tables:

* customer_segments
* delivery_performance
* payment_analysis
* product_analysis
* revenue_by_city

---

# Unity Catalog Components

### Storage Credential

Managed Identity-based authentication is used for secure access to ADLS Gen2.

---

### External Locations

Three external locations were configured:

* bronze_location
* silver_location
* gold_location

---

# Folder Structure

```text
Ecommerce_Azure_pipeline
│
├── config
│   ├── External_location.sql
│   ├── Read_File_Permission.py
│   └── Testing.py
│
├── exports
│   ├── silver_exports.py
│   └── gold_exports.py
│
├── transformations
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
│
├── screenshots
│
├── dataset_metadata.json
│
└── README.md
```

---

# Features

✅ Azure Data Factory Orchestration

✅ Medallion Architecture

✅ Spark Declarative Pipelines

✅ Unity Catalog

✅ Delta Lake

✅ Managed Identity Authentication

✅ External Locations

✅ Storage Credentials

✅ Data Quality Expectations

✅ Deduplication

✅ Timestamp Conversion

✅ Type Casting

✅ Revenue Analysis

✅ Delivery Performance Analysis

✅ Payment Analytics

✅ Product Analytics

✅ Customer Segmentation (RFM)

✅ Exporting Silver Layer to ADLS

✅ Exporting Gold Layer to ADLS

---

# Future Enhancements

* Databricks Workflows
* Auto CDC
* SCD Type 1
* SCD Type 2
* Streaming Tables
* CI/CD using GitHub Actions
* Power BI Dashboard
* Azure DevOps Integration

---

# Author

### Suraj Shivankar

Aspiring Data Engineer

---
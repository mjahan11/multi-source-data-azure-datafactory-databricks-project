# multi-source-data-azure-datafactory-databricks-project
Data engineering project using Azure Data Factory &amp; Databricks

# Medallion Architecture for End-to-End Data Flow
<img width="1334" height="680" alt="project image1" src="https://github.com/user-attachments/assets/b5785abc-c8c1-4257-9ea6-716830505e29" />


## Overview
This project demonstrates how to ingest, transform, and store data from multiple sources using **Azure Data Factory**, **Databricks**, and **Delta Lake**. It follows a **Bronze → Silver → Gold** medallion architecture to ensure clean and structured data pipelines.

## Business Requirements:

- Develop an end-to-end data pipeline for retail clients.
-  Ingest data from multiple sources and consolidate it into a data lake.
- Transaction, store, and product data are available in Azure SQL Database.
- Customer data is received from an API in JSON format.


## Project Components

## Bronze Layer
- Ingests raw data from multiple sources (SQL Database and API in JSON format)
- Stores the original data in Azure Data Lake Storage (ADLS) for future use
- No data transformations are applied

## Silver Layer
- Performs data cleaning and transformation using Databricks and SQL
- Manages data type conversions, null values, and duplicate records
- Prepares structured data for analytics and reporting

## Gold Layer
- Contains aggregated, business-ready datasets
- Combines multiple Silver layer tables
- Delivers the final data for Power BI dashboards
---

## Tools & Technologies
- **Azure Data Factory** – Orchestrates data pipelines
- **Azure Storage Account (ADLS Gen2)** – Stores raw, clean, and processed data
- **Databricks** – Data transformation and processing using PySpark and SQL
- **Delta Lake** – Ensures ACID-compliant data storage
- **Python / PySpark / SQL** – Used for scripting and performing data transformations in notebooks

---

## Folder Structure
notebooks/ <- Databricks notebooks for Bronze, Silver, and Gold layers
scripts/ <- Python / PySpark scripts
config/ <- Configuration files (if any)
README.md <- Project documentation


---

## How to Run

1. Configure your **Azure Storage Account** and **Databricks workspace**.
2. Upload the notebooks to Databricks.
3. Execute the notebooks in the following order:
   1. `Bronze` → ingest raw data
   2. `Silver` → clean and transform data
   3. `Gold` → aggregate and prepare business-ready datasets
4. Verify Delta tables in your Databricks workspace for each layer.

---

## Results / Screenshots:
# Azure Pipeline Activity:
<img width="1908" height="787" alt="image" src="https://github.com/user-attachments/assets/d3b1b3c2-baed-4a78-8aa5-535bd71e424b" />

# PowerBI Dashboard: 
<img width="1441" height="799" alt="image" src="https://github.com/user-attachments/assets/88f794be-2de5-4b21-8ed9-8ba9a11d5ed3" />



---

## Author
GitHub: [https://github.com/mjahan11](https://github.com/mjahan11)


---






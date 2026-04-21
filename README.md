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
- notebooks/ <- Databricks notebooks for Bronze, Silver, and Gold layers
- scripts/ <- Python / PySpark scripts
- config/ <- Configuration files (if any)
- README.md <- Project documentation


## ADLS folder structure (Bronze/Silver/Gold): 
<img width="906" height="247" alt="image" src="https://github.com/user-attachments/assets/eb7bc9f8-149d-47b0-9c6d-aad5f133d5cf" />


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

## Azure Databricks Implementation: Ingestion to Aggregation

 * Raw Data Ingestion & Bronze Layer Connectivity
   
<img width="1461" height="676" alt="screen1" src="https://github.com/user-attachments/assets/92c2aaff-7966-497e-930c-ab0393738620" />

 * Data Transformation & Silver Layer Modeling
<img width="1572" height="606" alt="screen3" src="https://github.com/user-attachments/assets/4ac98635-26ce-431f-ab84-83d798cf3d45" />

 *  Business Logic & Gold Layer Aggregations

<img width="1552" height="755" alt="screen5" src="https://github.com/user-attachments/assets/7c1fb608-61f3-4186-a2ae-c0d8c39cea85" />

# PowerBI Dashboard: 
<img width="1441" height="799" alt="image" src="https://github.com/user-attachments/assets/88f794be-2de5-4b21-8ed9-8ba9a11d5ed3" />



---

## Author
GitHub: [https://github.com/mjahan11](https://github.com/mjahan11)


---






# multi-source-data-azure-datafactory-databricks-project
Data engineering project using Azure Data Factory &amp; Databricks

# Medallion Architecture for End-to-End Data Flow
<img width="1696" height="608" alt="Gemini_Generated_Image_e96d9ae96d9ae96d" src="https://github.com/user-attachments/assets/11b95024-fbe8-42bf-8c46-2507e8697534" />


## Overview
This project demonstrates how to ingest, transform, and store data from multiple sources using **Azure Data Factory**, **Databricks**, and **Delta Lake**. It follows a **Bronze → Silver → Gold** medallion architecture to ensure clean and structured data pipelines.

# Business Requirements:
1.	We have to build an end-to-end data pipeline for the retail clients.
2.	We have data coming from multiple data sources and we need to bring the data into a data lake.
3.	We have transaction data available in AZURE SQL DB.
4.	We have store and products data available in AZURE SQL DB.
5.	We have customer data coming from API - JSON FORMAT.


## Project Components

### Bronze Layer
- Raw data ingestion from multiple sources (SQL DATABASE,API-JSON)
- Stores original data in Azure Data Lake Storage (ADLS) for future reference
- No transformations applied

### Silver Layer
- Data cleaning and transformation using Databricks and PySpark
- Handles data type conversions, null handling, and deduplication
- Prepares data for analytics and reporting

### Gold Layer
- Aggregated and business-ready datasets
- Joins multiple Silver tables
- Provides final output for Power BI dashboards 

---

## Tools & Technologies
- **Azure Data Factory** – Orchestrates data pipelines
- **Azure Storage Account (ADLS Gen2)** – Stores raw, clean, and processed data
- **Databricks** – Data transformation and processing using PySpark
- **Delta Lake** – Maintains ACID-compliant tables
- **Python / PySpark** – Scripts and notebook transformations

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
<img width="1892" height="922" alt="image" src="https://github.com/user-attachments/assets/7a148c23-ae01-4309-bbdf-1969f5995765" />



---

## Author
GitHub: [https://github.com/mjahan11](https://github.com/mjahan11)


---






# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.retailprojectstorageacct.dfs.core.windows.net",
  "key"
)


# COMMAND ----------
dbutils.fs.ls("abfss://retail@retailprojectstorageacct.dfs.core.windows.net/bronze")
dbutils.fs.ls("abfss://retail@retailprojectstorageacct.dfs.core.windows.net/bronze/transaction/")

# Read raw data from Bronze layer
df_transactions = spark.read.parquet("abfss://retail@retailprojectstorageacct.dfs.core.windows.net/bronze/transaction/")
df_store = spark.read.parquet("abfss://retail@retailprojectstorageacct.dfs.core.windows.net/bronze/store/")
df_product = spark.read.parquet("abfss://retail@retailprojectstorageacct.dfs.core.windows.net/bronze/product/")
df_customers = spark.read.parquet("abfss://retail@retailprojectstorageacct.dfs.core.windows.net/bronze/customer/")
display(df_transactions)
from pyspark.sql.functions import col# Convert types and clean data


# Convert types and clean data
df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_product.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_store.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])


# Join all data together
df_silver = df_transactions \
    .join(df_customers, "customer_id") \
    .join(df_products, "product_id") \
    .join(df_stores, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))

display(df_silver)

# dump the clean data to adls silver layer location

silver_path = "abfss://retail@retailprojectstorageacct.dfs.core.windows.net/silver/"

df_silver.write.mode("overwrite").format("delta").save(silver_path)

# COMMAND ----------
%sql
DROP EXTERNAL LOCATION IF EXISTS retail_silver_loc;

# COMMAND ----------
CREATE EXTERNAL LOCATION retail_silver_loc
URL 'abfss://retail@retailprojectstorageacct.dfs.core.windows.net/retail/silver'
WITH (STORAGE CREDENTIAL retailproject)
COMMENT 'External location for silver layer';

# COMMAND ----------
%sql
GRANT READ FILES, WRITE FILES
ON EXTERNAL LOCATION retail_silver_loc
TO `account users`;

# COMMAND ----------
df_clean = spark.read.format("delta").load("abfss://retail@retailprojectstorageacct.dfs.core.windows.net/silver/")
display(df_clean)
%sql

# COMMAND ----------
spark.sql("""
CREATE EXTERNAL LOCATION retail_silver_loc
URL 'abfss://retail@retailprojectstorageacct.dfs.core.windows.net/retail/silver'
WITH (STORAGE CREDENTIAL retailproject)
COMMENT 'External location for silver layer'
""")

# COMMAND ----------
%sql
GRANT READ FILES, WRITE FILES
ON EXTERNAL LOCATION retail_silver_loc
TO `account users`;
# COMMAND ---------- 
%sql
CREATE TABLE retail_silver_cleaned
USING DELTA
LOCATION 'abfss://unity-catalog-storage@dbstoragenwvfhtacdxxl2.dfs.core.windows.net/silver/retail_silver_cleaned';

# COMMAND ----------
%sql
SELECT * FROM retail_silver_cleaned
# Load cleaned DATA  from Silver layer 
silver_df = spark.read.format("delta").load('abfss://unity-catalog-storage@dbstoragenwvfhtacdxxl2.dfs.core.windows.net/silver/retail_silver_cleaned')
display(silver_df)
from pyspark.sql.functions import sum, countDistinct, avg

gold_df = silver_df.groupBy(
    "transaction_date",
    "product_id", "product_name", "category",
    "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)

# COMMAND ----------
display(gold_df)

gold_path = 'abfss://unity-catalog-storage@dbstoragenwvfhtacdxxl2.dfs.core.windows.net/retail/gold/'
gold_df.write.mode("overwrite").format("delta").save(gold_path)


display(dbutils.fs.ls('abfss://unity-catalog-storage@dbstoragenwvfhtacdxxl2.dfs.core.windows.net/retail/gold/'))
# 1. Define the path where we just saw the files
gold_path = 'abfss://unity-catalog-storage@dbstoragenwvfhtacdxxl2.dfs.core.windows.net/retail/gold/'

# 2. Read the data
gold_df.write.mode("overwrite").format("delta").save(gold_path)

# 3. View the data and the schema
display(gold_df)

# COMMAND ----------
spark.sql("""
CREATE TABLE retail_gold_sales_summary
USING DELTA
LOCATION 'abfss://unity-catalog-storage@dbstoragenwvfhtacdxxl2.dfs.core.windows.net/retail/gold/'
 """)
%sql
select * from retail_gold_sales_summary

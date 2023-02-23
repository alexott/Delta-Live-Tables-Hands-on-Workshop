# Databricks notebook source
# MAGIC %md
# MAGIC When creating the pipeline add the following configuration to point to the data source:
# MAGIC 
# MAGIC `data_source_path` : `/databricks-datasets/online_retail/data-001/`

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Read in Raw Data via Autoloader in Python
@dlt.table(
  comment = "This is the RAW bronze input data read in with Autoloader - no optimizations or expectations",
  partition_cols = ["Country"],
  table_properties={"quality" : "bronze"},
)
def raw_retail():
  location = spark.conf.get("data_source_path")
  df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .option("cloudFiles.schemaHints", "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity FLOAT, InvoiceDate STRING, UnitPrice FLOAT, CustomerID STRING, Country STRING") \
    .load(location)
  return df.select("*", F.col("_metadata.file_path").alias("inputFileName"))

# COMMAND ----------

# DBTITLE 1,Optimize Data Layout for Performance
@dlt.table(
  comment = "This is the raw bronze table with data cleaned (dates, etc.), data partitioned, and optimized",
  partition_cols = ["Country"],
  table_properties={"quality" : "bronze", "pipelines.autoOptimize.managed": "true", "pipelines.autoOptimize.zOrderCols": "CustomerID, InvoiceNo"},
)
def cleaned_retail():
  return dlt.read_stream("raw_retail")

# COMMAND ----------

# DBTITLE 1,Perform ETL & Enforce Quality Expectations
@dlt.table(
  comment = "This is the raw bronze table with data cleaned (dates, etc.), data partitioned, and optimized",
  partition_cols = ["Country"],
  table_properties={"quality" : "silver", "pipelines.autoOptimize.managed": "true", "pipelines.autoOptimize.zOrderCols": "CustomerID, InvoiceNo"},
)
@dlt.expect_all_or_drop({
  "has_customer": "CustomerID IS NOT NULL", 
  "has_invoice": "InvoiceNo IS NOT NULL", 
  "valid_date_time": "InvoiceDateTime IS NOT NULL"
})
def quality_retail():
  df = dlt.read_stream("cleaned_retail")
  df = df.withColumn("InvoiceDateTime",  F.expr("dateadd(YEAR, 2000, to_timestamp(InvoiceDate, 'd/M/y H:m'))")) \
    .withColumn("InvoiceDate", F.expr("cast(InvoiceDateTime as date)"))

  return df.select("InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "InvoiceDateTime", "UnitPrice", "CustomerID", "Country")

# COMMAND ----------



# COMMAND ----------

# df = spark.read.load("dbfs:/pipelines/ec1d6c3a-4043-4dd2-ab94-788af5597323/tables/cleaned_retail")

# df = df.withColumn("InvoiceDateTime",  F.expr("dateadd(YEAR, 2000, to_timestamp(InvoiceDate, 'd/M/y H:m'))")) \
#   .withColumn("InvoiceDate", F.expr("cast(InvoiceDateTime as date)"))
# display(df.select("InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "InvoiceDateTime", "UnitPrice", "CustomerID", "Country"))

# COMMAND ----------

# DBTITLE 1,Quarantine Data with Expectations
@dlt.table(
  comment = "Quarantine bad data",
  table_properties = {"quality" : "bronze", "pipelines.autoOptimize.managed": "true", "pipelines.autoOptimize.zOrderCols": "CustomerID, InvoiceNo"},
)
@dlt.expect_all_or_drop({
  "bad_record": "CustomerID IS NULL or InvoiceNo IS NULL or InvoiceDate IS NULL"
})
def quarantined_retail():
  return dlt.read_stream("cleaned_retail")

# COMMAND ----------

# DBTITLE 1,Upsert New Data APPLY CHANGES INTO
dlt.create_streaming_live_table(
  name = "retail_sales_all_countries",
  table_properties = {"quality" : "silver", 
    "pipelines.autoOptimize.managed": "true", 
    "pipelines.autoOptimize.zOrderCols": "CustomerID, InvoiceNo",
    "delta.tuneFileSizesForRewrites": "true",
  },
  schema="InvoiceNo STRING, StockCode STRING, Description STRING, Quantity FLOAT, InvoiceDate date, InvoiceDatetime TIMESTAMP, UnitPrice FLOAT, CustomerID STRING, Country STRING"
)

dlt.apply_changes(
  target = "retail_sales_all_countries",
  source = "quality_retail",
  keys = ["CustomerID", "InvoiceNo"],
  sequence_by = "InvoiceDateTime"
)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Create Complete Tables -- Use Case #1 --  for metadata downstream
@dlt.table
def distinct_countries_retail():
  df = dlt.read("quality_retail")
  return df.select("Country").dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Create Complete Tables -- Summary Analytics
@dlt.table
def sales_by_day():
  df = dlt.read("retail_sales_all_countries")
  return df.groupBy("InvoiceDate").agg(F.sum("Quantity").alias("TotalSales")).orderBy(F.desc("InvoiceDate"))

# COMMAND ----------

@dlt.table
def sales_by_country():
  df = dlt.read("retail_sales_all_countries")
  return df.groupBy("Country").agg(F.sum("Quantity").alias("TotalSales")).orderBy(F.desc("TotalSales"))

# COMMAND ----------

@dlt.table
def top_ten_customers():
  df = dlt.read("retail_sales_all_countries")
  return df.groupBy("Country").agg(F.sum("Quantity").alias("TotalSales")).orderBy(F.desc("TotalSales")).limit(10)

# COMMAND ----------

# DBTITLE 1,Just for Visuals -- Separate pipeline to split by country
@dlt.table(
  comment = "This is the raw bronze table with data cleaned (dates, etc.), data partitioned, and optimized",
  partition_cols = ["Country"],
  table_properties={"quality" : "silver", "pipelines.autoOptimize.managed": "true", "pipelines.autoOptimize.zOrderCols": "CustomerID, InvoiceNo"},
)
def quality_retail_split_by_country():
  return dlt.read_stream("quality_retail")

# COMMAND ----------



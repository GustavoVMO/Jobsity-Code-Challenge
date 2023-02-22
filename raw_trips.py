# Databricks notebook source
# Import required modules from PySpark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Set up Azure Blob Storage account information and file location
storage_account_name = "jobsity"
storage_account_access_key = "okkKcjU7p8U9A5yRD9mBY51432zoItBrpVXq2Sr8vdyWwOnonckmVum0ivGV4yG1MjHVv6p1jNLZ+ASt9LeFEg=="
blob_container = "raw"
file_location = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/trips.csv"
file_type = "csv"

# Set the Azure Blob Storage account key for PySpark to access the file
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# Define the schema for the data to be read in
trips_schema = StructType(fields=[StructField("region", StringType(), True),
                                  StructField("origin_coord", StringType(), True),
                                  StructField("destination_coord", StringType(), True),
                                  StructField("datetime", TimestampType(), True),
                                  StructField("datasource", StringType(), True),
])

# Read in the CSV file from Azure Blob Storage, using the defined schema and treating the first row as a header
df = spark.read.format(file_type).schema(trips_schema).option("header", "true").load(file_location)

# Write the data to a Parquet file format, and save it as a PySpark table
df.write.format("parquet").mode("overwrite").saveAsTable("raw_trips")
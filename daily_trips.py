# Databricks notebook source
from pyspark.sql.functions import date_format, hour

# Read the data from the raw_trips table
raw_trips_df = spark.table("raw_trips")

# Group the trips by region, date, and hour
daily_trips_df = raw_trips_df.groupBy(
    "region",
    date_format("datetime", "yyyy-MM-dd").alias("date"),
    hour("datetime").alias("hour")
).count()

# Rename the count column to trip_count
daily_trips_df = daily_trips_df.withColumnRenamed("count", "trips_count")

# Save the trips data to a new table named trips
daily_trips_df.write.format("parquet").mode("overwrite").saveAsTable("daily_trips")
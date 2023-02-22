# Databricks notebook source
# Import necessary libraries
from pyspark.sql.functions import avg, date_trunc, weekofyear, year

# Read the trips data from the CSV file and apply the schema
trips_df = spark.table("raw_trips")

# Calculate the weekly counts of trips for each region
trips_by_region_week = trips_df.groupBy("region", year("datetime").alias("year"), weekofyear("datetime").alias("week")) \
                              .count()

# Calculate the weekly average number of trips for each region
avg_trips_by_region = trips_by_region_week.groupBy("region", "year") \
                                          .agg(avg("count").alias("average_trips_per_week"))

# Save the trips data to a new table named weekly_average_trips
avg_trips_by_region.write.format("parquet").mode("overwrite").saveAsTable("weekly_average_trips")
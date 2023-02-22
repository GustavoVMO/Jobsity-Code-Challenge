# Databricks notebook source
# DBTITLE 1,From the two most commonly appearing regions, which is the latest datasource?
# MAGIC %sql
# MAGIC 
# MAGIC SELECT region, datasource
# MAGIC FROM (
# MAGIC   SELECT region, datasource, ROW_NUMBER() OVER (PARTITION BY region ORDER BY datetime DESC) AS row_num
# MAGIC   FROM raw_trips
# MAGIC   WHERE region IS NOT NULL AND datasource IS NOT NULL
# MAGIC ) AS ranked
# MAGIC WHERE row_num = 1
# MAGIC ORDER BY region
# MAGIC LIMIT 2

# COMMAND ----------

# DBTITLE 1,What regions has the "cheap_mobile" datasource appeared in?
# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT region
# MAGIC FROM raw_trips
# MAGIC WHERE datasource = 'cheap_mobile'

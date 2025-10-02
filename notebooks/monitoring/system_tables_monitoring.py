# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables Monitoring
# MAGIC
# MAGIC This notebook integrates with Databricks system tables to collect:
# MAGIC - Cost and billing data
# MAGIC - Job performance metrics
# MAGIC - Cluster utilization
# MAGIC - Query performance
# MAGIC - Table lineage
# MAGIC
# MAGIC **Requirements:**
# MAGIC - System tables enabled in Databricks account
# MAGIC - Unity Catalog enabled
# MAGIC - Appropriate permissions to query system tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Configuration parameters
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "observability", "Schema Name")
dbutils.widgets.text("days_back", "7", "Days to Look Back")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
days_back = int(dbutils.widgets.get("days_back"))

# Target tables for storing monitoring data
cost_metrics_table = f"{catalog}.{schema}.cost_metrics"
job_metrics_table = f"{catalog}.{schema}.job_metrics"
cluster_metrics_table = f"{catalog}.{schema}.cluster_metrics"
query_metrics_table = f"{catalog}.{schema}.query_metrics"

print(f"Monitoring Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Days Back: {days_back}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Target Tables

# COMMAND ----------

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Cost metrics table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cost_metrics_table} (
  usage_date DATE,
  workspace_id STRING,
  sku_name STRING,
  cloud STRING,
  usage_unit STRING,
  usage_quantity DOUBLE,
  list_price DOUBLE,
  cost DOUBLE,
  billing_origin_product STRING,
  usage_metadata MAP<STRING, STRING>,
  ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (usage_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

# Job metrics table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {job_metrics_table} (
  job_id BIGINT,
  job_name STRING,
  run_id BIGINT,
  run_name STRING,
  workspace_id BIGINT,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  result_state STRING,
  task_type STRING,
  cluster_id STRING,
  total_dbu_usage DOUBLE,
  total_cost DOUBLE,
  error_message STRING,
  ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(start_time))
""")

# Cluster metrics table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cluster_metrics_table} (
  timestamp TIMESTAMP,
  workspace_id BIGINT,
  cluster_id STRING,
  cluster_name STRING,
  cluster_source STRING,
  driver_node_type STRING,
  worker_node_type STRING,
  autoscale_min_workers INT,
  autoscale_max_workers INT,
  num_workers INT,
  state STRING,
  uptime_seconds DOUBLE,
  dbu_rate DOUBLE,
  ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(timestamp))
""")

# Query metrics table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {query_metrics_table} (
  query_id STRING,
  workspace_id BIGINT,
  query_text STRING,
  query_start_time TIMESTAMP,
  query_end_time TIMESTAMP,
  duration_ms BIGINT,
  rows_produced BIGINT,
  bytes_scanned BIGINT,
  executed_as_user_name STRING,
  executed_by_user_name STRING,
  warehouse_id STRING,
  error_message STRING,
  ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(query_start_time))
""")

print(f"✅ Schema and tables created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Cost and Billing Data

# COMMAND ----------

def collect_cost_metrics(days=7):
    """
    Collect cost and billing data from system.billing.usage
    """

    query = f"""
    SELECT
      usage_date,
      workspace_id,
      sku_name,
      cloud,
      usage_unit,
      usage_quantity,
      list_price,
      usage_quantity * list_price as cost,
      billing_origin_product,
      usage_metadata,
      current_timestamp() as ingestion_timestamp
    FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL {days} DAYS
    """

    try:
        cost_df = spark.sql(query)
        return cost_df
    except Exception as e:
        print(f"⚠️  Error collecting cost metrics: {e}")
        print("Note: System tables must be enabled. Contact your Databricks account admin.")
        return None

# Collect cost data
cost_df = collect_cost_metrics(days_back)

if cost_df:
    print(f"📊 Collected {cost_df.count()} cost metric records")

    # Show summary
    cost_summary = cost_df.groupBy("usage_date", "sku_name").agg(
        F.sum("cost").alias("total_cost"),
        F.sum("usage_quantity").alias("total_usage")
    ).orderBy(F.desc("usage_date"), F.desc("total_cost"))

    display(cost_summary.limit(20))

    # Write to monitoring table
    cost_df.write.mode("overwrite").saveAsTable(cost_metrics_table)
    print(f"✅ Cost metrics written to {cost_metrics_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Job Performance Metrics

# COMMAND ----------

def collect_job_metrics(days=7):
    """
    Collect job execution metrics from system.lakeflow.jobs
    """

    query = f"""
    SELECT
      job_id,
      job_name,
      run_id,
      run_name,
      workspace_id,
      start_time,
      end_time,
      CASE
        WHEN end_time IS NOT NULL
        THEN (unix_timestamp(end_time) - unix_timestamp(start_time))
        ELSE NULL
      END as duration_seconds,
      result_state,
      task_type,
      cluster_id,
      -- Estimate DBU usage based on duration (adjust multiplier based on cluster size)
      CASE
        WHEN end_time IS NOT NULL
        THEN (unix_timestamp(end_time) - unix_timestamp(start_time)) / 3600.0 * 2.0
        ELSE NULL
      END as total_dbu_usage,
      -- Estimate cost (DBU * price, adjust price based on your rates)
      CASE
        WHEN end_time IS NOT NULL
        THEN (unix_timestamp(end_time) - unix_timestamp(start_time)) / 3600.0 * 2.0 * 0.15
        ELSE NULL
      END as total_cost,
      error_message,
      current_timestamp() as ingestion_timestamp
    FROM system.lakeflow.jobs
    WHERE start_time >= current_timestamp() - INTERVAL {days} DAYS
    """

    try:
        job_df = spark.sql(query)
        return job_df
    except Exception as e:
        print(f"⚠️  Error collecting job metrics: {e}")
        return None

# Collect job data
job_df = collect_job_metrics(days_back)

if job_df:
    print(f"📊 Collected {job_df.count()} job execution records")

    # Show summary
    job_summary = job_df.groupBy("job_name", "result_state").agg(
        F.count("*").alias("run_count"),
        F.avg("duration_seconds").alias("avg_duration_seconds"),
        F.sum("total_cost").alias("total_cost")
    ).orderBy(F.desc("total_cost"))

    display(job_summary.limit(20))

    # Write to monitoring table
    job_df.write.mode("overwrite").saveAsTable(job_metrics_table)
    print(f"✅ Job metrics written to {job_metrics_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Cluster Utilization Metrics

# COMMAND ----------

def collect_cluster_metrics(days=7):
    """
    Collect cluster utilization metrics from system.compute.clusters
    """

    query = f"""
    SELECT
      timestamp,
      workspace_id,
      cluster_id,
      cluster_name,
      cluster_source,
      driver_node_type_id as driver_node_type,
      node_type_id as worker_node_type,
      autoscale.min_workers as autoscale_min_workers,
      autoscale.max_workers as autoscale_max_workers,
      num_workers,
      state,
      uptime_in_seconds as uptime_seconds,
      -- Estimate DBU rate based on node types (adjust based on actual SKU)
      CASE
        WHEN driver_node_type_id LIKE '%xl%' THEN 2.5
        WHEN driver_node_type_id LIKE '%large%' THEN 1.5
        ELSE 1.0
      END * (num_workers + 1) as dbu_rate,
      current_timestamp() as ingestion_timestamp
    FROM system.compute.clusters
    WHERE timestamp >= current_timestamp() - INTERVAL {days} DAYS
    """

    try:
        cluster_df = spark.sql(query)
        return cluster_df
    except Exception as e:
        print(f"⚠️  Error collecting cluster metrics: {e}")
        return None

# Collect cluster data
cluster_df = collect_cluster_metrics(days_back)

if cluster_df:
    print(f"📊 Collected {cluster_df.count()} cluster metric records")

    # Show summary
    cluster_summary = cluster_df.groupBy("cluster_name", "state").agg(
        F.avg("num_workers").alias("avg_workers"),
        F.sum("uptime_seconds").alias("total_uptime_seconds"),
        F.avg("dbu_rate").alias("avg_dbu_rate")
    ).orderBy(F.desc("total_uptime_seconds"))

    display(cluster_summary.limit(20))

    # Write to monitoring table
    cluster_df.write.mode("overwrite").saveAsTable(cluster_metrics_table)
    print(f"✅ Cluster metrics written to {cluster_metrics_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Query Performance Metrics

# COMMAND ----------

def collect_query_metrics(days=7):
    """
    Collect query performance metrics from system.query.history
    """

    query = f"""
    SELECT
      query_id,
      workspace_id,
      query_text,
      query_start_time,
      query_end_time,
      duration_ms,
      rows_produced,
      bytes_scanned,
      executed_as_user_name,
      executed_by_user_name,
      warehouse_id,
      error_message,
      current_timestamp() as ingestion_timestamp
    FROM system.query.history
    WHERE query_start_time >= current_timestamp() - INTERVAL {days} DAYS
    """

    try:
        query_df = spark.sql(query)
        return query_df
    except Exception as e:
        print(f"⚠️  Error collecting query metrics: {e}")
        return None

# Collect query data
query_df = collect_query_metrics(days_back)

if query_df:
    print(f"📊 Collected {query_df.count()} query execution records")

    # Show summary of slowest queries
    slow_queries = query_df.filter(
        F.col("duration_ms") > 10000  # > 10 seconds
    ).select(
        "query_id",
        "query_start_time",
        "duration_ms",
        "rows_produced",
        "bytes_scanned",
        F.substring("query_text", 1, 100).alias("query_preview")
    ).orderBy(F.desc("duration_ms"))

    display(slow_queries.limit(20))

    # Write to monitoring table
    query_df.write.mode("overwrite").saveAsTable(query_metrics_table)
    print(f"✅ Query metrics written to {query_metrics_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Cost Trends

# COMMAND ----------

spark.sql(f"""
SELECT
  usage_date,
  cloud,
  SUM(cost) as total_cost,
  SUM(usage_quantity) as total_dbu_usage
FROM {cost_metrics_table}
GROUP BY usage_date, cloud
ORDER BY usage_date DESC, cloud
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Cost Drivers by SKU

# COMMAND ----------

spark.sql(f"""
SELECT
  sku_name,
  billing_origin_product,
  SUM(cost) as total_cost,
  SUM(usage_quantity) as total_usage,
  COUNT(DISTINCT usage_date) as days_active,
  SUM(cost) / COUNT(DISTINCT usage_date) as avg_daily_cost
FROM {cost_metrics_table}
WHERE usage_date >= current_date() - INTERVAL {days_back} DAYS
GROUP BY sku_name, billing_origin_product
ORDER BY total_cost DESC
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Failure Analysis

# COMMAND ----------

spark.sql(f"""
SELECT
  job_name,
  result_state,
  COUNT(*) as run_count,
  AVG(duration_seconds) as avg_duration_seconds,
  MAX(duration_seconds) as max_duration_seconds,
  SUM(total_cost) as total_cost,
  MAX(start_time) as last_run_time
FROM {job_metrics_table}
WHERE start_time >= current_timestamp() - INTERVAL {days_back} DAYS
GROUP BY job_name, result_state
ORDER BY job_name, result_state
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most Expensive Jobs

# COMMAND ----------

spark.sql(f"""
SELECT
  job_name,
  COUNT(*) as run_count,
  SUM(total_cost) as total_cost,
  AVG(total_cost) as avg_cost_per_run,
  AVG(duration_seconds) as avg_duration_seconds,
  SUM(total_dbu_usage) as total_dbu_usage
FROM {job_metrics_table}
WHERE start_time >= current_timestamp() - INTERVAL {days_back} DAYS
  AND result_state = 'SUCCESS'
GROUP BY job_name
ORDER BY total_cost DESC
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster Utilization Summary

# COMMAND ----------

spark.sql(f"""
SELECT
  cluster_name,
  state,
  AVG(num_workers) as avg_workers,
  MAX(num_workers) as max_workers,
  SUM(uptime_seconds) / 3600.0 as total_uptime_hours,
  AVG(dbu_rate) as avg_dbu_rate,
  SUM(uptime_seconds) / 3600.0 * AVG(dbu_rate) as estimated_dbu_usage
FROM {cluster_metrics_table}
WHERE timestamp >= current_timestamp() - INTERVAL {days_back} DAYS
GROUP BY cluster_name, state
ORDER BY estimated_dbu_usage DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Slowest Queries Analysis

# COMMAND ----------

spark.sql(f"""
SELECT
  query_id,
  query_start_time,
  duration_ms / 1000.0 as duration_seconds,
  rows_produced,
  bytes_scanned / 1024.0 / 1024.0 as mb_scanned,
  executed_by_user_name,
  SUBSTRING(query_text, 1, 200) as query_preview
FROM {query_metrics_table}
WHERE query_start_time >= current_timestamp() - INTERVAL {days_back} DAYS
  AND error_message IS NULL
ORDER BY duration_ms DESC
LIMIT 25
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Error Patterns

# COMMAND ----------

spark.sql(f"""
SELECT
  SUBSTRING(error_message, 1, 100) as error_pattern,
  COUNT(*) as occurrence_count,
  COUNT(DISTINCT executed_by_user_name) as affected_users,
  MAX(query_start_time) as last_occurred
FROM {query_metrics_table}
WHERE query_start_time >= current_timestamp() - INTERVAL {days_back} DAYS
  AND error_message IS NOT NULL
GROUP BY SUBSTRING(error_message, 1, 100)
ORDER BY occurrence_count DESC
LIMIT 20
""").display()

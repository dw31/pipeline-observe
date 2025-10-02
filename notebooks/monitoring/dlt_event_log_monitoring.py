# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Event Log Monitoring
# MAGIC
# MAGIC This notebook collects and analyzes Delta Live Tables event logs to provide insights into:
# MAGIC - Pipeline execution status and performance
# MAGIC - Data quality expectation results
# MAGIC - Error patterns and failure analysis
# MAGIC - Pipeline lineage and dependencies
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Unity Catalog enabled
# MAGIC - DLT pipelines running with event logs stored
# MAGIC - Permissions to read DLT event logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json
from datetime import datetime, timedelta

# Configuration parameters
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "observability", "Schema Name")
dbutils.widgets.text("pipeline_id", "", "DLT Pipeline ID (optional)")
dbutils.widgets.text("days_back", "7", "Days to Look Back")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
pipeline_id = dbutils.widgets.get("pipeline_id")
days_back = int(dbutils.widgets.get("days_back"))

# Target tables for storing monitoring data
event_log_table = f"{catalog}.{schema}.dlt_event_logs"
quality_metrics_table = f"{catalog}.{schema}.dlt_quality_metrics"
pipeline_health_table = f"{catalog}.{schema}.dlt_pipeline_health"

print(f"Monitoring Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Days Back: {days_back}")
print(f"  Pipeline ID: {pipeline_id if pipeline_id else 'All pipelines'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Target Schema and Tables

# COMMAND ----------

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Create event log table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {event_log_table} (
  event_id STRING,
  pipeline_id STRING,
  pipeline_name STRING,
  update_id STRING,
  timestamp TIMESTAMP,
  event_type STRING,
  level STRING,
  message STRING,
  details MAP<STRING, STRING>,
  error STRING,
  origin MAP<STRING, STRING>,
  ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(timestamp))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

# Create quality metrics table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {quality_metrics_table} (
  pipeline_id STRING,
  pipeline_name STRING,
  update_id STRING,
  timestamp TIMESTAMP,
  dataset_name STRING,
  expectation_name STRING,
  expectation_query STRING,
  expectation_action STRING,
  passed_records BIGINT,
  failed_records BIGINT,
  pass_rate DOUBLE,
  ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(timestamp))
""")

# Create pipeline health summary table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {pipeline_health_table} (
  pipeline_id STRING,
  pipeline_name STRING,
  update_id STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  status STRING,
  num_datasets INT,
  total_records_processed BIGINT,
  total_expectations_checked INT,
  total_expectations_failed INT,
  overall_quality_score DOUBLE,
  error_count INT,
  warning_count INT,
  ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(start_time))
""")

print(f"✅ Schema and tables created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect DLT Event Logs

# COMMAND ----------

def get_dlt_event_logs(pipeline_id_filter=None, days=7):
    """
    Retrieve DLT event logs from system tables or storage location

    Args:
        pipeline_id_filter: Optional pipeline ID to filter
        days: Number of days to look back
    """

    # Query DLT system tables for event logs
    # Note: Adjust the path based on your DLT event log storage location

    query = f"""
    SELECT
      id as event_id,
      origin.pipeline_id,
      origin.pipeline_name,
      origin.update_id,
      timestamp,
      event_type,
      level,
      message,
      details,
      error,
      origin,
      current_timestamp() as ingestion_timestamp
    FROM event_log('pipelines/{pipeline_id_filter if pipeline_id_filter else '*'}')
    WHERE timestamp >= current_timestamp() - INTERVAL {days} DAYS
    """

    try:
        events_df = spark.sql(query)
        return events_df
    except Exception as e:
        print(f"⚠️  Error accessing event logs via SQL: {e}")
        print("Note: This requires DLT event log access. Ensure the pipeline ID and permissions are correct.")
        return None

# Collect event logs
events_df = get_dlt_event_logs(pipeline_id if pipeline_id else None, days_back)

if events_df:
    # Show sample of events
    print(f"📊 Collected {events_df.count()} event log entries")
    display(events_df.orderBy(F.desc("timestamp")).limit(10))

    # Write to monitoring table
    events_df.write.mode("append").saveAsTable(event_log_table)
    print(f"✅ Event logs written to {event_log_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Data Quality Metrics

# COMMAND ----------

def extract_quality_metrics(events_df):
    """
    Extract data quality expectation results from event logs
    """

    # Filter for flow_progress events that contain expectation results
    quality_events = events_df.filter(
        (F.col("event_type") == "flow_progress") &
        (F.col("details").isNotNull())
    )

    # Parse expectation details
    quality_df = quality_events.select(
        F.col("pipeline_id"),
        F.col("pipeline_name"),
        F.col("origin.update_id").alias("update_id"),
        F.col("timestamp"),
        F.col("details.flow_name").alias("dataset_name"),
        F.explode(F.col("details.data_quality.expectations")).alias("expectation")
    ).select(
        "pipeline_id",
        "pipeline_name",
        "update_id",
        "timestamp",
        "dataset_name",
        F.col("expectation.name").alias("expectation_name"),
        F.col("expectation.dataset").alias("expectation_query"),
        F.col("expectation.failed_records").cast("bigint").alias("failed_records"),
        F.col("expectation.passed_records").cast("bigint").alias("passed_records")
    ).withColumn(
        "pass_rate",
        F.when(
            F.col("passed_records") + F.col("failed_records") > 0,
            F.col("passed_records") / (F.col("passed_records") + F.col("failed_records"))
        ).otherwise(1.0)
    ).withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )

    return quality_df

if events_df:
    quality_df = extract_quality_metrics(events_df)

    if quality_df.count() > 0:
        print(f"📊 Extracted {quality_df.count()} quality metrics")
        display(quality_df.orderBy(F.desc("timestamp")).limit(10))

        # Write to quality metrics table
        quality_df.write.mode("append").saveAsTable(quality_metrics_table)
        print(f"✅ Quality metrics written to {quality_metrics_table}")
    else:
        print("ℹ️  No quality metrics found in event logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Pipeline Health Summary

# COMMAND ----------

def generate_pipeline_health_summary(events_df):
    """
    Generate pipeline health summary from event logs
    """

    # Get update start events
    update_starts = events_df.filter(
        F.col("event_type") == "update_progress"
    ).select(
        F.col("pipeline_id"),
        F.col("pipeline_name"),
        F.col("origin.update_id").alias("update_id"),
        F.col("timestamp").alias("start_time")
    )

    # Get update complete events
    update_completes = events_df.filter(
        F.col("event_type") == "update_progress"
    ).select(
        F.col("origin.update_id").alias("update_id"),
        F.col("timestamp").alias("end_time"),
        F.col("details.state").alias("status")
    )

    # Count errors and warnings per update
    error_counts = events_df.filter(
        F.col("level") == "ERROR"
    ).groupBy("origin.update_id").agg(
        F.count("*").alias("error_count")
    ).withColumnRenamed("update_id", "update_id_err")

    warning_counts = events_df.filter(
        F.col("level") == "WARN"
    ).groupBy("origin.update_id").agg(
        F.count("*").alias("warning_count")
    ).withColumnRenamed("update_id", "update_id_warn")

    # Join all together
    health_df = update_starts.join(
        update_completes, "update_id", "left"
    ).join(
        error_counts,
        F.col("update_id") == F.col("update_id_err"),
        "left"
    ).join(
        warning_counts,
        F.col("update_id") == F.col("update_id_warn"),
        "left"
    ).select(
        "pipeline_id",
        "pipeline_name",
        "update_id",
        "start_time",
        "end_time",
        F.when(F.col("end_time").isNotNull(),
               F.unix_timestamp("end_time") - F.unix_timestamp("start_time")
        ).alias("duration_seconds"),
        F.coalesce(F.col("status"), F.lit("RUNNING")).alias("status"),
        F.coalesce(F.col("error_count"), F.lit(0)).alias("error_count"),
        F.coalesce(F.col("warning_count"), F.lit(0)).alias("warning_count"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

    return health_df

if events_df:
    health_df = generate_pipeline_health_summary(events_df)

    if health_df.count() > 0:
        print(f"📊 Generated health summary for {health_df.count()} pipeline updates")
        display(health_df.orderBy(F.desc("start_time")).limit(10))

        # Write to health table
        health_df.write.mode("append").saveAsTable(pipeline_health_table)
        print(f"✅ Pipeline health summary written to {pipeline_health_table}")
    else:
        print("ℹ️  No pipeline health data found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Execution Summary

# COMMAND ----------

spark.sql(f"""
SELECT
  pipeline_name,
  status,
  COUNT(*) as execution_count,
  AVG(duration_seconds) as avg_duration_seconds,
  MAX(duration_seconds) as max_duration_seconds,
  SUM(error_count) as total_errors,
  SUM(warning_count) as total_warnings
FROM {pipeline_health_table}
WHERE start_time >= current_timestamp() - INTERVAL {days_back} DAYS
GROUP BY pipeline_name, status
ORDER BY pipeline_name, status
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Trends

# COMMAND ----------

spark.sql(f"""
SELECT
  DATE(timestamp) as date,
  dataset_name,
  expectation_name,
  AVG(pass_rate) as avg_pass_rate,
  SUM(failed_records) as total_failed_records,
  SUM(passed_records) as total_passed_records
FROM {quality_metrics_table}
WHERE timestamp >= current_timestamp() - INTERVAL {days_back} DAYS
GROUP BY DATE(timestamp), dataset_name, expectation_name
ORDER BY date DESC, dataset_name, expectation_name
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Failed Expectations Report

# COMMAND ----------

spark.sql(f"""
SELECT
  pipeline_name,
  dataset_name,
  expectation_name,
  timestamp,
  passed_records,
  failed_records,
  pass_rate,
  expectation_query
FROM {quality_metrics_table}
WHERE pass_rate < 1.0
  AND timestamp >= current_timestamp() - INTERVAL {days_back} DAYS
ORDER BY timestamp DESC, pass_rate ASC
LIMIT 50
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error Analysis

# COMMAND ----------

spark.sql(f"""
SELECT
  pipeline_name,
  event_type,
  level,
  error,
  COUNT(*) as occurrence_count,
  MAX(timestamp) as last_occurred
FROM {event_log_table}
WHERE level IN ('ERROR', 'WARN')
  AND timestamp >= current_timestamp() - INTERVAL {days_back} DAYS
GROUP BY pipeline_name, event_type, level, error
ORDER BY occurrence_count DESC, last_occurred DESC
LIMIT 25
""").display()

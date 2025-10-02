# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Monitoring Setup
# MAGIC
# MAGIC This notebook automates the setup of Databricks Lakehouse Monitoring for data quality profiling.
# MAGIC
# MAGIC **Features:**
# MAGIC - Create monitors for Delta tables
# MAGIC - Configure profile and time series metrics
# MAGIC - Set up drift detection
# MAGIC - Schedule monitoring jobs
# MAGIC - Access metrics and dashboards
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Unity Catalog enabled
# MAGIC - Lakehouse Monitoring available
# MAGIC - Delta tables to monitor

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import databricks.lakehouse_monitoring as lm
from pyspark.sql import functions as F

# Configuration parameters
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "observability", "Schema Name")
dbutils.widgets.text("table_to_monitor", "", "Table to Monitor (catalog.schema.table)")
dbutils.widgets.dropdown("monitor_type", "TimeSeries", ["TimeSeries", "Snapshot", "InferenceLog"], "Monitor Type")
dbutils.widgets.text("timestamp_col", "created_at", "Timestamp Column")
dbutils.widgets.text("granularity", "1 day", "Granularity")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_to_monitor = dbutils.widgets.get("table_to_monitor")
monitor_type = dbutils.widgets.get("monitor_type")
timestamp_col = dbutils.widgets.get("timestamp_col")
granularity = dbutils.widgets.get("granularity")

print(f"Lakehouse Monitoring Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Table to Monitor: {table_to_monitor}")
print(f"  Monitor Type: {monitor_type}")
print(f"  Timestamp Column: {timestamp_col}")
print(f"  Granularity: {granularity}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def create_timeseries_monitor(
    table_name: str,
    timestamp_col: str,
    granularities: list = ["1 day"],
    output_schema: str = None,
    baseline_table: str = None,
    slicing_exprs: list = None
):
    """
    Create a time series monitor for a Delta table

    Args:
        table_name: Full table name (catalog.schema.table)
        timestamp_col: Column to use for time series analysis
        granularities: List of time granularities (e.g., ["1 hour", "1 day"])
        output_schema: Schema to store monitoring metrics (defaults to same as table)
        baseline_table: Optional baseline table for drift detection
        slicing_exprs: Optional list of SQL expressions for data slicing
    """

    print(f"Creating time series monitor for {table_name}...")

    try:
        monitor_info = lm.create_monitor(
            table_name=table_name,
            profile_type=lm.TimeSeries(
                timestamp_col=timestamp_col,
                granularities=granularities
            ),
            output_schema_name=output_schema,
            baseline_table_name=baseline_table,
            slicing_exprs=slicing_exprs
        )

        print(f"✅ Monitor created successfully!")
        print(f"   Profile Metrics Table: {monitor_info.profile_metrics_table_name}")
        print(f"   Drift Metrics Table: {monitor_info.drift_metrics_table_name}")
        print(f"   Dashboard: {monitor_info.dashboard_id}")

        return monitor_info

    except Exception as e:
        print(f"❌ Error creating monitor: {e}")
        return None

def create_snapshot_monitor(
    table_name: str,
    output_schema: str = None,
    baseline_table: str = None,
    slicing_exprs: list = None
):
    """
    Create a snapshot monitor for a Delta table

    Args:
        table_name: Full table name (catalog.schema.table)
        output_schema: Schema to store monitoring metrics
        baseline_table: Optional baseline table for drift detection
        slicing_exprs: Optional list of SQL expressions for data slicing
    """

    print(f"Creating snapshot monitor for {table_name}...")

    try:
        monitor_info = lm.create_monitor(
            table_name=table_name,
            profile_type=lm.Snapshot(),
            output_schema_name=output_schema,
            baseline_table_name=baseline_table,
            slicing_exprs=slicing_exprs
        )

        print(f"✅ Monitor created successfully!")
        print(f"   Profile Metrics Table: {monitor_info.profile_metrics_table_name}")
        print(f"   Drift Metrics Table: {monitor_info.drift_metrics_table_name}")
        print(f"   Dashboard: {monitor_info.dashboard_id}")

        return monitor_info

    except Exception as e:
        print(f"❌ Error creating monitor: {e}")
        return None

def create_inference_monitor(
    table_name: str,
    timestamp_col: str,
    model_id_col: str,
    prediction_col: str,
    label_col: str = None,
    granularities: list = ["1 day"],
    output_schema: str = None,
    problem_type: str = None
):
    """
    Create an inference log monitor for ML model predictions

    Args:
        table_name: Full table name (catalog.schema.table)
        timestamp_col: Column with prediction timestamp
        model_id_col: Column with model identifier
        prediction_col: Column with model predictions
        label_col: Optional column with ground truth labels
        granularities: List of time granularities
        output_schema: Schema to store monitoring metrics
        problem_type: ML problem type (classification, regression)
    """

    print(f"Creating inference log monitor for {table_name}...")

    try:
        monitor_info = lm.create_monitor(
            table_name=table_name,
            profile_type=lm.InferenceLog(
                timestamp_col=timestamp_col,
                model_id_col=model_id_col,
                prediction_col=prediction_col,
                label_col=label_col,
                granularities=granularities,
                problem_type=problem_type
            ),
            output_schema_name=output_schema
        )

        print(f"✅ Monitor created successfully!")
        print(f"   Profile Metrics Table: {monitor_info.profile_metrics_table_name}")
        print(f"   Drift Metrics Table: {monitor_info.drift_metrics_table_name}")
        print(f"   Dashboard: {monitor_info.dashboard_id}")

        return monitor_info

    except Exception as e:
        print(f"❌ Error creating monitor: {e}")
        return None

def get_monitor_info(table_name: str):
    """Get information about an existing monitor"""
    try:
        monitor_info = lm.get_monitor(table_name=table_name)
        return monitor_info
    except Exception as e:
        print(f"⚠️  Monitor not found or error: {e}")
        return None

def refresh_monitor(table_name: str):
    """Manually refresh monitor metrics"""
    try:
        refresh_info = lm.run_refresh(table_name=table_name)
        print(f"✅ Monitor refresh started for {table_name}")
        print(f"   Refresh ID: {refresh_info.refresh_id}")
        return refresh_info
    except Exception as e:
        print(f"❌ Error refreshing monitor: {e}")
        return None

def delete_monitor(table_name: str):
    """Delete a monitor"""
    try:
        lm.delete_monitor(table_name=table_name)
        print(f"✅ Monitor deleted for {table_name}")
    except Exception as e:
        print(f"❌ Error deleting monitor: {e}")

print("✅ Helper functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Monitor for Specified Table

# COMMAND ----------

if table_to_monitor:
    # Check if monitor already exists
    existing_monitor = get_monitor_info(table_to_monitor)

    if existing_monitor:
        print(f"ℹ️  Monitor already exists for {table_to_monitor}")
        print(f"   Status: {existing_monitor.status}")
        print(f"   Profile Metrics: {existing_monitor.profile_metrics_table_name}")
        print(f"   Dashboard: {existing_monitor.dashboard_id}")

        # Optionally refresh
        refresh = dbutils.widgets.get("refresh_if_exists")
        if refresh and refresh.lower() == "true":
            refresh_monitor(table_to_monitor)
    else:
        # Create new monitor based on type
        output_schema = f"{catalog}.{schema}"

        if monitor_type == "TimeSeries":
            monitor_info = create_timeseries_monitor(
                table_name=table_to_monitor,
                timestamp_col=timestamp_col,
                granularities=[granularity],
                output_schema=output_schema
            )
        elif monitor_type == "Snapshot":
            monitor_info = create_snapshot_monitor(
                table_name=table_to_monitor,
                output_schema=output_schema
            )
        else:
            print(f"⚠️  Monitor type {monitor_type} requires additional configuration")
else:
    print("ℹ️  No table specified. Use the 'table_to_monitor' widget to specify a table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bulk Monitor Setup

# COMMAND ----------

def setup_monitors_for_schema(
    catalog_name: str,
    schema_name: str,
    timestamp_col_map: dict = None,
    exclude_tables: list = None
):
    """
    Set up monitors for all tables in a schema

    Args:
        catalog_name: Catalog name
        schema_name: Schema name
        timestamp_col_map: Dict mapping table names to timestamp columns
        exclude_tables: List of table names to exclude
    """

    # Get all tables in schema
    tables = spark.sql(f"""
        SHOW TABLES IN {catalog_name}.{schema_name}
    """).collect()

    exclude_tables = exclude_tables or []
    results = []

    for table in tables:
        table_name = table.tableName

        if table_name in exclude_tables:
            print(f"⏭️  Skipping {table_name} (excluded)")
            continue

        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

        # Check if already monitored
        if get_monitor_info(full_table_name):
            print(f"ℹ️  {table_name} already has a monitor")
            results.append({"table": table_name, "status": "exists"})
            continue

        # Determine timestamp column
        timestamp_col = timestamp_col_map.get(table_name) if timestamp_col_map else None

        if timestamp_col:
            # Create time series monitor
            monitor_info = create_timeseries_monitor(
                table_name=full_table_name,
                timestamp_col=timestamp_col,
                granularities=["1 day"],
                output_schema=f"{catalog_name}.{schema}"
            )
            results.append({"table": table_name, "status": "created_timeseries"})
        else:
            # Create snapshot monitor
            monitor_info = create_snapshot_monitor(
                table_name=full_table_name,
                output_schema=f"{catalog_name}.{schema}"
            )
            results.append({"table": table_name, "status": "created_snapshot"})

    return results

# Example: Set up monitors for all tables in a schema
# Uncomment and customize as needed
"""
timestamp_columns = {
    "customers": "created_at",
    "orders": "order_date",
    "transactions": "transaction_timestamp"
}

results = setup_monitors_for_schema(
    catalog_name=catalog,
    schema_name="bronze",
    timestamp_col_map=timestamp_columns,
    exclude_tables=["temp_table", "staging_table"]
)

print(f"\n✅ Setup complete for {len(results)} tables")
for result in results:
    print(f"   {result['table']}: {result['status']}")
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Monitoring Metrics

# COMMAND ----------

def get_profile_metrics(table_name: str, limit: int = 100):
    """Get profile metrics for a monitored table"""

    monitor_info = get_monitor_info(table_name)

    if not monitor_info:
        print(f"⚠️  No monitor found for {table_name}")
        return None

    profile_table = monitor_info.profile_metrics_table_name

    metrics_df = spark.sql(f"""
        SELECT *
        FROM {profile_table}
        ORDER BY window.start DESC
        LIMIT {limit}
    """)

    return metrics_df

def get_drift_metrics(table_name: str, limit: int = 100):
    """Get drift metrics for a monitored table"""

    monitor_info = get_monitor_info(table_name)

    if not monitor_info:
        print(f"⚠️  No monitor found for {table_name}")
        return None

    drift_table = monitor_info.drift_metrics_table_name

    drift_df = spark.sql(f"""
        SELECT *
        FROM {drift_table}
        ORDER BY window.start DESC
        LIMIT {limit}
    """)

    return drift_df

# Example: Query metrics for the monitored table
if table_to_monitor:
    print(f"\n📊 Profile Metrics for {table_to_monitor}:")
    profile_metrics = get_profile_metrics(table_to_monitor)

    if profile_metrics:
        display(profile_metrics)

    print(f"\n📊 Drift Metrics for {table_to_monitor}:")
    drift_metrics = get_drift_metrics(table_to_monitor)

    if drift_metrics:
        display(drift_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Analysis Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Statistics Over Time

# COMMAND ----------

if table_to_monitor:
    monitor_info = get_monitor_info(table_to_monitor)

    if monitor_info:
        spark.sql(f"""
        SELECT
          window.start as time_window,
          column_name,
          null_count,
          null_percentage,
          num_distinct,
          min,
          max,
          avg,
          stddev
        FROM {monitor_info.profile_metrics_table_name}
        ORDER BY window.start DESC, column_name
        """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drift Detection Results

# COMMAND ----------

if table_to_monitor:
    monitor_info = get_monitor_info(table_to_monitor)

    if monitor_info:
        spark.sql(f"""
        SELECT
          window.start as time_window,
          column_name,
          drift_type,
          drift_score,
          threshold,
          CASE
            WHEN drift_score > threshold THEN 'DRIFT_DETECTED'
            ELSE 'NO_DRIFT'
          END as drift_status
        FROM {monitor_info.drift_metrics_table_name}
        WHERE drift_score IS NOT NULL
        ORDER BY drift_score DESC, window.start DESC
        """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule Monitor Refreshes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Monitoring Job
# MAGIC
# MAGIC To schedule regular monitor refreshes, create a Databricks job that:
# MAGIC 1. Runs this notebook or calls `lm.run_refresh()` for each monitored table
# MAGIC 2. Schedules based on data freshness requirements (hourly, daily, etc.)
# MAGIC 3. Sends notifications on failures
# MAGIC
# MAGIC Example job configuration:
# MAGIC ```python
# MAGIC {
# MAGIC   "name": "Lakehouse Monitoring Refresh",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "refresh_monitors",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/path/to/this/notebook",
# MAGIC         "base_parameters": {
# MAGIC           "catalog": "main",
# MAGIC           "schema": "observability",
# MAGIC           "table_to_monitor": "main.bronze.customers"
# MAGIC         }
# MAGIC       },
# MAGIC       "existing_cluster_id": "xxx-xxxxxx-xxxxxxx"
# MAGIC     }
# MAGIC   ],
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 * * * ?",
# MAGIC     "timezone_id": "America/Los_Angeles"
# MAGIC   },
# MAGIC   "email_notifications": {
# MAGIC     "on_failure": ["data-team@company.com"]
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## List All Monitors

# COMMAND ----------

def list_all_monitors(catalog_name: str = None, schema_name: str = None):
    """
    List all monitors in the workspace or specific catalog/schema
    """

    query = "SHOW MONITORS"

    if catalog_name and schema_name:
        query = f"SHOW MONITORS IN {catalog_name}.{schema_name}"
    elif catalog_name:
        query = f"SHOW MONITORS IN {catalog_name}"

    try:
        monitors_df = spark.sql(query)
        return monitors_df
    except Exception as e:
        print(f"⚠️  Error listing monitors: {e}")
        return None

# List all monitors
all_monitors = list_all_monitors(catalog)

if all_monitors:
    print(f"📊 Active Monitors:")
    display(all_monitors)

# Databricks notebook source
# MAGIC %md
# MAGIC # Sample DLT Pipeline with Data Quality Expectations
# MAGIC
# MAGIC This notebook demonstrates a complete Delta Live Tables pipeline with:
# MAGIC - Bronze, Silver, and Gold layer transformations
# MAGIC - Comprehensive data quality expectations
# MAGIC - Dynamic rule loading from quality rules table
# MAGIC - Quarantine tables for invalid records
# MAGIC - Data lineage tracking
# MAGIC
# MAGIC **Pipeline Architecture:**
# MAGIC - **Bronze Layer**: Raw data ingestion with basic validation
# MAGIC - **Silver Layer**: Cleaned and transformed data with business rules
# MAGIC - **Gold Layer**: Aggregated data ready for analytics
# MAGIC
# MAGIC **To Deploy:**
# MAGIC 1. Create a DLT pipeline in Databricks UI
# MAGIC 2. Select this notebook as the source
# MAGIC 3. Configure target catalog and schema
# MAGIC 4. Enable Change Data Capture if needed
# MAGIC 5. Run the pipeline

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Pipeline configuration
CATALOG = "main"
SCHEMA = "sample_pipeline"
QUALITY_RULES_TABLE = f"{CATALOG}.observability.data_quality_rules"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def load_quality_rules(dataset_name: str):
    """
    Load quality rules from centralized rules table

    Args:
        dataset_name: Name of the dataset to get rules for

    Returns:
        Dictionary of expectations
    """

    try:
        rules_df = spark.sql(f"""
            SELECT
                expectation_name,
                constraint_expression,
                action
            FROM {QUALITY_RULES_TABLE}
            WHERE dataset_name = '{dataset_name}'
              AND is_enabled = TRUE
        """)

        expectations = {}
        for row in rules_df.collect():
            expectations[row.expectation_name] = {
                "constraint": row.constraint_expression,
                "action": row.action
            }

        return expectations

    except Exception as e:
        print(f"Warning: Could not load quality rules: {e}")
        return {}

def apply_expectations(expectations: dict):
    """
    Apply expectations dynamically based on configuration

    Args:
        expectations: Dictionary of expectation configs

    Returns:
        Decorator function
    """

    def decorator(func):
        # Apply expectations based on action
        for name, config in expectations.items():
            constraint = config["constraint"]
            action = config["action"]

            if action == "fail":
                func = dlt.expect_or_fail(name, constraint)(func)
            elif action == "drop":
                func = dlt.expect_or_drop(name, constraint)(func)
            else:  # warn
                func = dlt.expect(name, constraint)(func)

        return func

    return decorator

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Data Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_customers",
    comment="Raw customer data from source system",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "customer_id"
    }
)
@dlt.expect("valid_record", "customer_id IS NOT NULL")
def bronze_customers():
    """
    Bronze layer: Raw customer data with minimal validation
    """

    # Simulate reading from a source
    # In production, this would read from cloud storage, Kafka, etc.

    return spark.createDataFrame([
        (1, "john.doe@email.com", "John", "Doe", "2024-01-15", "active", 35, "555-1234"),
        (2, "jane.smith@email.com", "Jane", "Smith", "2024-01-16", "active", 28, "555-5678"),
        (3, None, "Invalid", "User", "2024-01-17", "active", 150, "555-9999"),  # Invalid: null email, age > 120
        (4, "bob@email.com", "Bob", "Johnson", "2024-01-18", "inactive", 42, "555-1111"),
        (5, "alice.wong@email.com", "Alice", "Wong", "2024-01-19", "suspended", 31, "555-2222"),
    ], ["customer_id", "email", "first_name", "last_name", "created_date", "status", "age", "phone_number"])

@dlt.table(
    name="bronze_orders",
    comment="Raw order data from source system",
    table_properties={"quality": "bronze"}
)
@dlt.expect("valid_order", "order_id IS NOT NULL")
def bronze_orders():
    """
    Bronze layer: Raw order data
    """

    return spark.createDataFrame([
        (101, 1, "2024-01-20", 150.00, "completed", "2024-01-20"),
        (102, 2, "2024-01-21", 225.50, "completed", "2024-01-21"),
        (103, 1, "2024-01-22", 89.99, "pending", None),
        (104, 999, "2024-01-23", 500.00, "completed", "2024-01-23"),  # Invalid: customer doesn't exist
        (105, 4, "2024-01-24", -50.00, "completed", "2024-01-24"),  # Invalid: negative amount
    ], ["order_id", "customer_id", "order_date", "order_amount", "order_status", "shipped_date"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned and Validated Data

# COMMAND ----------

@dlt.table(
    name="silver_customers",
    comment="Cleaned and validated customer data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "customer_id"
    }
)
# Static expectations
@dlt.expect_or_fail("customer_id_not_null", "customer_id IS NOT NULL")
@dlt.expect_or_drop("email_not_null", "email IS NOT NULL")
@dlt.expect_or_drop("valid_email_format", "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$'")
@dlt.expect_or_drop("valid_status", "status IN ('active', 'inactive', 'suspended')")
@dlt.expect_or_drop("valid_age_range", "age >= 18 AND age <= 120")
@dlt.expect("valid_phone_length", "LENGTH(phone_number) >= 10")
def silver_customers():
    """
    Silver layer: Validated and cleaned customer data

    Data Quality Rules:
    - customer_id must not be null (fail pipeline if violated)
    - email must not be null (drop invalid records)
    - email must match valid format (drop invalid records)
    - status must be one of: active, inactive, suspended (drop invalid)
    - age must be between 18 and 120 (drop invalid)
    - phone number should have at least 10 digits (warn only)
    """

    return (
        dlt.read("bronze_customers")
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        .withColumn("created_date", F.to_date(F.col("created_date")))
        .withColumn("email_domain", F.split(F.col("email"), "@").getItem(1))
        .withColumn("processing_timestamp", F.current_timestamp())
    )

@dlt.table(
    name="silver_customers_quarantine",
    comment="Quarantined customer records that failed validation"
)
def silver_customers_quarantine():
    """
    Quarantine table for customers that failed validation

    This allows data engineers to review and correct invalid records
    """

    return (
        dlt.read("bronze_customers")
        .filter(
            F.col("email").isNull() |
            ~F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$") |
            ~F.col("status").isin("active", "inactive", "suspended") |
            (F.col("age") < 18) |
            (F.col("age") > 120)
        )
        .withColumn("quarantine_reason", F.lit("Failed silver layer validation"))
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )

@dlt.table(
    name="silver_orders",
    comment="Cleaned and validated order data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_fail("customer_id_not_null", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_amount", "order_amount > 0")
@dlt.expect_or_drop("valid_order_status", "order_status IN ('pending', 'completed', 'cancelled', 'refunded')")
@dlt.expect("shipped_date_after_order", "shipped_date IS NULL OR shipped_date >= order_date")
# Referential integrity check
@dlt.expect_or_drop(
    "customer_exists",
    "customer_id IN (SELECT customer_id FROM LIVE.silver_customers)"
)
def silver_orders():
    """
    Silver layer: Validated order data with referential integrity

    Data Quality Rules:
    - order_id and customer_id must not be null (fail pipeline)
    - order_amount must be positive (drop invalid)
    - order_status must be valid value (drop invalid)
    - shipped_date should be after order_date (warn only)
    - customer_id must exist in silver_customers (drop invalid - referential integrity)
    """

    return (
        dlt.read("bronze_orders")
        .withColumn("order_date", F.to_date(F.col("order_date")))
        .withColumn("shipped_date", F.to_date(F.col("shipped_date")))
        .withColumn("days_to_ship",
                   F.when(F.col("shipped_date").isNotNull(),
                         F.datediff(F.col("shipped_date"), F.col("order_date")))
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Aggregated Analytics Tables

# COMMAND ----------

@dlt.table(
    name="gold_customer_metrics",
    comment="Customer-level metrics and aggregations",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "customer_id"
    }
)
@dlt.expect("has_orders", "total_orders > 0")
@dlt.expect("positive_revenue", "total_revenue > 0")
def gold_customer_metrics():
    """
    Gold layer: Customer-level aggregations

    Metrics:
    - Total orders per customer
    - Total revenue per customer
    - Average order value
    - Most recent order date
    - Customer lifetime value indicators
    """

    customers = dlt.read("silver_customers")
    orders = dlt.read("silver_orders")

    return (
        orders
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct(F.year("order_date")).alias("active_years")
        )
        .join(
            customers.select("customer_id", "email", "full_name", "status", "created_date", "email_domain"),
            "customer_id",
            "inner"
        )
        .withColumn("days_since_first_order",
                   F.datediff(F.current_date(), F.col("first_order_date")))
        .withColumn("days_since_last_order",
                   F.datediff(F.current_date(), F.col("last_order_date")))
        .withColumn("customer_segment",
            F.when(F.col("total_revenue") >= 1000, "VIP")
            .when(F.col("total_revenue") >= 500, "High Value")
            .when(F.col("total_revenue") >= 100, "Regular")
            .otherwise("Low Value")
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )

@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregations",
    table_properties={"quality": "gold"}
)
@dlt.expect("valid_date", "order_date IS NOT NULL")
@dlt.expect("positive_revenue", "daily_revenue > 0")
def gold_daily_revenue():
    """
    Gold layer: Daily revenue metrics

    Metrics:
    - Daily revenue totals
    - Order counts
    - Average order value
    - Unique customers
    """

    return (
        dlt.read("silver_orders")
        .filter(F.col("order_status") == "completed")
        .groupBy(F.col("order_date"))
        .agg(
            F.sum("order_amount").alias("daily_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        .withColumn("processing_timestamp", F.current_timestamp())
        .orderBy("order_date")
    )

@dlt.table(
    name="gold_customer_status_summary",
    comment="Customer status distribution summary",
    table_properties={"quality": "gold"}
)
def gold_customer_status_summary():
    """
    Gold layer: Summary of customers by status

    Provides quick insights into customer base health
    """

    return (
        dlt.read("silver_customers")
        .groupBy("status")
        .agg(
            F.count("customer_id").alias("customer_count"),
            F.avg("age").alias("avg_age"),
            F.collect_set("email_domain").alias("email_domains")
        )
        .withColumn("percentage",
            F.round(
                F.col("customer_count") * 100.0 / F.sum("customer_count").over(Window.partitionBy()),
                2
            )
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Monitoring Views

# COMMAND ----------

@dlt.view(
    name="data_quality_summary",
    comment="Real-time data quality metrics for all tables"
)
def data_quality_summary():
    """
    View providing data quality metrics across all pipeline tables

    This view can be used for monitoring dashboards
    """

    # This would aggregate DLT expectations from event logs
    # For demonstration, we'll create a placeholder structure

    return spark.createDataFrame([
        ("bronze_customers", "bronze", 5, 0, 100.0),
        ("silver_customers", "silver", 4, 1, 80.0),
        ("silver_orders", "silver", 4, 1, 80.0),
        ("gold_customer_metrics", "gold", 3, 0, 100.0),
    ], ["table_name", "layer", "records_passed", "records_failed", "quality_score"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration Notes

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Pipeline Settings (Configure in UI)
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "id": "sample-observability-pipeline",
# MAGIC   "name": "Sample DLT Pipeline with Quality Monitoring",
# MAGIC   "storage": "/mnt/dlt/sample_pipeline",
# MAGIC   "target": "main.sample_pipeline",
# MAGIC   "configuration": {
# MAGIC     "catalog": "main",
# MAGIC     "schema": "sample_pipeline"
# MAGIC   },
# MAGIC   "clusters": [
# MAGIC     {
# MAGIC       "label": "default",
# MAGIC       "autoscale": {
# MAGIC         "min_workers": 1,
# MAGIC         "max_workers": 5
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "development": false,
# MAGIC   "continuous": false,
# MAGIC   "channel": "CURRENT",
# MAGIC   "photon": true,
# MAGIC   "libraries": [],
# MAGIC   "notifications": [
# MAGIC     {
# MAGIC       "email_recipients": ["data-team@company.com"],
# MAGIC       "alerts": ["on-update-failure", "on-update-success", "on-flow-failure"]
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Key Features Enabled:
# MAGIC - **Photon**: Enabled for faster query execution
# MAGIC - **AutoLoader**: For incremental data ingestion (if reading from storage)
# MAGIC - **Change Data Capture**: Can be enabled on tables with `TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`
# MAGIC - **Expectations**: Comprehensive data quality rules at each layer
# MAGIC - **Quarantine Tables**: Invalid records routed to separate tables for review
# MAGIC - **Lineage**: Automatic tracking through Unity Catalog
# MAGIC
# MAGIC ### Monitoring:
# MAGIC - Event logs automatically captured by DLT
# MAGIC - Use `notebooks/monitoring/dlt_event_log_monitoring.py` to analyze
# MAGIC - Quality metrics available through DLT UI and event logs
# MAGIC - Set up alerts using `notebooks/monitoring/alerting_configuration.py`

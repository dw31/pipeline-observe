# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Rules Manager
# MAGIC
# MAGIC This notebook provides a centralized system for managing data quality rules (expectations)
# MAGIC that can be dynamically loaded and applied to DLT pipelines.
# MAGIC
# MAGIC **Features:**
# MAGIC - Define expectations in a Delta table
# MAGIC - Support multiple datasets and pipelines
# MAGIC - Version control for quality rules
# MAGIC - Enable/disable rules without code changes
# MAGIC - Rule templates for common validations
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Unity Catalog enabled
# MAGIC - Permissions to create tables and manage expectations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Configuration parameters
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "observability", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Quality rules table
quality_rules_table = f"{catalog}.{schema}.data_quality_rules"

print(f"Quality Rules Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Rules Table: {quality_rules_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Quality Rules Table

# COMMAND ----------

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Create quality rules table with schema
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {quality_rules_table} (
  rule_id STRING NOT NULL,
  pipeline_name STRING,
  dataset_name STRING NOT NULL,
  expectation_name STRING NOT NULL,
  expectation_type STRING NOT NULL,
  constraint_expression STRING NOT NULL,
  action STRING NOT NULL,
  description STRING,
  severity STRING,
  is_enabled BOOLEAN DEFAULT TRUE,
  created_by STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  tags MAP<STRING, STRING>,
  CONSTRAINT pk_rule_id PRIMARY KEY (rule_id)
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print(f"✅ Quality rules table created: {quality_rules_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule Templates and Helper Functions

# COMMAND ----------

import uuid

def create_rule(
    dataset_name: str,
    expectation_name: str,
    expectation_type: str,
    constraint_expression: str,
    action: str = "warn",
    description: str = None,
    severity: str = "medium",
    pipeline_name: str = None,
    tags: dict = None
):
    """
    Create a data quality rule

    Args:
        dataset_name: Name of the dataset/table to apply rule to
        expectation_name: Unique name for the expectation
        expectation_type: Type of validation (not_null, range, regex, custom, etc.)
        constraint_expression: SQL expression for the validation
        action: Action on failure (warn, drop, fail)
        description: Human-readable description
        severity: Severity level (low, medium, high, critical)
        pipeline_name: Optional pipeline name to scope the rule
        tags: Optional tags for categorization
    """

    rule = {
        "rule_id": str(uuid.uuid4()),
        "pipeline_name": pipeline_name,
        "dataset_name": dataset_name,
        "expectation_name": expectation_name,
        "expectation_type": expectation_type,
        "constraint_expression": constraint_expression,
        "action": action,
        "description": description or f"{expectation_type} validation for {dataset_name}",
        "severity": severity,
        "is_enabled": True,
        "created_by": spark.sql("SELECT current_user()").collect()[0][0],
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "tags": tags or {}
    }

    return rule

# Common rule templates
class RuleTemplates:
    """Predefined templates for common data quality rules"""

    @staticmethod
    def not_null(dataset_name: str, column_name: str, action: str = "warn"):
        """Rule to check column is not null"""
        return create_rule(
            dataset_name=dataset_name,
            expectation_name=f"{column_name}_not_null",
            expectation_type="not_null",
            constraint_expression=f"{column_name} IS NOT NULL",
            action=action,
            description=f"Ensure {column_name} has no null values",
            severity="high"
        )

    @staticmethod
    def valid_values(dataset_name: str, column_name: str, valid_values: list, action: str = "drop"):
        """Rule to check column values are in allowed set"""
        values_str = ", ".join([f"'{v}'" for v in valid_values])
        return create_rule(
            dataset_name=dataset_name,
            expectation_name=f"{column_name}_valid_values",
            expectation_type="valid_values",
            constraint_expression=f"{column_name} IN ({values_str})",
            action=action,
            description=f"Ensure {column_name} is one of: {', '.join(valid_values)}",
            severity="high"
        )

    @staticmethod
    def numeric_range(dataset_name: str, column_name: str, min_value: float = None, max_value: float = None, action: str = "warn"):
        """Rule to check numeric column is within range"""
        conditions = []
        if min_value is not None:
            conditions.append(f"{column_name} >= {min_value}")
        if max_value is not None:
            conditions.append(f"{column_name} <= {max_value}")

        constraint = " AND ".join(conditions)
        range_desc = f"between {min_value} and {max_value}" if min_value and max_value else \
                     f">= {min_value}" if min_value else f"<= {max_value}"

        return create_rule(
            dataset_name=dataset_name,
            expectation_name=f"{column_name}_range",
            expectation_type="range",
            constraint_expression=constraint,
            action=action,
            description=f"Ensure {column_name} is {range_desc}",
            severity="medium"
        )

    @staticmethod
    def unique_values(dataset_name: str, column_name: str, action: str = "fail"):
        """Rule to check column values are unique"""
        # Note: Uniqueness checks in DLT require aggregation approach
        return create_rule(
            dataset_name=dataset_name,
            expectation_name=f"{column_name}_unique",
            expectation_type="unique",
            constraint_expression=f"COUNT(*) OVER (PARTITION BY {column_name}) = 1",
            action=action,
            description=f"Ensure {column_name} contains unique values",
            severity="critical"
        )

    @staticmethod
    def regex_match(dataset_name: str, column_name: str, pattern: str, action: str = "drop"):
        """Rule to check column matches regex pattern"""
        return create_rule(
            dataset_name=dataset_name,
            expectation_name=f"{column_name}_format",
            expectation_type="regex",
            constraint_expression=f"{column_name} RLIKE '{pattern}'",
            action=action,
            description=f"Ensure {column_name} matches pattern: {pattern}",
            severity="medium"
        )

    @staticmethod
    def date_freshness(dataset_name: str, column_name: str, max_age_days: int = 1, action: str = "warn"):
        """Rule to check data freshness"""
        return create_rule(
            dataset_name=dataset_name,
            expectation_name=f"{column_name}_freshness",
            expectation_type="freshness",
            constraint_expression=f"{column_name} >= current_date() - INTERVAL {max_age_days} DAYS",
            action=action,
            description=f"Ensure {column_name} is within {max_age_days} days",
            severity="high"
        )

    @staticmethod
    def referential_integrity(dataset_name: str, foreign_key: str, reference_table: str, reference_key: str, action: str = "drop"):
        """Rule to check referential integrity"""
        return create_rule(
            dataset_name=dataset_name,
            expectation_name=f"{foreign_key}_fk_check",
            expectation_type="foreign_key",
            constraint_expression=f"{foreign_key} IN (SELECT {reference_key} FROM {reference_table})",
            action=action,
            description=f"Ensure {foreign_key} exists in {reference_table}.{reference_key}",
            severity="critical"
        )

print("✅ Rule templates defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Sample Rules

# COMMAND ----------

# Example: Create sample rules for a customer dataset
sample_rules = [
    # Not null checks
    RuleTemplates.not_null("customers", "customer_id", action="fail"),
    RuleTemplates.not_null("customers", "email", action="warn"),
    RuleTemplates.not_null("customers", "created_date", action="warn"),

    # Value validation
    RuleTemplates.valid_values(
        "customers",
        "status",
        ["active", "inactive", "suspended"],
        action="drop"
    ),

    # Range checks
    RuleTemplates.numeric_range(
        "customers",
        "age",
        min_value=18,
        max_value=120,
        action="drop"
    ),

    # Format validation
    RuleTemplates.regex_match(
        "customers",
        "email",
        "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
        action="drop"
    ),

    # Freshness check
    RuleTemplates.date_freshness(
        "customers",
        "last_updated_date",
        max_age_days=7,
        action="warn"
    ),

    # Custom validation
    create_rule(
        dataset_name="customers",
        expectation_name="valid_phone_length",
        expectation_type="custom",
        constraint_expression="LENGTH(phone_number) >= 10",
        action="drop",
        description="Ensure phone number has at least 10 digits",
        severity="medium",
        tags={"category": "format", "pii": "true"}
    )
]

# Convert to DataFrame and insert
rules_df = spark.createDataFrame(sample_rules)

# Merge rules (upsert based on rule_id)
rules_df.write.mode("append").saveAsTable(quality_rules_table)

print(f"✅ Added {len(sample_rules)} sample rules to {quality_rules_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Current Rules

# COMMAND ----------

# Display all active rules
spark.sql(f"""
SELECT
  rule_id,
  pipeline_name,
  dataset_name,
  expectation_name,
  expectation_type,
  constraint_expression,
  action,
  severity,
  is_enabled,
  description
FROM {quality_rules_table}
WHERE is_enabled = TRUE
ORDER BY dataset_name, severity DESC, expectation_name
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule Management Functions

# COMMAND ----------

def get_rules_for_dataset(dataset_name: str, pipeline_name: str = None):
    """
    Retrieve all active rules for a specific dataset

    Args:
        dataset_name: Name of the dataset
        pipeline_name: Optional pipeline name filter

    Returns:
        DataFrame with rules
    """
    query = f"""
    SELECT
      expectation_name,
      expectation_type,
      constraint_expression,
      action,
      description,
      severity
    FROM {quality_rules_table}
    WHERE dataset_name = '{dataset_name}'
      AND is_enabled = TRUE
    """

    if pipeline_name:
        query += f" AND (pipeline_name = '{pipeline_name}' OR pipeline_name IS NULL)"

    query += " ORDER BY severity DESC"

    return spark.sql(query)

def enable_rule(rule_id: str):
    """Enable a specific rule"""
    spark.sql(f"""
    UPDATE {quality_rules_table}
    SET is_enabled = TRUE, updated_at = CURRENT_TIMESTAMP()
    WHERE rule_id = '{rule_id}'
    """)
    print(f"✅ Rule {rule_id} enabled")

def disable_rule(rule_id: str):
    """Disable a specific rule"""
    spark.sql(f"""
    UPDATE {quality_rules_table}
    SET is_enabled = FALSE, updated_at = CURRENT_TIMESTAMP()
    WHERE rule_id = '{rule_id}'
    """)
    print(f"✅ Rule {rule_id} disabled")

def delete_rule(rule_id: str):
    """Delete a specific rule"""
    spark.sql(f"""
    DELETE FROM {quality_rules_table}
    WHERE rule_id = '{rule_id}'
    """)
    print(f"✅ Rule {rule_id} deleted")

def bulk_disable_rules(dataset_name: str = None, severity: str = None):
    """Disable multiple rules by criteria"""
    conditions = ["is_enabled = TRUE"]

    if dataset_name:
        conditions.append(f"dataset_name = '{dataset_name}'")
    if severity:
        conditions.append(f"severity = '{severity}'")

    where_clause = " AND ".join(conditions)

    spark.sql(f"""
    UPDATE {quality_rules_table}
    SET is_enabled = FALSE, updated_at = CURRENT_TIMESTAMP()
    WHERE {where_clause}
    """)
    print(f"✅ Rules disabled matching criteria: {where_clause}")

print("✅ Rule management functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Rules for DLT Pipeline

# COMMAND ----------

def generate_dlt_expectations(dataset_name: str, pipeline_name: str = None):
    """
    Generate DLT expectation code from rules

    Args:
        dataset_name: Name of the dataset
        pipeline_name: Optional pipeline name

    Returns:
        Dictionary of expectations ready for DLT
    """
    rules_df = get_rules_for_dataset(dataset_name, pipeline_name)
    rules = rules_df.collect()

    expectations = {}

    for rule in rules:
        exp_name = rule.expectation_name
        constraint = rule.constraint_expression
        action = rule.action

        # Map action to DLT decorator
        if action == "fail":
            expectations[exp_name] = {"constraint": constraint, "action": "expect_or_fail"}
        elif action == "drop":
            expectations[exp_name] = {"constraint": constraint, "action": "expect_or_drop"}
        else:  # warn
            expectations[exp_name] = {"constraint": constraint, "action": "expect"}

    return expectations

# Example: Generate expectations for customers dataset
customer_expectations = generate_dlt_expectations("customers")

print("Generated DLT Expectations:")
for name, config in customer_expectations.items():
    print(f"  @dlt.{config['action']}(\"{name}\", \"{config['constraint']}\")")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics on Rules

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rules by Dataset

# COMMAND ----------

spark.sql(f"""
SELECT
  dataset_name,
  COUNT(*) as total_rules,
  SUM(CASE WHEN is_enabled THEN 1 ELSE 0 END) as active_rules,
  SUM(CASE WHEN is_enabled = FALSE THEN 1 ELSE 0 END) as disabled_rules,
  COLLECT_LIST(DISTINCT expectation_type) as rule_types,
  COLLECT_LIST(DISTINCT severity) as severity_levels
FROM {quality_rules_table}
GROUP BY dataset_name
ORDER BY total_rules DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rules by Severity

# COMMAND ----------

spark.sql(f"""
SELECT
  severity,
  COUNT(*) as rule_count,
  SUM(CASE WHEN is_enabled THEN 1 ELSE 0 END) as active_count,
  COLLECT_LIST(DISTINCT action) as actions_used
FROM {quality_rules_table}
GROUP BY severity
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    WHEN 'low' THEN 4
  END
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recently Modified Rules

# COMMAND ----------

spark.sql(f"""
SELECT
  rule_id,
  dataset_name,
  expectation_name,
  is_enabled,
  updated_at,
  created_at,
  DATEDIFF(updated_at, created_at) as days_since_creation
FROM {quality_rules_table}
ORDER BY updated_at DESC
LIMIT 20
""").display()

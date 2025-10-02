# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Loader
# MAGIC
# MAGIC This notebook provides utilities to load and access the global observability configuration.
# MAGIC
# MAGIC **Usage in other notebooks:**
# MAGIC ```python
# MAGIC %run ./configs/config_loader
# MAGIC
# MAGIC # Load configuration
# MAGIC config = load_config()
# MAGIC
# MAGIC # Access parameters
# MAGIC catalog = config.catalog
# MAGIC schema = config.schema
# MAGIC event_log_table = config.get_table_name('event_logs')
# MAGIC
# MAGIC # Get fully qualified table name
# MAGIC full_table_name = config.get_full_table_name('quality_metrics')
# MAGIC # Returns: 'main.observability.dlt_quality_metrics'
# MAGIC ```

# COMMAND ----------

import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path

# COMMAND ----------

class ObservabilityConfig:
    """
    Configuration class for Databricks Pipeline Observability platform

    Provides centralized access to all configuration parameters with
    helper methods for common operations.
    """

    def __init__(self, config_dict: Dict[str, Any], environment: str = "default"):
        """
        Initialize configuration

        Args:
            config_dict: Dictionary containing configuration
            environment: Environment name (default, dev, prod)
        """
        self._config = config_dict
        self._environment = environment

        # Set environment-specific overrides if they exist
        if environment in config_dict and environment != "default":
            env_config = config_dict[environment]
            self.catalog = env_config.get('catalog', config_dict.get('catalog', 'main'))
            self.schema = env_config.get('schema', config_dict.get('schema', 'observability'))
        else:
            self.catalog = config_dict.get('catalog', 'main')
            self.schema = config_dict.get('schema', 'observability')

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key path (supports nested keys with dot notation)"""
        keys = key.split('.')
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value

    def get_table_name(self, table_key: str) -> str:
        """
        Get table name from configuration

        Args:
            table_key: Key in the tables section (e.g., 'event_logs', 'quality_metrics')

        Returns:
            Table name without catalog/schema prefix
        """
        return self.get(f'tables.{table_key}', table_key)

    def get_full_table_name(self, table_key: str) -> str:
        """
        Get fully qualified table name

        Args:
            table_key: Key in the tables section

        Returns:
            Fully qualified table name: catalog.schema.table
        """
        table_name = self.get_table_name(table_key)
        return f"{self.catalog}.{self.schema}.{table_name}"

    def get_system_table(self, table_key: str) -> str:
        """
        Get system table name

        Args:
            table_key: Key in system_tables section (e.g., 'billing', 'jobs')

        Returns:
            Fully qualified system table name
        """
        return self.get(f'system_tables.{table_key}', f'system.{table_key}')

    def get_alert_threshold(self, category: str, metric: str) -> Any:
        """
        Get alert threshold value

        Args:
            category: Alert category (cost, job_failure, data_quality, etc.)
            metric: Metric name within category

        Returns:
            Threshold value
        """
        return self.get(f'alerting.thresholds.{category}.{metric}')

    def is_feature_enabled(self, feature_name: str) -> bool:
        """
        Check if a feature flag is enabled

        Args:
            feature_name: Feature flag name

        Returns:
            True if enabled, False otherwise
        """
        return self.get(f'features.{feature_name}', False)

    def get_table_properties(self) -> Dict[str, str]:
        """Get default table properties for Delta tables"""
        props = self.get('storage.table_properties', {})

        # Convert boolean values to strings for Delta table properties
        return {
            k: str(v).lower() if isinstance(v, bool) else str(v)
            for k, v in props.items()
        }

    def get_partition_columns(self, table_key: str) -> list:
        """
        Get partition columns for a table

        Args:
            table_key: Table key

        Returns:
            List of partition column expressions
        """
        return self.get(f'optimization.partitioning.{table_key}', [])

    def get_z_order_columns(self, table_key: str) -> list:
        """
        Get Z-order columns for a table

        Args:
            table_key: Table key

        Returns:
            List of Z-order columns
        """
        return self.get(f'optimization.auto_optimize.z_order_columns.{table_key}', [])

    def get_notification_channels(self) -> Dict[str, Dict]:
        """Get enabled notification channels"""
        channels = self.get('alerting.channels', {})
        return {
            name: config
            for name, config in channels.items()
            if config.get('enabled', False)
        }

    def __repr__(self):
        return f"ObservabilityConfig(catalog={self.catalog}, schema={self.schema}, env={self._environment})"

# COMMAND ----------

def load_config(config_path: Optional[str] = None, environment: str = "default") -> ObservabilityConfig:
    """
    Load observability configuration from YAML file

    Args:
        config_path: Path to config file (if None, uses default location)
        environment: Environment name (default, dev, prod)

    Returns:
        ObservabilityConfig instance
    """
    if config_path is None:
        # Try to determine the workspace path
        try:
            # In Databricks, get the notebook context
            notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            workspace_root = str(Path(notebook_path).parent.parent)
            config_path = f"{workspace_root}/configs/observability_config.yaml"
        except:
            # Fallback to relative path
            config_path = "./configs/observability_config.yaml"

    try:
        # Read YAML file
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)

        return ObservabilityConfig(config_dict, environment)

    except FileNotFoundError:
        print(f"⚠️  Config file not found at {config_path}")
        print("Using default configuration values")

        # Return default configuration
        return ObservabilityConfig({
            'catalog': 'main',
            'schema': 'observability',
            'tables': {},
            'system_tables': {}
        }, environment)

    except yaml.YAMLError as e:
        print(f"❌ Error parsing YAML config: {e}")
        raise

# COMMAND ----------

def create_widgets_from_config(config: ObservabilityConfig):
    """
    Create Databricks notebook widgets from configuration

    This is useful for notebooks that need to override config values

    Args:
        config: ObservabilityConfig instance
    """
    try:
        dbutils.widgets.text("catalog", config.catalog, "Catalog Name")
        dbutils.widgets.text("schema", config.schema, "Schema Name")
        dbutils.widgets.dropdown(
            "environment",
            "default",
            ["default", "dev", "prod"],
            "Environment"
        )

        print("✅ Widgets created from configuration")
    except NameError:
        print("⚠️  dbutils not available - skipping widget creation")

# COMMAND ----------

def get_config_from_widgets() -> ObservabilityConfig:
    """
    Load configuration and override with widget values

    Returns:
        ObservabilityConfig with widget overrides applied
    """
    try:
        environment = dbutils.widgets.get("environment")
    except:
        environment = "default"

    config = load_config(environment=environment)

    # Override with widget values if they exist
    try:
        catalog_override = dbutils.widgets.get("catalog")
        schema_override = dbutils.widgets.get("schema")

        if catalog_override:
            config.catalog = catalog_override
        if schema_override:
            config.schema = schema_override

    except:
        pass  # Widgets don't exist, use config defaults

    return config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions for Common Patterns

# COMMAND ----------

def create_schema_if_not_exists(config: ObservabilityConfig):
    """
    Create catalog and schema if they don't exist

    Args:
        config: ObservabilityConfig instance
    """
    print(f"Creating schema {config.catalog}.{config.schema} if not exists...")

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema}")

    print(f"✅ Schema {config.catalog}.{config.schema} ready")

# COMMAND ----------

def get_table_ddl(config: ObservabilityConfig, table_key: str) -> str:
    """
    Generate CREATE TABLE DDL with configuration-driven properties

    Args:
        config: ObservabilityConfig instance
        table_key: Table key from config

    Returns:
        DDL statement string
    """
    full_table_name = config.get_full_table_name(table_key)
    partition_cols = config.get_partition_columns(table_key)
    table_props = config.get_table_properties()

    ddl_parts = [f"CREATE TABLE IF NOT EXISTS {full_table_name}"]

    # Add schema definition (would need to be customized per table)
    ddl_parts.append("(")
    ddl_parts.append("  -- Define columns here")
    ddl_parts.append(")")

    ddl_parts.append("USING DELTA")

    # Add partitioning
    if partition_cols:
        partition_clause = ", ".join(partition_cols)
        ddl_parts.append(f"PARTITIONED BY ({partition_clause})")

    # Add table properties
    if table_props:
        props_str = ", ".join([f"'{k}' = '{v}'" for k, v in table_props.items()])
        ddl_parts.append(f"TBLPROPERTIES ({props_str})")

    return "\n".join(ddl_parts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage

# COMMAND ----------

if __name__ == "__main__":
    # Example: Load configuration
    config = load_config()

    print("Configuration loaded:")
    print(f"  Catalog: {config.catalog}")
    print(f"  Schema: {config.schema}")
    print(f"  Event logs table: {config.get_full_table_name('event_logs')}")
    print(f"  Quality metrics table: {config.get_full_table_name('quality_metrics')}")
    print()

    # Example: Check feature flags
    print("Feature flags:")
    print(f"  Streaming metrics enabled: {config.is_feature_enabled('enable_streaming_metrics')}")
    print(f"  ML monitoring enabled: {config.is_feature_enabled('enable_ml_monitoring')}")
    print()

    # Example: Get alert thresholds
    print("Alert thresholds:")
    cost_threshold = config.get_alert_threshold('cost', 'daily_increase_percent')
    print(f"  Cost daily increase: {cost_threshold}%")
    quality_threshold = config.get_alert_threshold('data_quality', 'quality_score_threshold')
    print(f"  Quality score threshold: {quality_threshold}")
    print()

    # Example: Get notification channels
    print("Enabled notification channels:")
    channels = config.get_notification_channels()
    for channel_name in channels.keys():
        print(f"  - {channel_name}")

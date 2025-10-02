# Configuration Guide

This directory contains centralized configuration for the Databricks Pipeline Observability platform.

## Files

- **`observability_config.yaml`** - Main configuration file with all global parameters
- **`config_loader.py`** - Python/notebook utility for loading and accessing configuration
- **`quality_rules/`** - Directory for quality rule configuration examples

## Quick Start

### Using Configuration in Notebooks

```python
# Load the config loader
%run ./configs/config_loader

# Load configuration
config = load_config()

# Access basic parameters
catalog = config.catalog  # 'main'
schema = config.schema    # 'observability'

# Get fully qualified table names
event_log_table = config.get_full_table_name('event_logs')
# Returns: 'main.observability.dlt_event_logs'

quality_metrics_table = config.get_full_table_name('quality_metrics')
# Returns: 'main.observability.dlt_quality_metrics'

# Get system table names
billing_table = config.get_system_table('billing')
# Returns: 'system.billing.usage'
```

### Using Configuration in DLT Pipelines

```python
# At the top of your DLT pipeline notebook
%run ./configs/config_loader

config = load_config()

# Use config for pipeline settings
CATALOG = config.catalog
SCHEMA = config.schema
QUALITY_RULES_TABLE = config.get_full_table_name('quality_rules')

# Rest of your DLT pipeline code
import dlt

@dlt.table(
    name="my_table",
    table_properties=config.get_table_properties()
)
def my_table():
    return spark.read.table("source_table")
```

### Creating Widgets from Config

```python
%run ./configs/config_loader

config = load_config()

# Create widgets with config defaults
create_widgets_from_config(config)

# Or get config with widget overrides
config = get_config_from_widgets()
```

## Configuration Sections

### Unity Catalog Settings
```yaml
catalog: main
schema: observability
```

Controls where all observability tables are created.

### Table Names
```yaml
tables:
  event_logs: dlt_event_logs
  quality_metrics: dlt_quality_metrics
  pipeline_health: dlt_pipeline_health
  quality_rules: data_quality_rules
  # ... more tables
```

Centralized table naming. Access via `config.get_table_name('event_logs')` or `config.get_full_table_name('event_logs')`.

### Alert Thresholds
```yaml
alerting:
  thresholds:
    cost:
      daily_increase_percent: 20
      absolute_threshold_usd: 1000
    data_quality:
      quality_score_threshold: 0.95
      pass_rate_threshold: 0.90
    # ... more thresholds
```

Access via `config.get_alert_threshold('cost', 'daily_increase_percent')`.

### Feature Flags
```yaml
features:
  enable_ml_monitoring: false
  enable_streaming_metrics: true
  enable_cost_optimization: true
  # ... more features
```

Check with `config.is_feature_enabled('enable_streaming_metrics')`.

### Table Properties
```yaml
storage:
  table_properties:
    enable_change_data_feed: true
    auto_optimize_write: true
    auto_compact: true
```

Get all properties with `config.get_table_properties()`.

### Partitioning and Optimization
```yaml
optimization:
  partitioning:
    event_logs: ["DATE(timestamp)"]
    quality_metrics: ["DATE(timestamp)"]

  auto_optimize:
    z_order_columns:
      event_logs: ["pipeline_id", "timestamp"]
      quality_metrics: ["dataset_name", "timestamp"]
```

Access via `config.get_partition_columns('event_logs')` and `config.get_z_order_columns('event_logs')`.

## Environment-Specific Configuration

You can define environment-specific overrides:

```yaml
# Default configuration
catalog: main
schema: observability

# Development environment override
dev:
  catalog: dev
  schema: observability

# Production environment override
prod:
  catalog: prod
  schema: observability
```

Load environment-specific config:

```python
# Load dev config
config = load_config(environment='dev')

# Or use widgets
dbutils.widgets.dropdown("environment", "default", ["default", "dev", "prod"])
config = get_config_from_widgets()
```

## Helper Functions

### `create_schema_if_not_exists(config)`
Creates the catalog and schema if they don't exist.

```python
config = load_config()
create_schema_if_not_exists(config)
```

### `config.get(key, default)`
Get any config value using dot notation:

```python
# Get nested value
lookback_days = config.get('monitoring.default_lookback_days', 7)

# Get alert channel config
slack_enabled = config.get('alerting.channels.slack.enabled', False)
```

### `config.get_notification_channels()`
Get all enabled notification channels:

```python
channels = config.get_notification_channels()
for channel_name, channel_config in channels.items():
    print(f"Channel: {channel_name}")
    print(f"  Webhook: {channel_config.get('webhook_url')}")
```

## Updating Configuration

### Modifying `observability_config.yaml`

1. Edit the YAML file with your preferred values
2. Commit changes to version control
3. No need to restart notebooks - config is loaded at runtime

### Best Practices

1. **Don't hardcode credentials** - Use Databricks secrets instead:
   ```python
   slack_webhook = dbutils.secrets.get(scope="observability", key="slack-webhook")
   ```

2. **Use environment-specific configs** for dev/prod separation:
   ```yaml
   dev:
     catalog: dev
     schema: observability
   prod:
     catalog: prod
     schema: observability
   ```

3. **Override with widgets** when you need notebook-specific values:
   ```python
   config = get_config_from_widgets()
   ```

4. **Version control** - Always commit config changes with descriptive messages

## Common Patterns

### Pattern 1: Monitoring Notebook Setup

```python
%run ./configs/config_loader

# Load config
config = load_config()

# Create widgets for overrides
dbutils.widgets.text("catalog", config.catalog, "Catalog Name")
dbutils.widgets.text("schema", config.schema, "Schema Name")
dbutils.widgets.text("days_back", str(config.get('monitoring.default_lookback_days', 7)), "Days Back")

# Get final config with widget overrides
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
days_back = int(dbutils.widgets.get("days_back"))

# Use in queries
event_log_table = f"{catalog}.{schema}.{config.get_table_name('event_logs')}"
```

### Pattern 2: DLT Pipeline with Config

```python
%run ./configs/config_loader

config = load_config()

CATALOG = config.catalog
SCHEMA = config.schema
QUALITY_RULES_TABLE = config.get_full_table_name('quality_rules')

import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="bronze_data",
    table_properties=config.get_table_properties()
)
def bronze_data():
    return spark.read.table("source.raw_data")
```

### Pattern 3: Alert Configuration

```python
%run ./configs/config_loader

config = load_config()

# Get alert thresholds
cost_threshold = config.get_alert_threshold('cost', 'daily_increase_percent')
quality_threshold = config.get_alert_threshold('data_quality', 'quality_score_threshold')

# Check quality scores
quality_df = spark.sql(f"""
    SELECT
        dataset_name,
        AVG(pass_rate) as avg_pass_rate
    FROM {config.get_full_table_name('quality_metrics')}
    WHERE timestamp >= current_timestamp() - INTERVAL {config.get('monitoring.default_lookback_days')} DAYS
    GROUP BY dataset_name
    HAVING AVG(pass_rate) < {quality_threshold}
""")

# Send alerts using configured channels
channels = config.get_notification_channels()
if 'email' in channels:
    # Send email alert
    pass
```

## Troubleshooting

### Config file not found
If you see "Config file not found", check that:
1. The `observability_config.yaml` file exists in the `configs/` directory
2. The notebook path resolution is correct
3. You can specify an explicit path: `load_config(config_path='/path/to/config.yaml')`

### YAML parsing errors
If you get YAML parsing errors:
1. Verify YAML syntax is correct (indentation, colons, etc.)
2. Use a YAML validator: https://www.yamllint.com/
3. Check for special characters that need quoting

### Widget values not applying
Make sure to use `get_config_from_widgets()` instead of `load_config()` if you want widget overrides.

## Migration from Hardcoded Values

To migrate existing notebooks to use config:

**Before:**
```python
catalog = "main"
schema = "observability"
event_log_table = f"{catalog}.{schema}.dlt_event_logs"
```

**After:**
```python
%run ./configs/config_loader
config = load_config()
event_log_table = config.get_full_table_name('event_logs')
```

This centralizes configuration and makes it easier to manage multiple environments.

# Databricks Pipeline Observability and Data Quality Monitoring

A comprehensive observability and data quality monitoring solution for Databricks data pipelines, built using Delta Live Tables, Unity Catalog, and Lakehouse Monitoring.

## 🎯 Overview

This project implements an end-to-end observability platform for Databricks data pipelines that provides:

- **Real-time Pipeline Monitoring**: Track DLT pipeline execution, performance, and health
- **Data Quality Management**: Centralized quality rules with automated validation and quarantine
- **Cost and Performance Analytics**: Monitor compute costs, resource utilization, and query performance
- **Data Lineage Tracking**: Automatic capture and visualization of table and column-level lineage
- **Intelligent Alerting**: Proactive notifications for quality issues, failures, and anomalies
- **Statistical Profiling**: Automated data profiling and drift detection using Lakehouse Monitoring

## 📋 Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Components](#components)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Monitoring Dashboards](#monitoring-dashboards)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## ✨ Features

### Data Collection and Integration
- ✅ Automated DLT event log collection and analysis
- ✅ Integration with Databricks system tables (billing, jobs, queries, clusters)
- ✅ Custom application-level logging with Python logging module
- ✅ Unity Catalog lineage capture (table and column level)

### Data Quality Monitoring
- ✅ Centralized quality rules management with Delta tables
- ✅ Dynamic expectation loading in DLT pipelines
- ✅ Multiple action types: warn, drop invalid records, fail pipeline
- ✅ Quarantine tables for invalid data review
- ✅ Quality score tracking and trending
- ✅ Template library for common validation patterns

### Observability and Reporting
- ✅ End-to-end data lineage visualization
- ✅ Impact analysis for upstream/downstream dependencies
- ✅ Performance metrics: backlog, throughput, latency
- ✅ Cost analysis: job costs, SKU breakdown, anomaly detection
- ✅ Cluster utilization monitoring
- ✅ Query performance analysis

### Statistical Profiling
- ✅ Lakehouse Monitoring setup automation
- ✅ Time series and snapshot monitoring
- ✅ Drift detection against baseline tables
- ✅ Column-level statistics tracking
- ✅ Automated dashboard generation

### Alerting and Notifications
- ✅ Multi-channel notifications (Email, Slack, Teams)
- ✅ Configurable alert thresholds
- ✅ DBSQL alerts for scheduled monitoring
- ✅ Job-level failure and SLA alerts
- ✅ Alert history tracking

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
│  DLT Pipelines │ Jobs │ Queries │ Clusters │ Tables         │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│                  Data Collection Layer                       │
│  • DLT Event Logs        • Unity Catalog Lineage            │
│  • System Tables         • Custom App Logs                  │
│  • Lakehouse Monitoring  • Query History                    │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│              Observability Data Store (Delta)                │
│  • Event Logs            • Quality Metrics                  │
│  • Job Metrics           • Cost Metrics                     │
│  • Query Metrics         • Lineage Metadata                 │
│  • Pipeline Health       • Alert History                    │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│           Analytics and Alerting Layer                       │
│  • DBSQL Dashboards      • Scheduled Alerts                 │
│  • Ad-hoc Analysis       • Webhook Notifications            │
│  • Lineage Visualization • Custom Reports                   │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

1. **DLT Pipelines with Quality Gates**: Bronze/Silver/Gold architecture with expectations
2. **Centralized Quality Rules**: Delta table storing reusable validation rules
3. **Monitoring Notebooks**: Collect and analyze observability data
4. **Lakehouse Monitoring**: Automated statistical profiling and drift detection
5. **Alert System**: Multi-channel notifications for SLA violations
6. **Lineage Tracking**: Unity Catalog-based lineage capture and visualization

## 📦 Prerequisites

### Required
- Databricks workspace (AWS, Azure, or GCP)
- Unity Catalog enabled
- Databricks Runtime 13.3 LTS or higher
- System tables enabled (contact Databricks account admin)
- Delta Live Tables capability

### Recommended
- DBSQL warehouse for dashboards and alerts
- Photon enabled for better performance
- Enhanced autoscaling for DLT pipelines

### Permissions Required
- Create and manage tables in Unity Catalog
- Create and run DLT pipelines
- Access system tables (`system.billing`, `system.lakeflow`, `system.access`)
- Create DBSQL queries and alerts
- Configure job-level notifications

## 🚀 Installation

### Step 1: Clone or Import Project

Import this project into your Databricks workspace:

```bash
# Using Databricks CLI
databricks workspace import-dir . /Workspace/Users/<your-email>/pipeline-observe

# Or upload via Databricks UI: Workspace → Import
```

### Step 2: Create Observability Catalog and Schema

Run the setup notebook to create required schemas:

```python
# In a Databricks notebook
%run ./scripts/setup_observability_schema
```

Or manually:

```sql
-- Create catalog and schema
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.observability;

-- Grant permissions
GRANT USE CATALOG ON CATALOG main TO `data-engineers`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA main.observability TO `data-engineers`;
```

### Step 3: Initialize Quality Rules

Run the quality rules manager notebook:

```bash
# Navigate to: notebooks/data_quality/quality_rules_manager.py
# Run all cells to create rules table and sample rules
```

### Step 4: Deploy Sample DLT Pipeline (Optional)

1. Navigate to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure:
   - **Name**: Sample Observability Pipeline
   - **Notebook**: `notebooks/dlt_pipelines/sample_dlt_pipeline.py`
   - **Target**: `main.sample_pipeline`
   - **Storage**: `/mnt/dlt/sample_pipeline`
4. Click **Create** and **Start**

### Step 5: Set Up Monitoring Jobs

Create Databricks jobs to run monitoring notebooks on schedule:

```python
# Example job configuration for DLT monitoring
{
  "name": "DLT Event Log Collection",
  "tasks": [{
    "task_key": "collect_logs",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/<email>/pipeline-observe/notebooks/monitoring/dlt_event_log_monitoring",
      "base_parameters": {
        "catalog": "main",
        "schema": "observability",
        "days_back": "7"
      }
    },
    "job_cluster_key": "monitoring_cluster"
  }],
  "schedule": {
    "quartz_cron_expression": "0 */6 * * * ?",  # Every 6 hours
    "timezone_id": "America/Los_Angeles"
  }
}
```

### Step 6: Configure Alerts

1. Navigate to **SQL** → **Alerts**
2. Import alert queries from `sql/monitoring_dashboard_queries.sql`
3. Configure notification destinations (email, Slack, Teams)
4. Set alert schedules and thresholds

## 📁 Project Structure

```
pipeline-observe/
├── README.md                          # This file
├── databricks_observability_and_data_quality_prd.md  # Product requirements
│
├── notebooks/
│   ├── dlt_pipelines/
│   │   └── sample_dlt_pipeline.py     # Sample DLT pipeline with quality checks
│   │
│   ├── monitoring/
│   │   ├── dlt_event_log_monitoring.py        # DLT event log collection
│   │   ├── system_tables_monitoring.py        # System tables integration
│   │   ├── lineage_tracking.py                # Lineage visualization
│   │   └── alerting_configuration.py          # Alert setup and testing
│   │
│   └── data_quality/
│       └── quality_rules_manager.py    # Quality rules management
│
├── scripts/
│   └── setup_lakehouse_monitoring.py   # Lakehouse Monitoring automation
│
├── sql/
│   └── monitoring_dashboard_queries.sql  # DBSQL dashboard queries
│
├── configs/
│   └── quality_rules/                  # Quality rule configurations
│
└── dashboards/                         # DBSQL dashboard exports
```

## 🚀 Quick Start

### 1. Monitor a DLT Pipeline

```python
# Run DLT event log monitoring
%run ./notebooks/monitoring/dlt_event_log_monitoring

# Widget parameters:
# - catalog: main
# - schema: observability
# - pipeline_id: <your-pipeline-id>
# - days_back: 7
```

### 2. Set Up Quality Rules

```python
# Open quality rules manager
%run ./notebooks/data_quality/quality_rules_manager

# Create a rule
from quality_rules_manager import RuleTemplates

rule = RuleTemplates.not_null(
    dataset_name="customers",
    column_name="email",
    action="drop"
)

# Rules are automatically available to DLT pipelines
```

### 3. Track Data Lineage

```python
# Run lineage tracking notebook
%run ./notebooks/monitoring/lineage_tracking

# Widget parameters:
# - target_table: main.sample_pipeline.gold_customer_metrics
# - lineage_direction: both
# - max_depth: 3
```

### 4. Set Up Lakehouse Monitoring

```python
# Run monitoring setup script
%run ./scripts/setup_lakehouse_monitoring

# Widget parameters:
# - table_to_monitor: main.sample_pipeline.silver_customers
# - monitor_type: TimeSeries
# - timestamp_col: created_date
# - granularity: 1 day
```

### 5. Configure Alerts

```python
# Run alerting configuration
%run ./notebooks/monitoring/alerting_configuration

# Configure notification channels:
# - notification_email: data-team@company.com
# - slack_webhook: https://hooks.slack.com/services/...
# - teams_webhook: https://outlook.office.com/webhook/...
```

## 🔧 Configuration

### Alert Thresholds

Edit thresholds in `notebooks/monitoring/alerting_configuration.py`:

```python
ALERT_THRESHOLDS = {
    "cost": {
        "daily_increase_percent": 20,
        "absolute_threshold": 1000,
    },
    "job_failure": {
        "failure_rate_percent": 10,
        "consecutive_failures": 3,
    },
    "data_quality": {
        "quality_score_threshold": 95,
        "failed_records_threshold": 1000,
    },
    "pipeline_freshness": {
        "hours_stale": 24,
        "critical_hours_stale": 48,
    }
}
```

### Notification Channels

Configure in notebook widgets or environment:

```python
# Email
notification_email = "data-team@company.com"

# Slack webhook
slack_webhook = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# Microsoft Teams webhook
teams_webhook = "https://outlook.office.com/webhook/YOUR/WEBHOOK/URL"
```

## 📊 Usage Examples

### Example 1: Create Custom Quality Rules

```python
from quality_rules_manager import create_rule

# Create a custom validation rule
custom_rule = create_rule(
    dataset_name="orders",
    expectation_name="valid_order_total",
    expectation_type="custom",
    constraint_expression="order_total = (quantity * unit_price) + tax",
    action="warn",
    description="Ensure order total matches calculated value",
    severity="medium",
    tags={"category": "calculation", "owner": "finance-team"}
)

# Rule will be automatically applied in DLT pipelines
```

### Example 2: Query Cost Metrics

```python
# Analyze daily costs
daily_costs = spark.sql("""
    SELECT
        usage_date,
        sku_name,
        SUM(cost) as total_cost,
        SUM(usage_quantity) as total_dbu
    FROM main.observability.cost_metrics
    WHERE usage_date >= current_date() - INTERVAL 30 DAYS
    GROUP BY usage_date, sku_name
    ORDER BY usage_date DESC, total_cost DESC
""")

display(daily_costs)
```

### Example 3: Impact Analysis for Table Changes

```python
from lineage_tracking import analyze_impact

# Analyze impact of changing a table
impact = analyze_impact("main.sample_pipeline.silver_customers")

print(f"Total Downstream Tables: {impact['total_downstream']}")
print(f"Affected Catalogs: {', '.join(impact['affected_catalogs'])}")
print(f"Critical Endpoints: {', '.join(impact['critical_endpoints'])}")
```

### Example 4: Monitor Pipeline Health

```python
# Query pipeline health summary
health = spark.sql("""
    SELECT
        pipeline_name,
        COUNT(*) as total_runs,
        AVG(duration_seconds) / 60 as avg_duration_min,
        SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate,
        SUM(error_count) as total_errors
    FROM main.observability.dlt_pipeline_health
    WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
    GROUP BY pipeline_name
""")

display(health)
```

## 📈 Monitoring Dashboards

### Create DBSQL Dashboards

1. Navigate to **SQL** → **Dashboards** → **Create Dashboard**
2. Import queries from `sql/monitoring_dashboard_queries.sql`
3. Create visualizations:
   - **Line Chart**: Daily cost trends
   - **Pie Chart**: Cost by service/SKU
   - **Bar Chart**: Top expensive jobs
   - **Gauge**: Data quality score
   - **Table**: Recent pipeline failures

### Recommended Dashboard Layout

```
┌─────────────────────────────────────────────────┐
│  Pipeline Observability Dashboard               │
├─────────────────────────────────────────────────┤
│  ┌───────────┐  ┌───────────┐  ┌───────────┐   │
│  │ Quality   │  │ Success   │  │  Cost     │   │
│  │  Score    │  │   Rate    │  │  Today    │   │
│  │   98%     │  │   95%     │  │  $1,234   │   │
│  └───────────┘  └───────────┘  └───────────┘   │
├─────────────────────────────────────────────────┤
│  Daily Cost Trend (Line Chart)                  │
│  Cost by Service (Pie Chart)                    │
│  Pipeline Failures (Table)                      │
│  Quality Metrics Over Time (Line Chart)         │
└─────────────────────────────────────────────────┘
```

## 🎯 Best Practices

### Data Quality
1. **Start Simple**: Begin with critical validations (not null, referential integrity)
2. **Iterative Improvement**: Add rules as you understand your data better
3. **Use Quarantine Tables**: Review invalid data to refine rules
4. **Monitor Rule Effectiveness**: Track pass rates and adjust thresholds

### Pipeline Design
1. **Layer Separation**: Keep bronze/silver/gold boundaries clear
2. **Idempotency**: Ensure pipelines can be re-run safely
3. **Incremental Processing**: Use DLT's incremental features for large datasets
4. **Error Handling**: Use appropriate expectation actions (warn vs. drop vs. fail)

### Monitoring
1. **Set Baselines**: Establish normal ranges for metrics before alerting
2. **Alert Fatigue**: Start with fewer, high-value alerts and expand
3. **Dashboard Design**: Focus on actionable metrics for each audience
4. **Regular Reviews**: Schedule weekly reviews of monitoring data

### Cost Optimization
1. **Rightsize Clusters**: Use monitoring data to optimize worker counts
2. **Schedule Pipelines**: Run during off-peak hours when possible
3. **Enable Photon**: For significant performance improvements
4. **Monitor Waste**: Identify idle clusters and redundant processing

## 🔍 Troubleshooting

### Issue: System Tables Not Available

**Symptom**: `Table or view not found: system.billing.usage`

**Solution**:
- System tables must be enabled by Databricks account admin
- Check with your admin or contact Databricks support
- Alternative: Use Databricks usage APIs

### Issue: DLT Event Logs Not Found

**Symptom**: `event_log('pipelines/xxx') returns no data`

**Solution**:
- Verify pipeline ID is correct
- Ensure pipeline has run at least once
- Check permissions to read pipeline event logs
- Use `event_log('pipelines/*')` to see all pipelines

### Issue: Quality Rules Not Applied

**Symptom**: Expectations don't appear in DLT pipeline

**Solution**:
- Verify rules table path is correct
- Check rules are enabled (`is_enabled = TRUE`)
- Ensure dataset name matches exactly
- Restart DLT pipeline after adding rules

### Issue: Lakehouse Monitoring Fails

**Symptom**: `Monitor creation failed`

**Solution**:
- Verify table exists and has data
- Check timestamp column exists and is valid date/timestamp type
- Ensure you have permissions to create monitors
- Verify output schema exists and is accessible

### Issue: Alerts Not Firing

**Symptom**: No notifications received despite conditions met

**Solution**:
- Test notification channels separately
- Verify webhook URLs are correct
- Check DBSQL alert query returns expected results
- Confirm alert schedule is active
- Review alert history for errors

## 📚 Additional Resources

### Databricks Documentation
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
- [Unity Catalog Lineage](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html)
- [Lakehouse Monitoring](https://docs.databricks.com/lakehouse-monitoring/index.html)
- [System Tables](https://docs.databricks.com/administration-guide/system-tables/index.html)

### Related Projects
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html) - For CI/CD deployment
- [dbt-databricks](https://github.com/databricks/dbt-databricks) - For dbt integration
- [Great Expectations](https://greatexpectations.io/) - Additional data quality framework

## 🤝 Contributing

Contributions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly in a Databricks workspace
5. Submit a pull request with detailed description

## 📄 License

This project is provided as-is for educational and reference purposes.

## 🙏 Acknowledgments

Built following best practices from:
- Databricks Product Documentation
- Databricks Solution Accelerators
- Data Engineering community patterns

---

**Questions or Issues?**
- Review the [PRD](databricks_observability_and_data_quality_prd.md) for detailed requirements
- Check [Troubleshooting](#troubleshooting) section
- Open an issue in your repository

**Happy Monitoring! 🎉**

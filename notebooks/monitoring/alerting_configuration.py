# Databricks notebook source
# MAGIC %md
# MAGIC # Alerting and Notification Configuration
# MAGIC
# MAGIC This notebook provides tools to configure and manage alerts for:
# MAGIC - Data quality violations
# MAGIC - Pipeline failures
# MAGIC - Cost anomalies
# MAGIC - Performance degradation
# MAGIC - Data freshness issues
# MAGIC
# MAGIC **Features:**
# MAGIC - Configure DBSQL alerts
# MAGIC - Set up job-level alerts
# MAGIC - Create custom notification webhooks
# MAGIC - Test alert configurations
# MAGIC
# MAGIC **Requirements:**
# MAGIC - DBSQL warehouse access
# MAGIC - Permission to create alerts
# MAGIC - Notification destinations configured (email, Slack, Teams)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
import requests
import json
from datetime import datetime

# Configuration parameters
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "observability", "Schema Name")
dbutils.widgets.text("notification_email", "", "Notification Email")
dbutils.widgets.text("slack_webhook", "", "Slack Webhook URL")
dbutils.widgets.text("teams_webhook", "", "Teams Webhook URL")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
notification_email = dbutils.widgets.get("notification_email")
slack_webhook = dbutils.widgets.get("slack_webhook")
teams_webhook = dbutils.widgets.get("teams_webhook")

# Databricks workspace URL and token
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

print(f"Alerting Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Workspace: {workspace_url}")
print(f"  Email: {notification_email if notification_email else 'Not configured'}")
print(f"  Slack: {'Configured' if slack_webhook else 'Not configured'}")
print(f"  Teams: {'Configured' if teams_webhook else 'Not configured'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert Configuration Templates

# COMMAND ----------

# Alert thresholds configuration
ALERT_THRESHOLDS = {
    "cost": {
        "daily_increase_percent": 20,  # Alert if daily cost increases by >20%
        "absolute_threshold": 1000,    # Alert if daily cost exceeds $1000
    },
    "job_failure": {
        "failure_rate_percent": 10,    # Alert if >10% of jobs fail in last hour
        "consecutive_failures": 3,     # Alert if job fails 3 times in a row
    },
    "data_quality": {
        "quality_score_threshold": 95, # Alert if quality score drops below 95%
        "failed_records_threshold": 1000, # Alert if >1000 records fail validation
    },
    "pipeline_freshness": {
        "hours_stale": 24,             # Alert if pipeline hasn't run in 24 hours
        "critical_hours_stale": 48,    # Critical alert if 48 hours
    },
    "query_performance": {
        "slow_query_seconds": 60,      # Alert on queries taking >60 seconds
        "error_rate_percent": 5,       # Alert if >5% of queries error
    }
}

print("✅ Alert thresholds configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notification Helper Functions

# COMMAND ----------

def send_email_notification(subject: str, message: str, recipients: list):
    """
    Send email notification using Databricks

    Args:
        subject: Email subject
        message: Email body
        recipients: List of email addresses
    """

    # Note: In production, use Databricks email notification API
    # This is a placeholder implementation

    print(f"📧 Email Notification:")
    print(f"   To: {', '.join(recipients)}")
    print(f"   Subject: {subject}")
    print(f"   Message: {message[:100]}...")

    # In production, integrate with:
    # - Databricks Jobs API email notifications
    # - SendGrid, SES, or other email service

def send_slack_notification(webhook_url: str, message: str, severity: str = "warning"):
    """
    Send notification to Slack

    Args:
        webhook_url: Slack webhook URL
        message: Message to send
        severity: Severity level (info, warning, critical)
    """

    if not webhook_url:
        print("⚠️  Slack webhook not configured")
        return

    # Map severity to colors
    colors = {
        "info": "#36a64f",
        "warning": "#ff9800",
        "critical": "#f44336"
    }

    payload = {
        "attachments": [
            {
                "color": colors.get(severity, "#808080"),
                "title": f"Databricks Observability Alert - {severity.upper()}",
                "text": message,
                "footer": "Databricks Observability Platform",
                "ts": int(datetime.now().timestamp())
            }
        ]
    }

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            print(f"✅ Slack notification sent successfully")
        else:
            print(f"❌ Failed to send Slack notification: {response.status_code}")
    except Exception as e:
        print(f"❌ Error sending Slack notification: {e}")

def send_teams_notification(webhook_url: str, message: str, severity: str = "warning"):
    """
    Send notification to Microsoft Teams

    Args:
        webhook_url: Teams webhook URL
        message: Message to send
        severity: Severity level (info, warning, critical)
    """

    if not webhook_url:
        print("⚠️  Teams webhook not configured")
        return

    # Map severity to colors
    colors = {
        "info": "0078D4",
        "warning": "FF8C00",
        "critical": "D13438"
    }

    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": f"Databricks Observability Alert - {severity.upper()}",
        "themeColor": colors.get(severity, "808080"),
        "title": f"Databricks Observability Alert - {severity.upper()}",
        "text": message
    }

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            print(f"✅ Teams notification sent successfully")
        else:
            print(f"❌ Failed to send Teams notification: {response.status_code}")
    except Exception as e:
        print(f"❌ Error sending Teams notification: {e}")

def send_notification(message: str, severity: str = "warning", channels: list = None):
    """
    Send notification to all configured channels

    Args:
        message: Alert message
        severity: Severity level
        channels: List of channels to notify (default: all configured)
    """

    channels = channels or ["email", "slack", "teams"]

    if "email" in channels and notification_email:
        send_email_notification(
            subject=f"Databricks Alert - {severity.upper()}",
            message=message,
            recipients=[notification_email]
        )

    if "slack" in channels and slack_webhook:
        send_slack_notification(slack_webhook, message, severity)

    if "teams" in channels and teams_webhook:
        send_teams_notification(teams_webhook, message, severity)

print("✅ Notification functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert Check Functions

# COMMAND ----------

def check_cost_alerts():
    """
    Check for cost-related alerts
    """

    alerts = []

    # Check daily cost increase
    cost_check = spark.sql(f"""
        WITH daily_costs AS (
          SELECT
            usage_date,
            SUM(cost) as daily_cost,
            LAG(SUM(cost)) OVER (ORDER BY usage_date) as previous_day_cost
          FROM {catalog}.{schema}.cost_metrics
          WHERE usage_date >= current_date() - INTERVAL 7 DAYS
          GROUP BY usage_date
        )
        SELECT
          usage_date,
          daily_cost,
          previous_day_cost,
          ROUND((daily_cost - previous_day_cost) / previous_day_cost * 100, 2) as percent_change
        FROM daily_costs
        WHERE usage_date = current_date() - INTERVAL 1 DAY
          AND previous_day_cost IS NOT NULL
    """).collect()

    if cost_check:
        row = cost_check[0]
        if row.percent_change > ALERT_THRESHOLDS["cost"]["daily_increase_percent"]:
            alerts.append({
                "type": "cost_increase",
                "severity": "warning",
                "message": f"Daily cost increased by {row.percent_change}% (${row.previous_day_cost:.2f} → ${row.daily_cost:.2f})",
                "details": {
                    "date": str(row.usage_date),
                    "current_cost": row.daily_cost,
                    "previous_cost": row.previous_day_cost,
                    "change_percent": row.percent_change
                }
            })

        if row.daily_cost > ALERT_THRESHOLDS["cost"]["absolute_threshold"]:
            alerts.append({
                "type": "cost_threshold",
                "severity": "critical",
                "message": f"Daily cost ${row.daily_cost:.2f} exceeds threshold ${ALERT_THRESHOLDS['cost']['absolute_threshold']}",
                "details": {
                    "date": str(row.usage_date),
                    "cost": row.daily_cost
                }
            })

    return alerts

def check_job_failure_alerts():
    """
    Check for job failure alerts
    """

    alerts = []

    # Check recent failure rate
    failure_check = spark.sql(f"""
        SELECT
          COUNT(*) as total_runs,
          SUM(CASE WHEN result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED') THEN 1 ELSE 0 END) as failed_runs,
          ROUND(
            SUM(CASE WHEN result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED') THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
          ) as failure_rate
        FROM {catalog}.{schema}.job_metrics
        WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
    """).collect()

    if failure_check and failure_check[0].total_runs > 0:
        row = failure_check[0]
        if row.failure_rate > ALERT_THRESHOLDS["job_failure"]["failure_rate_percent"]:
            alerts.append({
                "type": "job_failure_rate",
                "severity": "critical",
                "message": f"Job failure rate is {row.failure_rate}% ({row.failed_runs}/{row.total_runs} jobs failed in last hour)",
                "details": {
                    "total_runs": row.total_runs,
                    "failed_runs": row.failed_runs,
                    "failure_rate": row.failure_rate
                }
            })

    return alerts

def check_data_quality_alerts():
    """
    Check for data quality alerts
    """

    alerts = []

    # Check recent quality scores
    quality_check = spark.sql(f"""
        SELECT
          pipeline_name,
          dataset_name,
          ROUND(
            SUM(passed_records) * 100.0 / (SUM(passed_records) + SUM(failed_records)),
            2
          ) as quality_score,
          SUM(failed_records) as failed_records
        FROM {catalog}.{schema}.dlt_quality_metrics
        WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
        GROUP BY pipeline_name, dataset_name
        HAVING quality_score < {ALERT_THRESHOLDS["data_quality"]["quality_score_threshold"]}
        ORDER BY quality_score ASC
    """).collect()

    for row in quality_check:
        alerts.append({
            "type": "data_quality",
            "severity": "warning" if row.quality_score > 90 else "critical",
            "message": f"Data quality score for {row.dataset_name} is {row.quality_score}% ({row.failed_records} records failed)",
            "details": {
                "pipeline": row.pipeline_name,
                "dataset": row.dataset_name,
                "quality_score": row.quality_score,
                "failed_records": row.failed_records
            }
        })

    return alerts

def check_pipeline_freshness_alerts():
    """
    Check for pipeline freshness alerts
    """

    alerts = []

    # Check stale pipelines
    freshness_check = spark.sql(f"""
        SELECT
          pipeline_name,
          MAX(start_time) as last_run_time,
          DATEDIFF(HOUR, MAX(start_time), current_timestamp()) as hours_since_last_run
        FROM {catalog}.{schema}.dlt_pipeline_health
        GROUP BY pipeline_name
        HAVING hours_since_last_run > {ALERT_THRESHOLDS["pipeline_freshness"]["hours_stale"]}
        ORDER BY hours_since_last_run DESC
    """).collect()

    for row in freshness_check:
        severity = "critical" if row.hours_since_last_run > ALERT_THRESHOLDS["pipeline_freshness"]["critical_hours_stale"] else "warning"
        alerts.append({
            "type": "pipeline_freshness",
            "severity": severity,
            "message": f"Pipeline {row.pipeline_name} hasn't run in {row.hours_since_last_run} hours (last run: {row.last_run_time})",
            "details": {
                "pipeline": row.pipeline_name,
                "last_run": str(row.last_run_time),
                "hours_stale": row.hours_since_last_run
            }
        })

    return alerts

def check_all_alerts():
    """
    Run all alert checks and return consolidated results
    """

    all_alerts = []

    print("🔍 Checking cost alerts...")
    all_alerts.extend(check_cost_alerts())

    print("🔍 Checking job failure alerts...")
    all_alerts.extend(check_job_failure_alerts())

    print("🔍 Checking data quality alerts...")
    all_alerts.extend(check_data_quality_alerts())

    print("🔍 Checking pipeline freshness alerts...")
    all_alerts.extend(check_pipeline_freshness_alerts())

    return all_alerts

print("✅ Alert check functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Alert Checks

# COMMAND ----------

# Run all alert checks
alerts = check_all_alerts()

print(f"\n📊 Alert Summary: {len(alerts)} alerts found\n")

if alerts:
    # Group by severity
    critical_alerts = [a for a in alerts if a["severity"] == "critical"]
    warning_alerts = [a for a in alerts if a["severity"] == "warning"]
    info_alerts = [a for a in alerts if a["severity"] == "info"]

    print(f"🚨 Critical: {len(critical_alerts)}")
    print(f"⚠️  Warning: {len(warning_alerts)}")
    print(f"ℹ️  Info: {len(info_alerts)}")

    # Display alerts
    for alert in alerts:
        severity_icon = "🚨" if alert["severity"] == "critical" else "⚠️" if alert["severity"] == "warning" else "ℹ️"
        print(f"\n{severity_icon} [{alert['type'].upper()}] {alert['message']}")

    # Send notifications for critical alerts
    for alert in critical_alerts:
        message = f"[{alert['type'].upper()}] {alert['message']}\n\nDetails: {json.dumps(alert['details'], indent=2)}"
        send_notification(message, severity=alert["severity"])

else:
    print("✅ No alerts triggered - all systems healthy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DBSQL Alert Definitions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alert: Daily Cost Increase
# MAGIC
# MAGIC This query can be used to create a DBSQL alert:
# MAGIC 1. Go to DBSQL → Alerts
# MAGIC 2. Create New Alert
# MAGIC 3. Use the query below
# MAGIC 4. Set condition: `cost_alert > 0`
# MAGIC 5. Configure notification destinations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert Query: Daily Cost Increase
# MAGIC WITH avg_cost AS (
# MAGIC   SELECT AVG(daily_cost) as avg_daily_cost
# MAGIC   FROM (
# MAGIC     SELECT usage_date, SUM(cost) as daily_cost
# MAGIC     FROM main.observability.cost_metrics
# MAGIC     WHERE usage_date >= current_date() - INTERVAL 14 DAYS
# MAGIC       AND usage_date < current_date()
# MAGIC     GROUP BY usage_date
# MAGIC   )
# MAGIC ),
# MAGIC today_cost AS (
# MAGIC   SELECT SUM(cost) as today_cost
# MAGIC   FROM main.observability.cost_metrics
# MAGIC   WHERE usage_date = current_date() - INTERVAL 1 DAY
# MAGIC )
# MAGIC SELECT
# MAGIC   ROUND(today_cost, 2) as cost,
# MAGIC   ROUND(avg_daily_cost, 2) as avg_cost,
# MAGIC   ROUND((today_cost - avg_daily_cost) / avg_daily_cost * 100, 2) as percent_above_average,
# MAGIC   CASE
# MAGIC     WHEN (today_cost - avg_daily_cost) / avg_daily_cost > 0.2 THEN 1
# MAGIC     ELSE 0
# MAGIC   END as cost_alert
# MAGIC FROM today_cost, avg_cost;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job-Level Alert Configuration

# COMMAND ----------

def configure_job_alerts(job_id: str, alert_config: dict):
    """
    Configure alerts for a Databricks job using the Jobs API

    Args:
        job_id: Job ID
        alert_config: Alert configuration dictionary
    """

    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }

    # Example alert configuration
    # In production, use the Jobs API to configure:
    # - on_failure notifications
    # - on_success notifications
    # - on_duration_warning_threshold_exceeded
    # - on_streaming_backlog_exceeded

    print(f"📋 Job Alert Configuration for Job {job_id}:")
    print(json.dumps(alert_config, indent=2))

    # Example configuration structure:
    example_config = {
        "email_notifications": {
            "on_failure": [notification_email],
            "on_success": [],
            "no_alert_for_skipped_runs": False
        },
        "webhook_notifications": {
            "on_failure": [
                {
                    "id": "slack-webhook",
                    "url": slack_webhook
                }
            ]
        }
    }

    print("\n📋 Example Job Alert Configuration:")
    print(json.dumps(example_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert Testing

# COMMAND ----------

def test_notifications():
    """
    Test all notification channels
    """

    test_message = """
🧪 **Databricks Observability - Notification Test**

This is a test notification to verify alert configuration.

Timestamp: {timestamp}
Workspace: {workspace}

If you received this message, your notification channel is configured correctly.
    """.format(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        workspace=workspace_url
    )

    print("🧪 Testing notification channels...\n")

    send_notification(test_message, severity="info")

    print("\n✅ Notification test complete")

# Uncomment to test notifications
# test_notifications()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert History Tracking

# COMMAND ----------

# Create alert history table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.alert_history (
  alert_id STRING,
  alert_type STRING,
  severity STRING,
  message STRING,
  details MAP<STRING, STRING>,
  triggered_at TIMESTAMP,
  resolved_at TIMESTAMP,
  status STRING,
  notified_channels ARRAY<STRING>
)
USING DELTA
PARTITIONED BY (DATE(triggered_at))
""")

def log_alert(alert: dict, notified_channels: list = None):
    """
    Log alert to history table

    Args:
        alert: Alert dictionary
        notified_channels: List of channels notified
    """

    alert_record = {
        "alert_id": f"{alert['type']}_{datetime.now().timestamp()}",
        "alert_type": alert["type"],
        "severity": alert["severity"],
        "message": alert["message"],
        "details": alert.get("details", {}),
        "triggered_at": datetime.now(),
        "resolved_at": None,
        "status": "active",
        "notified_channels": notified_channels or []
    }

    spark.createDataFrame([alert_record]).write.mode("append").saveAsTable(
        f"{catalog}.{schema}.alert_history"
    )

# Log all triggered alerts
for alert in alerts:
    log_alert(alert, notified_channels=["email", "slack", "teams"])

print(f"✅ Logged {len(alerts)} alerts to history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Recent Alerts

# COMMAND ----------

spark.sql(f"""
SELECT
  alert_type,
  severity,
  message,
  triggered_at,
  status
FROM {catalog}.{schema}.alert_history
WHERE triggered_at >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY triggered_at DESC
LIMIT 50
""").display()

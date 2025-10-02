-- ============================================================================
-- DATABRICKS OBSERVABILITY - MONITORING DASHBOARD QUERIES
-- ============================================================================
-- This file contains SQL queries for creating monitoring dashboards in DBSQL
-- These queries can be used to create visualizations for pipeline health,
-- cost analysis, performance monitoring, and data quality tracking.
--
-- Prerequisites:
-- - System tables enabled
-- - Observability schema and tables created
-- - Appropriate permissions to query system and observability tables
-- 
-- CONFIGURE:
-- - Change catalog and schema names in SQL below
-- 
-- ============================================================================

-- ============================================================================
-- COST MONITORING QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Daily Cost Trend (Last 30 Days)
-- Use: Line chart showing daily costs over time
-- ----------------------------------------------------------------------------
SELECT
  usage_date,
  SUM(cost) as total_cost,
  SUM(usage_quantity) as total_dbu_usage
FROM main.observability.cost_metrics
WHERE usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY usage_date
ORDER BY usage_date;

-- ----------------------------------------------------------------------------
-- Cost by Service/SKU (Current Month)
-- Use: Pie chart or bar chart showing cost breakdown by service
-- ----------------------------------------------------------------------------
SELECT
  sku_name,
  billing_origin_product,
  SUM(cost) as total_cost,
  ROUND(SUM(cost) / SUM(SUM(cost)) OVER () * 100, 2) as cost_percentage
FROM main.observability.cost_metrics
WHERE usage_date >= DATE_TRUNC('month', current_date())
GROUP BY sku_name, billing_origin_product
ORDER BY total_cost DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- Top 10 Most Expensive Jobs (Last 7 Days)
-- Use: Bar chart showing most expensive jobs
-- ----------------------------------------------------------------------------
SELECT
  job_name,
  COUNT(*) as run_count,
  SUM(total_cost) as total_cost,
  AVG(total_cost) as avg_cost_per_run,
  AVG(duration_seconds) / 3600.0 as avg_duration_hours,
  SUM(total_dbu_usage) as total_dbu_usage
FROM main.observability.job_metrics
WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND result_state = 'SUCCESS'
GROUP BY job_name
ORDER BY total_cost DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- Cost Anomaly Detection (Day-over-Day)
-- Use: Alert on significant cost increases
-- ----------------------------------------------------------------------------
WITH daily_costs AS (
  SELECT
    usage_date,
    SUM(cost) as daily_cost,
    LAG(SUM(cost)) OVER (ORDER BY usage_date) as previous_day_cost
  FROM main.observability.cost_metrics
  WHERE usage_date >= current_date() - INTERVAL 14 DAYS
  GROUP BY usage_date
)
SELECT
  usage_date,
  daily_cost,
  previous_day_cost,
  ROUND((daily_cost - previous_day_cost) / previous_day_cost * 100, 2) as percent_change,
  CASE
    WHEN (daily_cost - previous_day_cost) / previous_day_cost > 0.5 THEN 'ALERT: >50% increase'
    WHEN (daily_cost - previous_day_cost) / previous_day_cost > 0.25 THEN 'WARNING: >25% increase'
    ELSE 'NORMAL'
  END as status
FROM daily_costs
WHERE previous_day_cost IS NOT NULL
ORDER BY usage_date DESC;

-- ============================================================================
-- PERFORMANCE MONITORING QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Job Success Rate (Last 7 Days)
-- Use: Gauge or counter showing success rate
-- ----------------------------------------------------------------------------
SELECT
  COUNT(*) as total_runs,
  SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
  SUM(CASE WHEN result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED') THEN 1 ELSE 0 END) as failed_runs,
  ROUND(SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
FROM main.observability.job_metrics
WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS;

-- ----------------------------------------------------------------------------
-- Job Failure Analysis (Last 7 Days)
-- Use: Table showing job failures with details
-- ----------------------------------------------------------------------------
SELECT
  job_name,
  result_state,
  COUNT(*) as failure_count,
  MAX(start_time) as last_failure,
  COLLECT_SET(SUBSTRING(error_message, 1, 100)) as error_messages
FROM main.observability.job_metrics
WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED')
GROUP BY job_name, result_state
ORDER BY failure_count DESC
LIMIT 25;

-- ----------------------------------------------------------------------------
-- Job Duration Trends (Last 30 Days)
-- Use: Line chart showing job duration over time
-- ----------------------------------------------------------------------------
SELECT
  DATE(start_time) as execution_date,
  job_name,
  AVG(duration_seconds) / 60.0 as avg_duration_minutes,
  MIN(duration_seconds) / 60.0 as min_duration_minutes,
  MAX(duration_seconds) / 60.0 as max_duration_minutes,
  COUNT(*) as run_count
FROM main.observability.job_metrics
WHERE start_time >= current_timestamp() - INTERVAL 30 DAYS
  AND result_state = 'SUCCESS'
  AND duration_seconds IS NOT NULL
GROUP BY DATE(start_time), job_name
HAVING COUNT(*) >= 3  -- Only jobs with multiple runs
ORDER BY execution_date DESC, job_name;

-- ----------------------------------------------------------------------------
-- Slow Query Analysis (Last 24 Hours)
-- Use: Table showing slowest queries for optimization
-- ----------------------------------------------------------------------------
SELECT
  query_id,
  query_start_time,
  duration_ms / 1000.0 as duration_seconds,
  rows_produced,
  ROUND(bytes_scanned / 1024.0 / 1024.0 / 1024.0, 2) as gb_scanned,
  executed_by_user_name,
  SUBSTRING(query_text, 1, 200) as query_preview
FROM main.observability.query_metrics
WHERE query_start_time >= current_timestamp() - INTERVAL 24 HOURS
  AND error_message IS NULL
  AND duration_ms > 10000  -- > 10 seconds
ORDER BY duration_ms DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- Cluster Utilization Summary (Last 7 Days)
-- Use: Bar chart showing cluster usage by name
-- ----------------------------------------------------------------------------
SELECT
  cluster_name,
  AVG(num_workers) as avg_workers,
  MAX(num_workers) as max_workers,
  ROUND(SUM(uptime_seconds) / 3600.0, 2) as total_uptime_hours,
  ROUND(SUM(uptime_seconds) / 3600.0 * AVG(dbu_rate), 2) as estimated_dbu_usage,
  ROUND(SUM(uptime_seconds) / 3600.0 * AVG(dbu_rate) * 0.15, 2) as estimated_cost
FROM main.observability.cluster_metrics
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
  AND state IN ('RUNNING', 'RESIZING')
GROUP BY cluster_name
ORDER BY total_uptime_hours DESC;

-- ============================================================================
-- DATA QUALITY MONITORING QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Data Quality Score by Pipeline (Last 7 Days)
-- Use: Gauge showing overall data quality health
-- ----------------------------------------------------------------------------
SELECT
  pipeline_name,
  COUNT(DISTINCT dataset_name) as dataset_count,
  SUM(passed_records) as total_passed,
  SUM(failed_records) as total_failed,
  ROUND(SUM(passed_records) * 100.0 / (SUM(passed_records) + SUM(failed_records)), 2) as overall_quality_score
FROM main.observability.dlt_quality_metrics
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY pipeline_name
ORDER BY overall_quality_score ASC;

-- ----------------------------------------------------------------------------
-- Failed Expectations by Dataset (Last 7 Days)
-- Use: Table showing which datasets have quality issues
-- ----------------------------------------------------------------------------
SELECT
  dataset_name,
  expectation_name,
  COUNT(*) as failure_count,
  SUM(failed_records) as total_failed_records,
  AVG(pass_rate) as avg_pass_rate,
  MIN(pass_rate) as min_pass_rate,
  MAX(timestamp) as last_failure
FROM main.observability.dlt_quality_metrics
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
  AND failed_records > 0
GROUP BY dataset_name, expectation_name
ORDER BY total_failed_records DESC, failure_count DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- Data Quality Trend (Last 30 Days)
-- Use: Line chart showing quality score over time
-- ----------------------------------------------------------------------------
SELECT
  DATE(timestamp) as quality_date,
  pipeline_name,
  ROUND(
    SUM(passed_records) * 100.0 / (SUM(passed_records) + SUM(failed_records)),
    2
  ) as daily_quality_score,
  SUM(passed_records + failed_records) as total_records_processed
FROM main.observability.dlt_quality_metrics
WHERE timestamp >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY DATE(timestamp), pipeline_name
ORDER BY quality_date DESC, pipeline_name;

-- ----------------------------------------------------------------------------
-- Data Quality Rules Status
-- Use: Table showing active rules and their effectiveness
-- ----------------------------------------------------------------------------
SELECT
  dr.dataset_name,
  dr.expectation_name,
  dr.expectation_type,
  dr.severity,
  dr.is_enabled,
  COALESCE(qm.check_count, 0) as times_checked,
  COALESCE(qm.failure_count, 0) as times_failed,
  ROUND(
    CASE
      WHEN qm.check_count > 0
      THEN (qm.check_count - qm.failure_count) * 100.0 / qm.check_count
      ELSE 100.0
    END,
    2
  ) as pass_rate
FROM main.observability.data_quality_rules dr
LEFT JOIN (
  SELECT
    dataset_name,
    expectation_name,
    COUNT(*) as check_count,
    SUM(CASE WHEN failed_records > 0 THEN 1 ELSE 0 END) as failure_count
  FROM main.observability.dlt_quality_metrics
  WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY dataset_name, expectation_name
) qm ON dr.dataset_name = qm.dataset_name
    AND dr.expectation_name = qm.expectation_name
WHERE dr.is_enabled = TRUE
ORDER BY pass_rate ASC, severity DESC;

-- ============================================================================
-- PIPELINE HEALTH QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Pipeline Execution Summary (Last 7 Days)
-- Use: Table showing pipeline health overview
-- ----------------------------------------------------------------------------
SELECT
  pipeline_name,
  COUNT(*) as total_runs,
  SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as successful_runs,
  SUM(CASE WHEN status IN ('FAILED', 'CANCELED') THEN 1 ELSE 0 END) as failed_runs,
  ROUND(AVG(duration_seconds) / 60.0, 2) as avg_duration_minutes,
  SUM(error_count) as total_errors,
  SUM(warning_count) as total_warnings,
  MAX(start_time) as last_run_time,
  ROUND(
    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    2
  ) as success_rate
FROM main.observability.dlt_pipeline_health
WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY pipeline_name
ORDER BY success_rate ASC, total_runs DESC;

-- ----------------------------------------------------------------------------
-- Pipeline Freshness Check
-- Use: Alert on stale pipelines (haven't run recently)
-- ----------------------------------------------------------------------------
SELECT
  pipeline_name,
  MAX(start_time) as last_run_time,
  DATEDIFF(HOUR, MAX(start_time), current_timestamp()) as hours_since_last_run,
  CASE
    WHEN DATEDIFF(HOUR, MAX(start_time), current_timestamp()) > 48 THEN 'CRITICAL: >48h'
    WHEN DATEDIFF(HOUR, MAX(start_time), current_timestamp()) > 24 THEN 'WARNING: >24h'
    ELSE 'OK'
  END as freshness_status
FROM main.observability.dlt_pipeline_health
GROUP BY pipeline_name
HAVING DATEDIFF(HOUR, MAX(start_time), current_timestamp()) > 12
ORDER BY hours_since_last_run DESC;

-- ----------------------------------------------------------------------------
-- Error Pattern Analysis (Last 7 Days)
-- Use: Table identifying common error patterns
-- ----------------------------------------------------------------------------
SELECT
  SUBSTRING(error, 1, 100) as error_pattern,
  COUNT(DISTINCT pipeline_name) as affected_pipelines,
  COUNT(*) as occurrence_count,
  MAX(timestamp) as last_occurred,
  COLLECT_SET(pipeline_name) as pipeline_list
FROM main.observability.dlt_event_logs
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
  AND level = 'ERROR'
  AND error IS NOT NULL
GROUP BY SUBSTRING(error, 1, 100)
ORDER BY occurrence_count DESC
LIMIT 25;

-- ============================================================================
-- ALERT QUERIES (Use with DBSQL Alerts)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Alert: High Cost Day (>20% above average)
-- Schedule: Daily at 9 AM
-- Condition: cost_alert > 0
-- ----------------------------------------------------------------------------
WITH avg_cost AS (
  SELECT AVG(daily_cost) as avg_daily_cost
  FROM (
    SELECT usage_date, SUM(cost) as daily_cost
    FROM main.observability.cost_metrics
    WHERE usage_date >= current_date() - INTERVAL 14 DAYS
      AND usage_date < current_date()
    GROUP BY usage_date
  )
),
today_cost AS (
  SELECT SUM(cost) as today_cost
  FROM main.observability.cost_metrics
  WHERE usage_date = current_date() - INTERVAL 1 DAY
)
SELECT
  today_cost,
  avg_daily_cost,
  ROUND((today_cost - avg_daily_cost) / avg_daily_cost * 100, 2) as percent_above_average,
  CASE
    WHEN (today_cost - avg_daily_cost) / avg_daily_cost > 0.2 THEN 1
    ELSE 0
  END as cost_alert
FROM today_cost, avg_cost;

-- ----------------------------------------------------------------------------
-- Alert: Job Failure Rate Exceeds Threshold
-- Schedule: Hourly
-- Condition: failure_rate > 10
-- ----------------------------------------------------------------------------
SELECT
  COUNT(*) as total_runs,
  SUM(CASE WHEN result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED') THEN 1 ELSE 0 END) as failed_runs,
  ROUND(
    SUM(CASE WHEN result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED') THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    2
  ) as failure_rate
FROM main.observability.job_metrics
WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR;

-- ----------------------------------------------------------------------------
-- Alert: Data Quality Score Below Threshold
-- Schedule: After each pipeline run
-- Condition: quality_score < 95
-- ----------------------------------------------------------------------------
SELECT
  pipeline_name,
  dataset_name,
  ROUND(
    SUM(passed_records) * 100.0 / (SUM(passed_records) + SUM(failed_records)),
    2
  ) as quality_score,
  SUM(failed_records) as failed_records
FROM main.observability.dlt_quality_metrics
WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY pipeline_name, dataset_name
HAVING quality_score < 95
ORDER BY quality_score ASC;

-- ----------------------------------------------------------------------------
-- Alert: Pipeline Not Running (Freshness)
-- Schedule: Daily at 8 AM
-- Condition: hours_since_last_run > 24
-- ----------------------------------------------------------------------------
SELECT
  pipeline_name,
  MAX(start_time) as last_run_time,
  DATEDIFF(HOUR, MAX(start_time), current_timestamp()) as hours_since_last_run
FROM main.observability.dlt_pipeline_health
GROUP BY pipeline_name
HAVING DATEDIFF(HOUR, MAX(start_time), current_timestamp()) > 24
ORDER BY hours_since_last_run DESC;

-- ============================================================================
-- EXECUTIVE SUMMARY QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Daily Executive Summary
-- Use: Email report or dashboard header
-- ----------------------------------------------------------------------------
SELECT
  current_date() - INTERVAL 1 DAY as report_date,

  -- Cost metrics
  (SELECT SUM(cost)
   FROM main.observability.cost_metrics
   WHERE usage_date = current_date() - INTERVAL 1 DAY) as yesterday_cost,

  -- Job metrics
  (SELECT COUNT(*)
   FROM main.observability.job_metrics
   WHERE DATE(start_time) = current_date() - INTERVAL 1 DAY) as total_jobs_run,

  (SELECT COUNT(*)
   FROM main.observability.job_metrics
   WHERE DATE(start_time) = current_date() - INTERVAL 1 DAY
     AND result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED')) as failed_jobs,

  -- Quality metrics
  (SELECT ROUND(
     SUM(passed_records) * 100.0 / (SUM(passed_records) + SUM(failed_records)),
     2
   )
   FROM main.observability.dlt_quality_metrics
   WHERE DATE(timestamp) = current_date() - INTERVAL 1 DAY) as overall_quality_score,

  (SELECT COUNT(*)
   FROM main.observability.dlt_event_logs
   WHERE DATE(timestamp) = current_date() - INTERVAL 1 DAY
     AND level = 'ERROR') as total_errors;

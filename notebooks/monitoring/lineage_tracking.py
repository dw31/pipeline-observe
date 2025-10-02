# Databricks notebook source
# MAGIC %md
# MAGIC # Data Lineage Tracking and Visualization
# MAGIC
# MAGIC This notebook provides tools to track, query, and visualize data lineage using Unity Catalog.
# MAGIC
# MAGIC **Features:**
# MAGIC - Query table and column-level lineage
# MAGIC - Trace upstream and downstream dependencies
# MAGIC - Identify impact analysis for changes
# MAGIC - Export lineage for documentation
# MAGIC - Detect circular dependencies
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Unity Catalog enabled
# MAGIC - Lineage capture enabled for notebooks and jobs
# MAGIC - System tables access

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json
import networkx as nx
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Configuration parameters
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "observability", "Schema Name")
dbutils.widgets.text("target_table", "", "Target Table (full name)")
dbutils.widgets.dropdown("lineage_direction", "both", ["upstream", "downstream", "both"], "Lineage Direction")
dbutils.widgets.text("max_depth", "3", "Maximum Depth")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
target_table = dbutils.widgets.get("target_table")
lineage_direction = dbutils.widgets.get("lineage_direction")
max_depth = int(dbutils.widgets.get("max_depth"))

# Target tables for storing lineage data
lineage_table = f"{catalog}.{schema}.lineage_metadata"
lineage_graph_table = f"{catalog}.{schema}.lineage_graph"

print(f"Lineage Tracking Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Target Table: {target_table if target_table else 'All tables'}")
print(f"  Direction: {lineage_direction}")
print(f"  Max Depth: {max_depth}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lineage Tables

# COMMAND ----------

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Table lineage metadata
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {lineage_table} (
  source_table_full_name STRING,
  source_table_catalog STRING,
  source_table_schema STRING,
  source_table_name STRING,
  target_table_full_name STRING,
  target_table_catalog STRING,
  target_table_schema STRING,
  target_table_name STRING,
  source_type STRING,
  event_time TIMESTAMP,
  entity_id STRING,
  notebook_id STRING,
  job_id STRING,
  run_id STRING,
  ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(event_time))
""")

# Lineage graph representation
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {lineage_graph_table} (
  node_id STRING,
  node_type STRING,
  node_name STRING,
  upstream_nodes ARRAY<STRING>,
  downstream_nodes ARRAY<STRING>,
  depth_level INT,
  last_updated TIMESTAMP
)
USING DELTA
""")

print(f"✅ Lineage tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Lineage from System Tables

# COMMAND ----------

def collect_table_lineage(days_back: int = 30):
    """
    Collect table-level lineage from system.access.table_lineage

    Args:
        days_back: Number of days to look back
    """

    query = f"""
    SELECT
      source_table_full_name,
      source_table_catalog,
      source_table_schema,
      source_table_name,
      target_table_full_name,
      target_table_catalog,
      target_table_schema,
      target_table_name,
      source_type,
      event_time,
      entity_id,
      notebook_id,
      job_id,
      run_id,
      current_timestamp() as ingestion_timestamp
    FROM system.access.table_lineage
    WHERE event_time >= current_timestamp() - INTERVAL {days_back} DAYS
    """

    try:
        lineage_df = spark.sql(query)
        return lineage_df
    except Exception as e:
        print(f"⚠️  Error collecting lineage: {e}")
        return None

def collect_column_lineage(days_back: int = 30):
    """
    Collect column-level lineage from system.access.column_lineage

    Args:
        days_back: Number of days to look back
    """

    query = f"""
    SELECT
      source_table_full_name,
      source_column_name,
      target_table_full_name,
      target_column_name,
      event_time,
      source_type,
      entity_id
    FROM system.access.column_lineage
    WHERE event_time >= current_timestamp() - INTERVAL {days_back} DAYS
    """

    try:
        column_lineage_df = spark.sql(query)
        return column_lineage_df
    except Exception as e:
        print(f"⚠️  Error collecting column lineage: {e}")
        return None

# Collect lineage data
lineage_df = collect_table_lineage(days_back=30)

if lineage_df:
    print(f"📊 Collected {lineage_df.count()} lineage records")

    # Show sample
    display(lineage_df.limit(10))

    # Write to lineage table
    lineage_df.write.mode("overwrite").saveAsTable(lineage_table)
    print(f"✅ Lineage data written to {lineage_table}")

column_lineage_df = collect_column_lineage(days_back=30)

if column_lineage_df:
    print(f"📊 Collected {column_lineage_df.count()} column lineage records")
    display(column_lineage_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage Query Functions

# COMMAND ----------

def get_upstream_tables(table_name: str, max_depth: int = 3):
    """
    Get all upstream (source) tables for a given table

    Args:
        table_name: Full table name (catalog.schema.table)
        max_depth: Maximum depth to traverse

    Returns:
        DataFrame with upstream dependencies
    """

    query = f"""
    WITH RECURSIVE upstream AS (
      -- Base case: direct upstream
      SELECT
        source_table_full_name as table_name,
        target_table_full_name as reference_table,
        1 as depth,
        ARRAY(source_table_full_name) as path
      FROM {lineage_table}
      WHERE target_table_full_name = '{table_name}'

      UNION ALL

      -- Recursive case: upstream of upstream
      SELECT
        l.source_table_full_name as table_name,
        l.target_table_full_name as reference_table,
        u.depth + 1 as depth,
        ARRAY_UNION(u.path, ARRAY(l.source_table_full_name)) as path
      FROM {lineage_table} l
      INNER JOIN upstream u ON l.target_table_full_name = u.table_name
      WHERE u.depth < {max_depth}
        AND NOT array_contains(u.path, l.source_table_full_name)  -- Prevent cycles
    )
    SELECT DISTINCT
      table_name,
      reference_table,
      depth,
      path
    FROM upstream
    ORDER BY depth, table_name
    """

    try:
        # Note: Databricks SQL doesn't support recursive CTEs yet
        # Use iterative approach instead
        return get_upstream_iterative(table_name, max_depth)
    except Exception as e:
        print(f"⚠️  Error getting upstream tables: {e}")
        return None

def get_upstream_iterative(table_name: str, max_depth: int = 3):
    """
    Get upstream tables using iterative approach
    """

    upstream_tables = []
    current_level = [table_name]
    visited = set()

    for depth in range(1, max_depth + 1):
        if not current_level:
            break

        # Get upstream for current level
        level_df = spark.sql(f"""
            SELECT DISTINCT source_table_full_name
            FROM {lineage_table}
            WHERE target_table_full_name IN ({','.join([f"'{t}'" for t in current_level])})
        """)

        next_level = []
        for row in level_df.collect():
            source_table = row.source_table_full_name

            if source_table not in visited:
                visited.add(source_table)
                upstream_tables.append({
                    "table_name": source_table,
                    "depth": depth
                })
                next_level.append(source_table)

        current_level = next_level

    return spark.createDataFrame(upstream_tables) if upstream_tables else None

def get_downstream_tables(table_name: str, max_depth: int = 3):
    """
    Get all downstream (dependent) tables for a given table

    Args:
        table_name: Full table name (catalog.schema.table)
        max_depth: Maximum depth to traverse

    Returns:
        DataFrame with downstream dependencies
    """

    downstream_tables = []
    current_level = [table_name]
    visited = set()

    for depth in range(1, max_depth + 1):
        if not current_level:
            break

        # Get downstream for current level
        level_df = spark.sql(f"""
            SELECT DISTINCT target_table_full_name
            FROM {lineage_table}
            WHERE source_table_full_name IN ({','.join([f"'{t}'" for t in current_level])})
        """)

        next_level = []
        for row in level_df.collect():
            target_table = row.target_table_full_name

            if target_table not in visited:
                visited.add(target_table)
                downstream_tables.append({
                    "table_name": target_table,
                    "depth": depth
                })
                next_level.append(target_table)

        current_level = next_level

    return spark.createDataFrame(downstream_tables) if downstream_tables else None

def get_column_lineage(table_name: str, column_name: str = None):
    """
    Get column-level lineage for a table or specific column

    Args:
        table_name: Full table name
        column_name: Optional specific column name
    """

    if column_name:
        query = f"""
        SELECT
          source_table_full_name,
          source_column_name,
          target_table_full_name,
          target_column_name,
          source_type
        FROM system.access.column_lineage
        WHERE target_table_full_name = '{table_name}'
          AND target_column_name = '{column_name}'
        ORDER BY source_table_full_name, source_column_name
        """
    else:
        query = f"""
        SELECT
          source_table_full_name,
          source_column_name,
          target_table_full_name,
          target_column_name,
          source_type
        FROM system.access.column_lineage
        WHERE target_table_full_name = '{table_name}'
        ORDER BY target_column_name, source_table_full_name
        """

    try:
        return spark.sql(query)
    except Exception as e:
        print(f"⚠️  Error getting column lineage: {e}")
        return None

print("✅ Lineage query functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Lineage for Target Table

# COMMAND ----------

if target_table:
    print(f"\n📊 Lineage Analysis for {target_table}\n")

    # Get upstream tables
    if lineage_direction in ["upstream", "both"]:
        print(f"⬆️  Upstream Dependencies (Sources):")
        upstream_df = get_upstream_iterative(target_table, max_depth)

        if upstream_df:
            display(upstream_df.orderBy("depth"))
        else:
            print("   No upstream dependencies found")

    # Get downstream tables
    if lineage_direction in ["downstream", "both"]:
        print(f"\n⬇️  Downstream Dependencies (Consumers):")
        downstream_df = get_downstream_tables(target_table, max_depth)

        if downstream_df:
            display(downstream_df.orderBy("depth"))
        else:
            print("   No downstream dependencies found")

    # Get column lineage
    print(f"\n🔍 Column-Level Lineage:")
    column_lineage = get_column_lineage(target_table)

    if column_lineage:
        display(column_lineage)
    else:
        print("   No column lineage found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Impact Analysis

# COMMAND ----------

def analyze_impact(table_name: str):
    """
    Analyze the impact of changes to a table

    Args:
        table_name: Full table name

    Returns:
        Dict with impact analysis
    """

    # Get all downstream dependencies
    downstream_df = get_downstream_tables(table_name, max_depth=5)

    if not downstream_df:
        return {
            "table": table_name,
            "total_downstream": 0,
            "critical_paths": [],
            "affected_catalogs": set(),
            "affected_schemas": set()
        }

    downstream_tables = [row.table_name for row in downstream_df.collect()]

    # Parse catalog and schema
    affected_catalogs = set()
    affected_schemas = set()

    for table in downstream_tables:
        parts = table.split(".")
        if len(parts) == 3:
            affected_catalogs.add(parts[0])
            affected_schemas.add(f"{parts[0]}.{parts[1]}")

    # Find critical paths (tables that are end consumers)
    critical_tables = spark.sql(f"""
        SELECT DISTINCT target_table_full_name
        FROM {lineage_table}
        WHERE source_table_full_name IN ({','.join([f"'{t}'" for t in downstream_tables])})
          AND target_table_full_name NOT IN ({','.join([f"'{t}'" for t in downstream_tables])})
    """)

    return {
        "table": table_name,
        "total_downstream": len(downstream_tables),
        "downstream_tables": downstream_tables,
        "critical_endpoints": [row.target_table_full_name for row in critical_tables.collect()],
        "affected_catalogs": list(affected_catalogs),
        "affected_schemas": list(affected_schemas)
    }

# Example: Analyze impact for target table
if target_table:
    impact_analysis = analyze_impact(target_table)

    print(f"\n🎯 Impact Analysis for {target_table}:")
    print(f"   Total Downstream Tables: {impact_analysis['total_downstream']}")
    print(f"   Affected Catalogs: {', '.join(impact_analysis['affected_catalogs'])}")
    print(f"   Affected Schemas: {', '.join(impact_analysis['affected_schemas'])}")

    if impact_analysis.get('critical_endpoints'):
        print(f"   Critical Endpoints: {', '.join(impact_analysis['critical_endpoints'][:5])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage Visualization

# COMMAND ----------

def build_lineage_graph(table_name: str, max_depth: int = 3):
    """
    Build a networkx graph of lineage relationships

    Args:
        table_name: Center table for the graph
        max_depth: Maximum depth for upstream and downstream

    Returns:
        NetworkX directed graph
    """

    G = nx.DiGraph()

    # Add center node
    G.add_node(table_name, node_type="center", depth=0)

    # Get upstream
    upstream_df = get_upstream_iterative(table_name, max_depth)
    if upstream_df:
        for row in upstream_df.collect():
            G.add_node(row.table_name, node_type="upstream", depth=-row.depth)

    # Get downstream
    downstream_df = get_downstream_tables(table_name, max_depth)
    if downstream_df:
        for row in downstream_df.collect():
            G.add_node(row.table_name, node_type="downstream", depth=row.depth)

    # Add edges from lineage table
    edges_df = spark.sql(f"""
        SELECT DISTINCT source_table_full_name, target_table_full_name
        FROM {lineage_table}
    """)

    for row in edges_df.collect():
        if row.source_table_full_name in G.nodes and row.target_table_full_name in G.nodes:
            G.add_edge(row.source_table_full_name, row.target_table_full_name)

    return G

def visualize_lineage_graph(G, table_name: str):
    """
    Visualize lineage graph using matplotlib

    Args:
        G: NetworkX graph
        table_name: Center table name
    """

    plt.figure(figsize=(16, 10))

    # Position nodes using hierarchical layout
    pos = nx.spring_layout(G, k=2, iterations=50)

    # Color nodes by type
    colors = []
    for node in G.nodes():
        node_data = G.nodes[node]
        if node == table_name:
            colors.append('red')
        elif node_data.get('node_type') == 'upstream':
            colors.append('lightblue')
        elif node_data.get('node_type') == 'downstream':
            colors.append('lightgreen')
        else:
            colors.append('gray')

    # Draw the graph
    nx.draw(
        G,
        pos,
        node_color=colors,
        node_size=3000,
        with_labels=True,
        labels={node: node.split('.')[-1] for node in G.nodes()},  # Show only table name
        font_size=8,
        font_weight='bold',
        arrows=True,
        arrowsize=20,
        edge_color='gray',
        width=2,
        alpha=0.7
    )

    plt.title(f"Data Lineage for {table_name}", fontsize=16, fontweight='bold')
    plt.axis('off')
    plt.tight_layout()

    display(plt.gcf())
    plt.close()

# Example: Build and visualize lineage graph
if target_table:
    print(f"\n🌐 Building lineage graph for {target_table}...")
    lineage_graph = build_lineage_graph(target_table, max_depth)

    print(f"   Nodes: {lineage_graph.number_of_nodes()}")
    print(f"   Edges: {lineage_graph.number_of_edges()}")

    if lineage_graph.number_of_nodes() > 1:
        visualize_lineage_graph(lineage_graph, target_table)
    else:
        print("   ℹ️  Not enough nodes to visualize graph")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most Connected Tables

# COMMAND ----------

spark.sql(f"""
SELECT
  table_name,
  upstream_count,
  downstream_count,
  upstream_count + downstream_count as total_connections
FROM (
  SELECT
    source_table_full_name as table_name,
    COUNT(DISTINCT target_table_full_name) as downstream_count,
    0 as upstream_count
  FROM {lineage_table}
  GROUP BY source_table_full_name

  UNION ALL

  SELECT
    target_table_full_name as table_name,
    0 as downstream_count,
    COUNT(DISTINCT source_table_full_name) as upstream_count
  FROM {lineage_table}
  GROUP BY target_table_full_name
)
GROUP BY table_name
ORDER BY total_connections DESC
LIMIT 25
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables Without Upstream (Source Tables)

# COMMAND ----------

spark.sql(f"""
SELECT DISTINCT source_table_full_name as source_table
FROM {lineage_table}
WHERE source_table_full_name NOT IN (
  SELECT DISTINCT target_table_full_name
  FROM {lineage_table}
)
ORDER BY source_table
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables Without Downstream (End Consumers)

# COMMAND ----------

spark.sql(f"""
SELECT DISTINCT target_table_full_name as end_consumer_table
FROM {lineage_table}
WHERE target_table_full_name NOT IN (
  SELECT DISTINCT source_table_full_name
  FROM {lineage_table}
)
ORDER BY end_consumer_table
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lineage by Source Type

# COMMAND ----------

spark.sql(f"""
SELECT
  source_type,
  COUNT(DISTINCT source_table_full_name) as unique_source_tables,
  COUNT(DISTINCT target_table_full_name) as unique_target_tables,
  COUNT(*) as total_lineage_records
FROM {lineage_table}
GROUP BY source_type
ORDER BY total_lineage_records DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-Catalog Lineage

# COMMAND ----------

spark.sql(f"""
SELECT
  source_table_catalog,
  target_table_catalog,
  COUNT(*) as lineage_count
FROM {lineage_table}
WHERE source_table_catalog != target_table_catalog
GROUP BY source_table_catalog, target_table_catalog
ORDER BY lineage_count DESC
""").display()

# Databricks notebook source
# PL Live Scout Hub - Lakebase Setup & Sync
# This notebook demonstrates the OLAP → OLTP bridge:
#   - Syncs analytical Delta tables to Lakebase (read-only)
#   - Creates transactional OLTP tables in Lakebase for scout data
# This is the core of the Lakebase workshop demo!

# COMMAND ----------

# IMPORTANT: Prerequisites
# 1. You must have created a Lakebase instance in your workspace
#    - Go to Data in your workspace
#    - Create a new Lakebase instance
#    - Note the instance ID and connection string
# 2. Delta tables from 01_setup_delta_tables.py must exist in UC

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# Configuration
WORKSPACE_CATALOG = "dbw_lakebase_workshop"
SCHEMA = "pl_data"
LAKEBASE_INSTANCE_NAME = "pl-scout-hub"  # Lakebase instance name
LAKEBASE_DATABASE = "pl_scout_db"  # Database name in Lakebase

print(f"Workspace Catalog: {WORKSPACE_CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Lakebase Instance ID: {LAKEBASE_INSTANCE_ID}")

# COMMAND ----------

# ============================================================================
# STEP 1: CREATE SYNCED TABLES (OLAP → Lakebase)
# ============================================================================
# These are read-only replicas of our analytical Delta tables
# They sync automatically and provide <10ms latency for the app

print("\n" + "="*70)
print("STEP 1: SYNCING DELTA → LAKEBASE (Read-Only Synced Tables)")
print("="*70)

# COMMAND ----------

# Create synced tables via SQL
# In practice, you'd use the Lakebase UI or the Python SDK
# This is the SQL equivalent showing what synced tables look like

synced_table_sql = f"""
-- These SQL commands demonstrate creating synced tables
-- Execute in your Lakebase instance or via Python SDK

-- Synced Table 1: players
-- Source: {WORKSPACE_CATALOG}.{SCHEMA}.players (Delta Lake)
-- Refresh: Automatic, sub-10ms latency
CREATE TABLE IF NOT EXISTS lakebase_pl_data.players AS
SELECT * FROM {WORKSPACE_CATALOG}.{SCHEMA}.players;

-- Synced Table 2: player_season_stats
-- Source: {WORKSPACE_CATALOG}.{SCHEMA}.player_season_stats (Delta Lake)
CREATE TABLE IF NOT EXISTS lakebase_pl_data.player_season_stats AS
SELECT * FROM {WORKSPACE_CATALOG}.{SCHEMA}.player_season_stats;

-- Synced Table 3: match_events
-- Source: {WORKSPACE_CATALOG}.{SCHEMA}.match_events (Delta Lake)
CREATE TABLE IF NOT EXISTS lakebase_pl_data.match_events AS
SELECT * FROM {WORKSPACE_CATALOG}.{SCHEMA}.match_events;
"""

print("SQL to create synced tables (run in Lakebase instance):")
print(synced_table_sql)

# COMMAND ----------

# ============================================================================
# STEP 2: CREATE OLTP TABLES IN LAKEBASE
# ============================================================================
# These tables are written to by the Streamlit app
# They store transactional scout data

print("\n" + "="*70)
print("STEP 2: CREATING OLTP TABLES FOR SCOUT DATA")
print("="*70)

oltp_tables_sql = f"""
-- ====================================================================
-- OLTP Table 1: scout_reports
-- ====================================================================
-- Written to by the Streamlit app when scouts file reports
-- Each report contains observations on a specific player

CREATE TABLE IF NOT EXISTS lakebase_pl_data.scout_reports (
    report_id VARCHAR(20) PRIMARY KEY,
    player_id VARCHAR(10) NOT NULL,
    scout_name VARCHAR(100) NOT NULL,
    report_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    overall_rating INT NOT NULL CHECK (overall_rating >= 1 AND overall_rating <= 10),
    potential_rating INT NOT NULL CHECK (potential_rating >= 1 AND potential_rating <= 10),
    strengths TEXT,
    weaknesses TEXT,
    recommendation VARCHAR(20) CHECK (recommendation IN ('sign', 'watch', 'pass')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_scout_reports_player_id
    ON lakebase_pl_data.scout_reports(player_id);

-- ====================================================================
-- OLTP Table 2: player_watchlist
-- ====================================================================
-- Players of interest for the club
-- Scouts add/remove players they're monitoring

CREATE TABLE IF NOT EXISTS lakebase_pl_data.player_watchlist (
    watchlist_id VARCHAR(20) PRIMARY KEY,
    player_id VARCHAR(10) NOT NULL UNIQUE,
    added_by VARCHAR(100) NOT NULL,
    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    priority VARCHAR(10) CHECK (priority IN ('high', 'medium', 'low')),
    notes TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_watchlist_player_id
    ON lakebase_pl_data.player_watchlist(player_id);

-- ====================================================================
-- OLTP Table 3: transfer_recommendations
-- ====================================================================
-- Scout recommendations for player acquisitions
-- Includes estimated fees and confidence scores

CREATE TABLE IF NOT EXISTS lakebase_pl_data.transfer_recommendations (
    rec_id VARCHAR(20) PRIMARY KEY,
    player_id VARCHAR(10) NOT NULL,
    recommended_by VARCHAR(100) NOT NULL,
    rec_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    target_team VARCHAR(50),
    estimated_fee_eur BIGINT,
    confidence_score INT CHECK (confidence_score >= 1 AND confidence_score <= 100),
    reasoning TEXT,
    status VARCHAR(20) DEFAULT 'open' CHECK (status IN ('open', 'in_progress', 'completed', 'rejected')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_recommendations_player_id
    ON lakebase_pl_data.transfer_recommendations(player_id);
"""

print("SQL to create OLTP tables in Lakebase:")
print(oltp_tables_sql)

# COMMAND ----------

# ============================================================================
# STEP 3: PYTHON CODE FOR PROGRAMMATIC LAKEBASE SETUP
# ============================================================================
# Use this approach to automate table creation via Databricks SDK

print("\n" + "="*70)
print("STEP 3: PYTHON SDK CODE FOR AUTOMATION")
print("="*70)

python_setup_code = """
# Use databricks.sql to create tables in Lakebase
from databricks import sql

# Connection details (replace with your Lakebase instance)
LAKEBASE_HOST = "your-lakebase-instance.cloud.databricks.com"
LAKEBASE_PORT = 443
LAKEBASE_DB = "lakebase_pl_data"

# Get OAuth token from Databricks SDK
from databricks.sdk import WorkspaceClient
ws = WorkspaceClient()
token = ws.dbutils.secrets.get(scope="your-scope", key="lakebase-token")

# Create connection
with sql.connect(
    host=LAKEBASE_HOST,
    port=LAKEBASE_PORT,
    http_path="/sql/1.0/endpoints/your-endpoint-id",
    auth_type="pat",
    token=token,
) as connection:
    cursor = connection.cursor()

    # Create scout_reports table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scout_reports (
            report_id VARCHAR(20) PRIMARY KEY,
            player_id VARCHAR(10) NOT NULL,
            scout_name VARCHAR(100) NOT NULL,
            report_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            overall_rating INT CHECK (overall_rating >= 1 AND overall_rating <= 10),
            potential_rating INT CHECK (potential_rating >= 1 AND potential_rating <= 10),
            strengths TEXT,
            weaknesses TEXT,
            recommendation VARCHAR(20) CHECK (recommendation IN ('sign', 'watch', 'pass'))
        )
    ''')

    cursor.close()
"""

print(python_setup_code)

# COMMAND ----------

# ============================================================================
# STEP 4: DOCUMENTATION - How Lakebase Bridges OLTP & OLAP
# ============================================================================

print("\n" + "="*70)
print("LAKEBASE ARCHITECTURE: OLTP + OLAP BRIDGE")
print("="*70)

architecture_doc = """
┌─────────────────────────────────────────────────────────────────────┐
│                    PL LIVE SCOUT HUB - DATA FLOW                    │
└─────────────────────────────────────────────────────────────────────┘

ANALYTICAL LAYER (OLAP - Delta Lake in Unity Catalog)
─────────────────────────────────────────────────────
  • players.csv → Delta Table (210 players, all 20 PL teams)
  • player_season_stats.csv → Delta Table (2024/25 stats)
  • match_events.csv → Delta Table (~2000 match events)

  Used for: Analytics, ML, historical analysis
  Refresh: Batch (daily/weekly)

          ↓ LAKEBASE SYNCED TABLES (Read-Only)
          ↓ <10ms latency bridge

TRANSACTIONAL LAYER (OLTP - In Lakebase)
──────────────────────────────────────────
  • scout_reports (transactional INSERTS from app)
  • player_watchlist (CRUD operations)
  • transfer_recommendations (created by scouts)

  Used for: Live app writes, real-time scouting data
  Refresh: Real-time

          ↓ DUAL-SYNC (Optional)
          ↓ Back to Delta Lake for analytics

ANALYTICS QUERIES (OLAP + OLTP TOGETHER)
─────────────────────────────────────────
  SELECT
    p.name,
    p.team,
    s.goals,
    s.xg,
    sr.overall_rating,    ← Scout data (OLTP)
    sr.recommendation
  FROM
    lakebase.players p                    ← Synced (OLAP)
    JOIN lakebase.player_season_stats s   ← Synced (OLAP)
    LEFT JOIN lakebase.scout_reports sr   ← OLTP
    ON p.player_id = sr.player_id

This query demonstrates the VALUE: You can join live analytical data
with transactional scouting data in a single query!

ADVANTAGES OF LAKEBASE FOR THIS DEMO
────────────────────────────────────
1. Synced Tables: Read-only replicas of Delta Lake with <10ms latency
2. OLTP Guaranteed: ACID transactions for scout data (no data loss)
3. SQL Unified: Single query language for both OLAP & OLTP
4. No ETL: No extract, transform, load pipeline needed
5. Real-time: Scout data visible to analytics immediately
"""

print(architecture_doc)

# COMMAND ----------

# ============================================================================
# STEP 5: SAMPLE QUERIES THAT WOULD RUN IN THE STREAMLIT APP
# ============================================================================

print("\n" + "="*70)
print("SAMPLE QUERIES FOR STREAMLIT APP")
print("="*70)

sample_queries = """
-- Query 1: Player Dashboard - Top Performers
SELECT
    p.player_id,
    p.name,
    p.team,
    p.position,
    s.goals,
    s.assists,
    s.xg,
    s.xa,
    COALESCE(sr.overall_rating, 'Not scouted') as scout_rating
FROM lakebase.players p
JOIN lakebase.player_season_stats s ON p.player_id = s.player_id
LEFT JOIN (
    -- Get latest scout report for each player
    SELECT DISTINCT ON (player_id)
        player_id,
        overall_rating,
        recommendation
    FROM lakebase.scout_reports
    ORDER BY player_id, report_date DESC
) sr ON p.player_id = sr.player_id
WHERE p.position = 'FWD'
ORDER BY s.goals DESC
LIMIT 20;

-- Query 2: Scout Board - All Reports with Player Context
SELECT
    sr.report_id,
    p.name,
    p.team,
    sr.scout_name,
    sr.report_date,
    sr.overall_rating,
    sr.potential_rating,
    sr.recommendation,
    sr.strengths,
    sr.weaknesses
FROM lakebase.scout_reports sr
JOIN lakebase.players p ON sr.player_id = p.player_id
ORDER BY sr.report_date DESC;

-- Query 3: Scouted vs Unscouted Analysis
SELECT
    CASE
        WHEN sr.player_id IS NOT NULL THEN 'Scouted'
        ELSE 'Unscouted'
    END as scout_status,
    COUNT(*) as player_count,
    ROUND(AVG(s.goals), 1) as avg_goals,
    ROUND(AVG(s.xg), 1) as avg_xg,
    ROUND(AVG(s.assists), 1) as avg_assists
FROM lakebase.players p
JOIN lakebase.player_season_stats s ON p.player_id = s.player_id
LEFT JOIN lakebase.scout_reports sr ON p.player_id = sr.player_id
GROUP BY 1
ORDER BY 2 DESC;

-- Query 4: Player Watchlist with Stats
SELECT
    p.player_id,
    p.name,
    p.team,
    p.position,
    p.age,
    pw.priority,
    pw.added_date,
    s.goals,
    s.xg,
    s.assists
FROM lakebase.player_watchlist pw
JOIN lakebase.players p ON pw.player_id = p.player_id
JOIN lakebase.player_season_stats s ON p.player_id = s.player_id
ORDER BY
    CASE
        WHEN pw.priority = 'high' THEN 1
        WHEN pw.priority = 'medium' THEN 2
        ELSE 3
    END,
    pw.added_date DESC;

-- Query 5: Transfer Recommendations Board
SELECT
    tr.rec_id,
    p.name,
    p.team,
    tr.target_team,
    tr.estimated_fee_eur,
    tr.confidence_score,
    tr.recommended_by,
    tr.rec_date,
    tr.status
FROM lakebase.transfer_recommendations tr
JOIN lakebase.players p ON tr.player_id = p.player_id
WHERE tr.status = 'open'
ORDER BY tr.confidence_score DESC;
"""

print(sample_queries)

# COMMAND ----------

print("\n" + "="*70)
print("LAKEBASE SETUP COMPLETE")
print("="*70)
print("""
✓ Synced tables configuration documented
✓ OLTP table schemas defined
✓ Sample queries provided

NEXT STEPS:
1. In your Lakebase instance, create the synced tables (copy the SQL from STEP 1)
2. Create the OLTP tables in Lakebase (copy the SQL from STEP 2)
3. Obtain your Lakebase connection string and credentials
4. Run the Streamlit app (app/app.py) which will connect to Lakebase
5. Start scouting! The app will write scout data to the OLTP tables

KEY INSIGHT:
The beauty of Lakebase is that analytical queries can instantly access
both Delta Lake data (synced tables) and transactional scout data in one query!
""")

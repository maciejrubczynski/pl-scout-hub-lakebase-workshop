# Databricks notebook source
# PL Live Scout Hub - Delta Table Setup
# This notebook loads CSV data and creates Delta tables in Unity Catalog
# Part of the Lakebase OLAP → Synced Tables bridge demo

# COMMAND ----------

# Setup: Install required libraries
# %pip install pandas pyspark

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# Configuration - customize these values for your workspace
WORKSPACE_CATALOG = "dbw_lakebase_workshop"  # Your UC catalog
SCHEMA = "pl_data"
DATA_PATH = "/Volumes/{catalog}/{schema}/raw_data".format(catalog=WORKSPACE_CATALOG, schema=SCHEMA)

print(f"Target catalog: {WORKSPACE_CATALOG}")
print(f"Target schema: {SCHEMA}")
print(f"Data path: {DATA_PATH}")

# COMMAND ----------

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {WORKSPACE_CATALOG}.{SCHEMA}")

# COMMAND ----------

# 1. Load and create PLAYERS table
# This is the dimension table for all PL players
players_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{DATA_PATH}/players.csv")

# Display sample
print("Players table preview:")
players_df.show(5)

# Create/overwrite Delta table
players_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{WORKSPACE_CATALOG}.{SCHEMA}.players")

print(f"\n✓ Created Delta table: {WORKSPACE_CATALOG}.{SCHEMA}.players")
print(f"  Records: {players_df.count()}")

# COMMAND ----------

# 2. Load and create PLAYER_SEASON_STATS table
# This is the fact table for 2024/25 season statistics
season_stats_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{DATA_PATH}/player_season_stats.csv")

# Cast numeric columns
season_stats_df = season_stats_df \
    .withColumn("xg", F.col("xg").cast(DoubleType())) \
    .withColumn("xa", F.col("xa").cast(DoubleType()))

print("Season Stats table preview:")
season_stats_df.show(5)

season_stats_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{WORKSPACE_CATALOG}.{SCHEMA}.player_season_stats")

print(f"\n✓ Created Delta table: {WORKSPACE_CATALOG}.{SCHEMA}.player_season_stats")
print(f"  Records: {season_stats_df.count()}")

# COMMAND ----------

# 3. Load and create MATCH_EVENTS table
# Event-level data (goals, yellow cards, substitutions, etc.)
events_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{DATA_PATH}/match_events.csv")

print("Match Events table preview:")
events_df.show(5)

events_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{WORKSPACE_CATALOG}.{SCHEMA}.match_events")

print(f"\n✓ Created Delta table: {WORKSPACE_CATALOG}.{SCHEMA}.match_events")
print(f"  Records: {events_df.count()}")

# COMMAND ----------

# Validate the tables
print("\n" + "="*60)
print("DELTA TABLES CREATED SUCCESSFULLY")
print("="*60)

spark.sql(f"SELECT COUNT(*) as player_count FROM {WORKSPACE_CATALOG}.{SCHEMA}.players").show()
spark.sql(f"SELECT COUNT(*) as stat_count FROM {WORKSPACE_CATALOG}.{SCHEMA}.player_season_stats").show()
spark.sql(f"SELECT COUNT(*) as event_count FROM {WORKSPACE_CATALOG}.{SCHEMA}.match_events").show()

# COMMAND ----------

# Sample analytics queries on the Delta tables
print("\n" + "="*60)
print("SAMPLE QUERIES")
print("="*60)

# Query 1: Top 10 goal scorers
print("\n1. Top 10 Goal Scorers (2024/25):")
spark.sql(f"""
    SELECT
        p.name,
        p.team,
        p.position,
        s.goals,
        s.appearances,
        ROUND(s.goals / s.appearances, 2) as goals_per_game
    FROM {WORKSPACE_CATALOG}.{SCHEMA}.player_season_stats s
    JOIN {WORKSPACE_CATALOG}.{SCHEMA}.players p ON s.player_id = p.player_id
    ORDER BY s.goals DESC
    LIMIT 10
""").show()

# Query 2: Top 10 assist makers
print("\n2. Top 10 Assist Makers (2024/25):")
spark.sql(f"""
    SELECT
        p.name,
        p.team,
        p.position,
        s.assists,
        s.appearances
    FROM {WORKSPACE_CATALOG}.{SCHEMA}.player_season_stats s
    JOIN {WORKSPACE_CATALOG}.{SCHEMA}.players p ON s.player_id = p.player_id
    ORDER BY s.assists DESC
    LIMIT 10
""").show()

# Query 3: Top 10 by xG (underperformers and overperformers)
print("\n3. Top 10 by Expected Goals (xG) - 2024/25:")
spark.sql(f"""
    SELECT
        p.name,
        p.team,
        p.position,
        s.goals,
        s.xg,
        ROUND(s.goals - s.xg, 2) as goal_variance
    FROM {WORKSPACE_CATALOG}.{SCHEMA}.player_season_stats s
    JOIN {WORKSPACE_CATALOG}.{SCHEMA}.players p ON s.player_id = p.player_id
    WHERE s.position = 'FWD'
    ORDER BY s.xg DESC
    LIMIT 10
""").show()

# Query 4: Defensive stats by team
print("\n4. Defensive Stats by Team (avg tackles & interceptions per defender):")
spark.sql(f"""
    SELECT
        p.team,
        COUNT(*) as defenders,
        ROUND(AVG(s.tackles_won), 1) as avg_tackles,
        ROUND(AVG(s.interceptions), 1) as avg_interceptions,
        ROUND(AVG(s.clean_sheets), 1) as avg_clean_sheets
    FROM {WORKSPACE_CATALOG}.{SCHEMA}.player_season_stats s
    JOIN {WORKSPACE_CATALOG}.{SCHEMA}.players p ON s.player_id = p.player_id
    WHERE p.position = 'DEF'
    GROUP BY p.team
    ORDER BY avg_tackles DESC
""").show()

# COMMAND ----------

print("\n✓ Setup complete! Your Delta tables are ready for Lakebase sync.")
print(f"\nNext steps:")
print(f"1. Run 02_setup_lakebase_sync.py to sync these tables to Lakebase")
print(f"2. Deploy the Streamlit app for the scouting interface")

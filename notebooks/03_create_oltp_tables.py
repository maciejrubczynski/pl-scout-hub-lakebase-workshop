# Databricks notebook source
# PL Live Scout Hub - Create OLTP Tables in Lakebase
# Creates transactional tables for scout data in the Lakebase PostgreSQL instance
# These tables are written to by the Streamlit app

# COMMAND ----------

# IMPORTANT: Requires databricks-sdk >= 0.61.0
# %pip install databricks-sdk>=0.61.0

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import psycopg2
import uuid

# Configuration
LAKEBASE_INSTANCE = "pl-scout-hub"
LAKEBASE_DATABASE = "pl_scout_db"

w = WorkspaceClient()

# Get instance details
instance = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
print(f"Instance: {instance.name}")
print(f"Host: {instance.read_write_dns}")
print(f"UID: {instance.uid}")

# Generate database credential
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[LAKEBASE_INSTANCE]
)
print("Database credential obtained successfully")

# COMMAND ----------

# Connect to Lakebase PostgreSQL
conn = psycopg2.connect(
    host=instance.read_write_dns,
    port=5432,
    database=LAKEBASE_DATABASE,
    user=w.current_user.me().user_name,
    password=cred.token,
    sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()
print("Connected to Lakebase!")

# COMMAND ----------

# ============================================================================
# Create the 'scout' schema for OLTP tables
# ============================================================================

cur.execute("CREATE SCHEMA IF NOT EXISTS scout")
print("Schema 'scout' created")

# COMMAND ----------

# ============================================================================
# Table 1: scout_reports
# ============================================================================
# Written to by the Streamlit app when scouts file reports
# player_id is TEXT to match synced table format from Delta Lake

cur.execute("""
CREATE TABLE IF NOT EXISTS scout.scout_reports (
    report_id SERIAL PRIMARY KEY,
    player_id TEXT NOT NULL,
    scout_name VARCHAR(100) NOT NULL,
    report_date DATE DEFAULT CURRENT_DATE,
    overall_rating INTEGER CHECK (overall_rating >= 1 AND overall_rating <= 10),
    technical_rating INTEGER CHECK (technical_rating >= 1 AND technical_rating <= 10),
    physical_rating INTEGER CHECK (physical_rating >= 1 AND physical_rating <= 10),
    mental_rating INTEGER CHECK (mental_rating >= 1 AND mental_rating <= 10),
    potential_rating INTEGER CHECK (potential_rating >= 1 AND potential_rating <= 10),
    strengths TEXT,
    weaknesses TEXT,
    recommendation VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
print("Created scout.scout_reports")

# COMMAND ----------

# ============================================================================
# Table 2: player_watchlist
# ============================================================================

cur.execute("""
CREATE TABLE IF NOT EXISTS scout.player_watchlist (
    watchlist_id SERIAL PRIMARY KEY,
    player_id TEXT NOT NULL UNIQUE,
    added_by VARCHAR(100) NOT NULL,
    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    priority VARCHAR(10) CHECK (priority IN ('high', 'medium', 'low')),
    notes TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
print("Created scout.player_watchlist")

# COMMAND ----------

# ============================================================================
# Table 3: transfer_recommendations
# ============================================================================

cur.execute("""
CREATE TABLE IF NOT EXISTS scout.transfer_recommendations (
    rec_id SERIAL PRIMARY KEY,
    player_id TEXT NOT NULL,
    recommended_by VARCHAR(100) NOT NULL,
    rec_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    target_team VARCHAR(50),
    estimated_fee_eur BIGINT,
    confidence_score INTEGER CHECK (confidence_score >= 1 AND confidence_score <= 100),
    reasoning TEXT,
    status VARCHAR(20) DEFAULT 'open' CHECK (status IN ('open', 'in_progress', 'completed', 'rejected')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
print("Created scout.transfer_recommendations")

# COMMAND ----------

# ============================================================================
# Insert sample scout reports using real player IDs from synced tables
# ============================================================================

# Get player IDs for well-known players
cur.execute("""
    SELECT player_id, name, team FROM pl_data.players_synced
    WHERE name IN ('Mohamed Salah', 'Erling Haaland', 'Bukayo Saka',
                   'Cole Palmer', 'Bruno Fernandes', 'Declan Rice')
    ORDER BY name
""")
players = cur.fetchall()
print(f"Found {len(players)} players for sample data:")
for p in players:
    print(f"  {p[0]}: {p[1]} ({p[2]})")

# COMMAND ----------

# Insert sample reports
sample_reports = [
    (players[0][0], "John Smith", 9, 9, 8, 8, 9,
     "World-class finishing, incredible positioning",
     "Age might be a concern for long-term investment", "Sign"),
    (players[1][0], "Maria Garcia", 10, 9, 10, 8, 9,
     "Unstoppable in the box, dominant aerial presence",
     "Can be isolated in build-up play", "Sign"),
    (players[2][0], "James Wilson", 8, 9, 7, 8, 9,
     "Excellent dribbling, creative in tight spaces",
     "Needs to add more end product consistency", "Monitor"),
    (players[3][0], "Sarah Chen", 9, 10, 7, 9, 10,
     "Outstanding technical ability, mature beyond years",
     "Physical development still needed", "Sign"),
    (players[4][0], "David Brown", 8, 8, 7, 9, 8,
     "Great vision and passing range, set-piece specialist",
     "Defensive contribution inconsistent", "Monitor"),
]

if len(players) > 5:
    sample_reports.append(
        (players[5][0], "Emma Taylor", 8, 7, 9, 8, 8,
         "Elite ball-winning ability, excellent reading of the game",
         "Could improve on the ball in tight areas", "Sign")
    )

for r in sample_reports:
    cur.execute("""
        INSERT INTO scout.scout_reports
        (player_id, scout_name, report_date, overall_rating, technical_rating,
         physical_rating, mental_rating, potential_rating, strengths, weaknesses, recommendation)
        VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s)
    """, r)

print(f"Inserted {len(sample_reports)} sample scout reports")

# COMMAND ----------

# ============================================================================
# Verify: Cross-schema JOIN (OLAP + OLTP)
# ============================================================================

cur.execute("""
    SELECT
        sr.report_id,
        p.name,
        p.team,
        sr.scout_name,
        sr.overall_rating,
        sr.recommendation
    FROM scout.scout_reports sr
    JOIN pl_data.players_synced p ON sr.player_id = p.player_id
    ORDER BY sr.report_id
""")

print("\n=== Scout Reports with Player Names (OLAP+OLTP JOIN) ===")
for row in cur.fetchall():
    print(f"  #{row[0]}: {row[1]} ({row[2]}) - Scout: {row[3]}, Rating: {row[4]}/10, Rec: {row[5]}")

# COMMAND ----------

# Cleanup
cur.close()
conn.close()
print("\nOLTP tables created successfully!")
print("The Streamlit app can now write scout data to these tables.")

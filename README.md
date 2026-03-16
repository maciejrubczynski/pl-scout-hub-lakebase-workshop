# ⚽ PL Live Scout Hub

**A Databricks Apps Workshop Demo Showcasing Lakebase's OLTP+OLAP Bridge**

The PL Live Scout Hub is a production-ready demonstration of how **Lakebase** bridges the gap between analytical data (OLAP) and transactional operations (OLTP) on a single platform. Built for the Premier League scouting use case, this application showcases how scouts can work with live player analytics while simultaneously capturing their observations in a transactional system—all without complex ETL pipelines or data synchronization headaches.

---

## 🎯 What is Lakebase?

**Lakebase** is Databricks' unified analytics and transactions platform that provides:

- **OLAP (Read Layer)**: Fast analytical queries on Delta Lake data via synced, read-only tables
- **OLTP (Write Layer)**: ACID-guaranteed transactional writes for operational data
- **Single Query Engine**: One SQL query can join analytical + transactional data
- **<10ms Latency**: Synced tables from Delta Lake are cached for fast reads
- **No ETL**: No extract-transform-load pipelines needed

### The Problem Lakebase Solves

Traditional data architectures force a choice:
- **Data Warehouses** (Snowflake, BigQuery): Fast OLAP but poor for transactions
- **Operational Databases** (PostgreSQL, MySQL): Great for OLTP but slow for analytics
- **Hybrid Solutions**: ETL pipelines, dual systems, data consistency headaches

**Lakebase** unifies both in one platform.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│              PL LIVE SCOUT HUB - DATA FLOW                      │
└─────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────┐
│   ANALYTICAL LAYER (OLAP)              │
│   Delta Lake in Unity Catalog          │
├────────────────────────────────────────┤
│ • players.csv                          │
│ • player_season_stats.csv (2024/25)    │
│ • match_events.csv (~2000 events)      │
│                                        │
│ 210 Premier League players             │
│ All 20 teams, 10-11 players each       │
│ Realistic season statistics            │
└────────────────────────────────────────┘
          ↓
┌────────────────────────────────────────┐
│   LAKEBASE SYNCED TABLES               │
│   (Read-Only, <10ms latency)           │
├────────────────────────────────────────┤
│ • lakebase.players (synced)            │
│ • lakebase.player_season_stats (synced)│
│ • lakebase.match_events (synced)       │
│                                        │
│ → Streamlit app reads with low latency │
└────────────────────────────────────────┘
          ↕️ (OLAP + OLTP BRIDGE)
┌────────────────────────────────────────┐
│   TRANSACTIONAL LAYER (OLTP)           │
│   Lakebase ACID Tables                 │
├────────────────────────────────────────┤
│ • scout_reports                        │
│ • player_watchlist                     │
│ • transfer_recommendations             │
│                                        │
│ → Streamlit app writes scout data      │
│ → Visible immediately in queries       │
└────────────────────────────────────────┘
          ↓
┌────────────────────────────────────────┐
│   ANALYTICS (OLAP + OLTP TOGETHER)     │
│   Single SQL Query Joins Both Layers   │
├────────────────────────────────────────┤
│ SELECT p.name, s.goals, sr.rating      │
│ FROM synced_players p                  │
│ JOIN synced_stats s ON p.id = s.id     │
│ LEFT JOIN scout_reports sr ON ...      │
│                                        │
│ This is the MAGIC: Analytics + Ops    │
└────────────────────────────────────────┘
```

---

## 📊 Data Model

### Synced Tables (OLAP - Delta Lake)

#### `players` (210 records)
Real Premier League players for the 2024/25 season.

| Column | Type | Example |
|--------|------|---------|
| `player_id` | VARCHAR(10) | KC001 |
| `name` | VARCHAR(100) | Kevin De Bruyne |
| `team` | VARCHAR(50) | Manchester City |
| `position` | VARCHAR(5) | MID |
| `nationality` | VARCHAR(50) | Belgian |
| `age` | INT | 33 |
| `market_value_eur` | BIGINT | 60,000,000 |

**Real Players Include**: Haaland, Salah, Saka, Palmer, Rice, Akanji, van Dijk, Mount, Ødegaard, Martinelli...

#### `player_season_stats` (210 records)
2024/25 season statistics for all players.

| Column | Type | Notes |
|--------|------|-------|
| `stat_id` | VARCHAR(20) | Unique ID |
| `player_id` | VARCHAR(10) | FK to players |
| `season` | VARCHAR(10) | "2024/25" |
| `appearances` | INT | Games played |
| `goals` | INT | Goals scored |
| `assists` | INT | Assists |
| `shots_on_target` | INT | |
| `xg` | DECIMAL(5,2) | Expected goals |
| `xa` | DECIMAL(5,2) | Expected assists |
| ... more stats ... | | |

**Advanced Metrics**: xG, xA, tackles, interceptions, clean sheets, key passes.

#### `match_events` (~2000 records)
Event-level match data (goals, yellow cards, substitutions, etc.).

| Column | Type | Example |
|--------|------|---------|
| `event_id` | VARCHAR(20) | EV00001 |
| `match_id` | VARCHAR(20) | M00001 |
| `player_id` | VARCHAR(10) | MO001 |
| `event_type` | VARCHAR(20) | goal, assist, yellow_card, red_card, substitution |
| `minute` | INT | 45 |
| `match_date` | DATE | 2024-08-20 |
| `home_team` | VARCHAR(50) | Manchester City |
| `away_team` | VARCHAR(50) | Chelsea |
| `home_score` | INT | 2 |
| `away_score` | INT | 1 |

### OLTP Tables (Lakebase - Transactional)

#### `scout_reports`
Scout observations and ratings (written by app).

```sql
CREATE TABLE scout_reports (
    report_id VARCHAR(20) PRIMARY KEY,
    player_id VARCHAR(10) NOT NULL,
    scout_name VARCHAR(100) NOT NULL,
    report_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    overall_rating INT CHECK (overall_rating >= 1 AND overall_rating <= 10),
    potential_rating INT CHECK (potential_rating >= 1 AND potential_rating <= 10),
    strengths TEXT,
    weaknesses TEXT,
    recommendation VARCHAR(20) CHECK (recommendation IN ('sign', 'watch', 'pass')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `player_watchlist`
Players being monitored by scouts.

```sql
CREATE TABLE player_watchlist (
    watchlist_id VARCHAR(20) PRIMARY KEY,
    player_id VARCHAR(10) NOT NULL UNIQUE,
    added_by VARCHAR(100) NOT NULL,
    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    priority VARCHAR(10) CHECK (priority IN ('high', 'medium', 'low')),
    notes TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `transfer_recommendations`
Transfer suggestions with estimated fees.

```sql
CREATE TABLE transfer_recommendations (
    rec_id VARCHAR(20) PRIMARY KEY,
    player_id VARCHAR(10) NOT NULL,
    recommended_by VARCHAR(100) NOT NULL,
    rec_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    target_team VARCHAR(50),
    estimated_fee_eur BIGINT,
    confidence_score INT CHECK (confidence_score >= 1 AND confidence_score <= 100),
    reasoning TEXT,
    status VARCHAR(20) DEFAULT 'open',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 🚀 Step-by-Step Setup Guide

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Lakebase instance created (or ability to create one)
- Python 3.9+
- Databricks CLI configured
- A user with permissions to create catalogs/schemas

### Step 1: Generate Synthetic Data

The data generation script creates realistic Premier League player and match data.

```bash
cd data
python generate_data.py
```

**Output:**
- `players.csv` (210 players)
- `player_season_stats.csv` (210 season records)
- `match_events.csv` (~2000 match events)

### Step 2: Upload Data to Databricks

Upload the CSVs to your workspace (to Volumes or DBFS):

```bash
# Using Databricks CLI
databricks fs cp data/players.csv dbfs:/Volumes/workspace_catalog/pl_data/input_data/players.csv
databricks fs cp data/player_season_stats.csv dbfs:/Volumes/workspace_catalog/pl_data/input_data/player_season_stats.csv
databricks fs cp data/match_events.csv dbfs:/Volumes/workspace_catalog/pl_data/input_data/match_events.csv
```

Or upload via the Databricks UI:
1. Go to **Data** → **Volumes**
2. Create a new Volume: `workspace_catalog.pl_data.input_data`
3. Upload the 3 CSV files

### Step 3: Create Delta Tables

Run the first notebook in your Databricks workspace:

```bash
databricks workspace import --language PYTHON notebooks/01_setup_delta_tables.py /Users/your-email/pl-scout-hub/01_setup_delta_tables
```

Or copy-paste the notebook content into a Databricks notebook cell and run it.

**What it does:**
- Creates Unity Catalog schema `workspace_catalog.pl_data`
- Loads CSVs and creates Delta tables:
  - `workspace_catalog.pl_data.players`
  - `workspace_catalog.pl_data.player_season_stats`
  - `workspace_catalog.pl_data.match_events`
- Runs sample analytical queries

**Expected Output:**
```
✓ Created Delta table: workspace_catalog.pl_data.players
  Records: 210
✓ Created Delta table: workspace_catalog.pl_data.player_season_stats
  Records: 210
✓ Created Delta table: workspace_catalog.pl_data.match_events
  Records: 1968
```

### Step 4: Create Lakebase Instance

1. In your Databricks workspace, go to **Data**
2. Click **Create** → **Lakebase instance**
3. Configure:
   - **Name**: `pl-scout-hub-instance`
   - **Region**: Same as your workspace
   - **SQL Warehouse**: Create a new or select existing
4. Note the connection string and instance ID

### Step 5: Setup Lakebase Synced Tables & OLTP Tables

Run the second notebook:

```bash
databricks workspace import --language PYTHON notebooks/02_setup_lakebase_sync.py /Users/your-email/pl-scout-hub/02_setup_lakebase_sync
```

**What it does:**
- Documents how to sync Delta tables → Lakebase (read-only)
- Creates OLTP table schemas in Lakebase:
  - `scout_reports`
  - `player_watchlist`
  - `transfer_recommendations`
- Provides sample queries

**Execute the SQL sections in your Lakebase instance**:
- Log into your Lakebase instance
- Copy and run the CREATE TABLE statements from the notebook

### Step 6: Update App Configuration

Edit `app/db.py` with your Lakebase connection details:

```python
LAKEBASE_HOST = "your-instance.cloud.databricks.com"
LAKEBASE_PORT = 443
LAKEBASE_DB = "lakebase_pl_data"
LAKEBASE_USER = "user"
DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
```

### Step 7: Deploy Streamlit App (Local Testing)

Test locally before deploying to Databricks Apps:

```bash
cd app
pip install -r requirements.txt
streamlit run app.py
```

The app will open at `http://localhost:8501`

### Step 8: Deploy to Databricks Apps

Deploy the Streamlit app to your workspace:

```bash
databricks apps create --name pl-scout-hub --directory app/
```

Or upload and configure via the Databricks UI:
1. Go to **Apps**
2. Click **Create App**
3. Upload the `app/` directory
4. Configure the Lakebase resource in `app.yaml`
5. Deploy

---

## 🎮 Using the Application

### 📊 Page 1: Player Dashboard

**Purpose**: Browse and analyze player statistics using analytical data.

**Features:**
- Filter players by team, position, name
- Sort by goals, assists, xG, appearances, minutes
- View all 210 Premier League players
- **Charts:**
  - Top 10 goal scorers
  - Top 10 assist makers
  - Top 10 by xG
  - xG vs Goals scatter plot (shows over/underperformers)

**Data Source:** Synced Delta tables (OLAP)

**Use Case:** Scout is browsing available talent, identifying underperforming forwards with high xG.

---

### 📋 Page 2: Player Profile

**Purpose**: View detailed player stats and add scout reports.

**Features:**
- Select a player from dropdown
- View full season statistics
- Add a new scout report (form):
  - Overall rating (1-10)
  - Potential rating (1-10)
  - Recommendation: Sign / Watch / Pass
  - Strengths & weaknesses (free text)
- View all existing scout reports for that player

**Data Sources:**
- Synced stats (OLAP) for detailed view
- Scout reports written to Lakebase OLTP table

**Use Case:** After watching a match, scout files a detailed report on a player. The report is immediately visible in the Scout Board and Analytics Hub.

---

### 📊 Page 3: Scout Board

**Purpose**: Manage and review all scout reports across the team.

**Features:**
- View all scout reports with filters:
  - Team filter
  - Recommendation filter (sign/watch/pass)
  - Minimum rating threshold
- Summary statistics:
  - Total reports filed
  - Average overall rating
  - Recommendation distribution (pie chart)

**Data Source:** OLTP scout_reports table

**Use Case:** Team lead reviews all scout feedback to identify consensus recommendations.

---

### 🔍 Page 4: Analytics Hub

**Purpose**: Cross-layer analytics joining analytical + transactional data.

**Features:**
- **Tab 1: Scouted vs Unscouted**
  - Compare metrics for scouted vs non-scouted players
  - Average goals, assists, xG by scouting status
  - Find gaps in scouting coverage

- **Tab 2: Scout Ratings vs Performance**
  - Scatter plot: Scout ratings (OLTP) vs actual goals (OLAP)
  - See how well scouts predict performance

- **Tab 3: Team-Level Analysis**
  - Total goals and assists by team
  - Defensive stats aggregation

- **Tab 4: Opportunity Board**
  - High-performing players who haven't been scouted yet
  - Prioritize scouting work

**Data Source:** Joined OLAP (synced stats) + OLTP (scout reports) queries

**This is where Lakebase's value shines**: Single SQL query combines:
- Analytical facts (goals, xG, assists)
- Transactional operations (scout ratings, recommendations)

---

### ⚙️ Page 5: Settings

**Purpose**: View demo information and test database connection.

**Features:**
- Connection status indicator
- Test Lakebase connectivity
- Architecture overview
- Setup instructions
- Demo statistics

---

## 📈 Key Metrics & Features

### Real Player Data

The demo uses **real 2024/25 Premier League players** including:

- **Top Teams** (>10 players each):
  - Manchester City (Haaland, De Bruyne, Grealish, Foden)
  - Liverpool (Salah, van Dijk, Díaz)
  - Arsenal (Saka, Martinelli, Ødegaard, Rice)
  - Tottenham (Son, Maddison, Richarlison)
  - Chelsea (Palmer, Jackson)

- **All 20 PL Teams** represented
- **All Positions**: GK, DEF, MID, FWD
- **Realistic Statistics**:
  - Position-based stat distributions
  - xG/xA for forwards and midfielders
  - Defensive stats for defenders
  - Clean sheets for goalkeepers

### Advanced Metrics

- **Expected Goals (xG)**: Predicted goal-scoring chances
- **Expected Assists (xA)**: Predicted assist opportunities
- **Passes Completed**: Ball retention metric
- **Tackles & Interceptions**: Defensive effectiveness
- **Key Passes**: Creative play metric

### Data Volume

- **210 players** across 20 teams
- **~2000 match events** throughout season
- **200+ matches** represented
- **1968 individual events**: Goals, assists, yellow/red cards, substitutions

---

## 🏆 Why Lakebase Matters for This Use Case

### Traditional Approach (No Lakebase)

```
Problem: Scout files report → Write to operational DB
         Analytics team wants to analyze → Pulls from data warehouse
         Report not visible in analytics until ETL runs tomorrow

Result: Delayed insights, data inconsistency, complex pipelines
```

### Lakebase Approach

```
Scout files report → Write to Lakebase OLTP table (ACID guaranteed)
Analytics team queries → Joins OLAP synced table + OLTP table instantly
Report visible in dashboard immediately

Result: Real-time insights, single source of truth, no ETL!
```

### Benefits in This Demo

1. **Immediate Visibility**: Scout report appears in Analytics Hub within seconds
2. **Consistent Data**: One copy of player stats, not duplicated across systems
3. **No ETL**: No extract-transform-load pipelines, no batch jobs
4. **Fast Queries**: Synced tables cached in Lakebase (<10ms latency)
5. **ACID Transactions**: Scout data won't corrupt analytics
6. **Cost Efficient**: Shared infrastructure, one bill

---

## 📁 Project Structure

```
pl-scout-hub/
├── data/
│   ├── generate_data.py          # Data generation script
│   ├── players.csv               # Generated: 210 players
│   ├── player_season_stats.csv   # Generated: 2024/25 stats
│   └── match_events.csv          # Generated: ~2000 events
│
├── notebooks/
│   ├── 01_setup_delta_tables.py  # Create Delta tables in UC
│   └── 02_setup_lakebase_sync.py # Setup Lakebase sync & OLTP tables
│
├── app/
│   ├── app.py                    # Main Streamlit application
│   ├── db.py                     # Database connection module
│   ├── requirements.txt          # Python dependencies
│   └── app.yaml                  # Databricks Apps config
│
├── README.md                      # This file
└── .gitignore                    # Git ignore rules
```

---

## 🔧 Configuration

### Environment Variables

Set these before running the app:

```bash
export LAKEBASE_HOST="your-instance.cloud.databricks.com"
export LAKEBASE_PORT="443"
export LAKEBASE_DB="lakebase_pl_data"
export LAKEBASE_USER="user"
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"  # Or use SDK
```

### Database Connection Parameters

In `app/db.py`:

```python
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST", "your-instance.cloud.databricks.com")
LAKEBASE_PORT = int(os.getenv("LAKEBASE_PORT", "443"))
LAKEBASE_DB = os.getenv("LAKEBASE_DB", "lakebase_pl_data")
LAKEBASE_USER = os.getenv("LAKEBASE_USER", "user")
```

The app uses **OAuth tokens** from the Databricks SDK automatically in Databricks Apps environments.

---

## 🧪 Testing

### Test Data Generation

```bash
cd data
python generate_data.py
```

Verify files created:
```bash
ls -lh *.csv
# -rw-r--r-- players.csv (15 KB)
# -rw-r--r-- player_season_stats.csv (22 KB)
# -rw-r--r-- match_events.csv (150 KB)
```

### Test Database Connection

```python
# In Python REPL
from app import db
result = db.execute_query("SELECT COUNT(*) FROM lakebase.players")
print(result)  # Should show 210
```

### Test Streamlit App Locally

```bash
cd app
streamlit run app.py
```

Navigate to http://localhost:8501 and test each page.

---

## 📊 Sample Queries

### Query 1: Top Performers
```sql
SELECT
    p.name,
    p.team,
    s.goals,
    s.xg,
    ROUND(s.goals - s.xg, 2) as overperformance
FROM lakebase.players p
JOIN lakebase.player_season_stats s ON p.player_id = s.player_id
WHERE p.position = 'FWD'
ORDER BY s.goals DESC
LIMIT 10;
```

### Query 2: Scout Coverage
```sql
SELECT
    CASE WHEN sr.player_id IS NOT NULL THEN 'Scouted' ELSE 'Unscouted' END as status,
    COUNT(*) as player_count,
    ROUND(AVG(s.goals), 1) as avg_goals,
    ROUND(AVG(s.xg), 1) as avg_xg
FROM lakebase.players p
JOIN lakebase.player_season_stats s ON p.player_id = s.player_id
LEFT JOIN lakebase.scout_reports sr ON p.player_id = sr.player_id
GROUP BY 1
ORDER BY 2 DESC;
```

### Query 3: Signed Players Analysis
```sql
SELECT
    p.name,
    p.team,
    p.position,
    s.goals,
    s.assists,
    sr.overall_rating,
    sr.recommendation
FROM lakebase.scout_reports sr
JOIN lakebase.players p ON sr.player_id = p.player_id
JOIN lakebase.player_season_stats s ON p.player_id = s.player_id
WHERE sr.recommendation = 'sign'
ORDER BY sr.overall_rating DESC;
```

---

## 🧹 Cleanup

### Delete Lakebase Tables

```sql
DROP TABLE IF EXISTS lakebase_pl_data.scout_reports;
DROP TABLE IF EXISTS lakebase_pl_data.player_watchlist;
DROP TABLE IF EXISTS lakebase_pl_data.transfer_recommendations;
```

### Delete Delta Tables

```sql
DROP TABLE IF EXISTS workspace_catalog.pl_data.players;
DROP TABLE IF EXISTS workspace_catalog.pl_data.player_season_stats;
DROP TABLE IF EXISTS workspace_catalog.pl_data.match_events;
DROP SCHEMA IF EXISTS workspace_catalog.pl_data;
```

### Remove Lakebase Instance

In Databricks UI: **Data** → **Lakebase** → Delete instance

### Stop Streamlit App

Ctrl+C in terminal, or delete app from Databricks Apps UI.

---

## 📚 Learning Resources

- [Databricks Lakebase Documentation](https://docs.databricks.com/en/lakebase/)
- [Unity Catalog](https://docs.databricks.com/en/catalogs/index.html)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Delta Lake](https://delta.io/)
- [Premier League Statistics](https://www.premierleague.com/)

---

## 🤝 Contributing

This is a workshop demo. Improvements welcome!

- Add more advanced metrics
- Implement player comparison radar charts
- Add historical trend analysis
- Expand to other sports leagues
- Performance optimizations

---

## 📄 License

This project is provided as-is for educational purposes.

---

## ✉️ Support

For Databricks-specific questions, refer to:
- [Databricks Community](https://community.databricks.com/)
- [Databricks Documentation](https://docs.databricks.com/)

For Streamlit questions:
- [Streamlit Community](https://discuss.streamlit.io/)

---

## 🎓 Key Takeaways

### What This Demo Teaches

1. **Lakebase Architecture**: OLAP + OLTP unified
2. **Real-World Use Case**: Scouting operations
3. **SQL Across Layers**: Join analytical + transactional in one query
4. **Fast Reads**: Synced tables for app performance
5. **ACID Writes**: Guaranteed consistency for transactions
6. **No ETL Complexity**: Direct analytics on operational data

### The Lakebase Promise

> One platform for analytics and transactions. No ETL. No data silos. Real-time insights.

This demo proves it works.

---

**Last Updated**: March 2024
**Demo Version**: 1.0
**Databricks Lakebase**: v2024.03+

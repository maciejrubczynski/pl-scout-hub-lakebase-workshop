"""
PL Live Scout Hub - Streamlit Application
A Databricks Apps demo showcasing Lakebase's OLTP+OLAP bridge for scouting operations.

This app demonstrates the unique value of Lakebase:
- Read analytical data from Delta Lake (synced tables) with <10ms latency
- Write transactional scout data directly to Lakebase OLTP tables
- Query both layers together in real-time
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import db
from typing import Optional, List, Dict

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="PL Live Scout Hub",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for professional styling
st.markdown("""
    <style>
    .main {
        padding-top: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .highlight {
        background-color: #FFF9E6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .stat-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

if "db_connection" not in st.session_state:
    st.session_state.db_connection = None

if "selected_player" not in st.session_state:
    st.session_state.selected_player = None

# ============================================================================
# SIDEBAR NAVIGATION
# ============================================================================

st.sidebar.title("⚽ PL SCOUT HUB")
st.sidebar.write("*Powered by Lakebase: OLTP+OLAP Bridge*")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation",
    [
        "🏆 Player Dashboard",
        "📊 Player Profile",
        "📋 Scout Board",
        "🔍 Analytics Hub",
        "⚙️ Settings",
    ],
)

st.sidebar.divider()
st.sidebar.info(
    """
    **About This Demo:**
    - **Analytical Data (OLAP)**: Player stats from Delta Lake (synced via Lakebase)
    - **Transactional Data (OLTP)**: Scout reports written directly to Lakebase
    - **Query Both**: Single SQL query joins stats + scout data

    This showcases Lakebase's unique value: one platform for analytics & transactions.
    """
)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@st.cache_data
def load_players_data() -> pd.DataFrame:
    """Load players from Lakebase synced table."""
    query = """
    SELECT
        player_id,
        name,
        team,
        position,
        nationality,
        age,
        market_value_eur
    FROM pl_data.players_synced
    ORDER BY name
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Failed to load players: {e}")
        return pd.DataFrame()

@st.cache_data
def load_season_stats() -> pd.DataFrame:
    """Load season stats from Lakebase synced table."""
    query = """
    SELECT
        player_id,
        season,
        appearances,
        goals,
        assists,
        yellow_cards,
        red_cards,
        minutes_played,
        clean_sheets,
        passes_completed,
        tackles_won,
        interceptions,
        shots_on_target,
        key_passes,
        xg,
        xa
    FROM pl_data.player_season_stats_synced
    WHERE season = '2024/25'
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Failed to load season stats: {e}")
        return pd.DataFrame()

@st.cache_data
def load_match_events() -> pd.DataFrame:
    """Load match events from Lakebase synced table."""
    query = """
    SELECT
        event_id,
        match_id,
        player_id,
        event_type,
        minute,
        match_date,
        home_team,
        away_team,
        home_score,
        away_score
    FROM pl_data.match_events_synced
    ORDER BY match_date DESC
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Failed to load match events: {e}")
        return pd.DataFrame()

def get_player_profile(player_id: int) -> Optional[pd.Series]:
    """Get detailed profile for a player."""
    query = """
    SELECT
        p.player_id,
        p.name,
        p.team,
        p.position,
        p.nationality,
        p.age,
        p.market_value_eur,
        s.appearances,
        s.goals,
        s.assists,
        s.yellow_cards,
        s.red_cards,
        s.minutes_played,
        s.clean_sheets,
        s.passes_completed,
        s.tackles_won,
        s.interceptions,
        s.shots_on_target,
        s.key_passes,
        s.xg,
        s.xa
    FROM pl_data.players_synced p
    LEFT JOIN pl_data.player_season_stats_synced s ON p.player_id = s.player_id
    WHERE p.player_id = %s
    LIMIT 1
    """
    try:
        df = db.execute_query(query, (player_id,))
        return df.iloc[0] if len(df) > 0 else None
    except Exception as e:
        st.error(f"Failed to load player profile: {e}")
        return None

def get_scout_reports(player_id: Optional[int] = None) -> pd.DataFrame:
    """Get scout reports (OLTP data from Lakebase)."""
    if player_id:
        query = """
        SELECT
            report_id,
            player_id,
            scout_name,
            report_date,
            overall_rating,
            technical_rating,
            physical_rating,
            mental_rating,
            potential_rating,
            strengths,
            weaknesses,
            recommendation,
            notes
        FROM scout.scout_reports
        WHERE player_id = %s
        ORDER BY report_date DESC
        """
        params = (player_id,)
    else:
        query = """
        SELECT
            sr.report_id,
            sr.player_id,
            p.name,
            p.team,
            sr.scout_name,
            sr.report_date,
            sr.overall_rating,
            sr.potential_rating,
            sr.recommendation
        FROM scout.scout_reports sr
        JOIN pl_data.players_synced p ON sr.player_id = p.player_id
        ORDER BY sr.report_date DESC
        """
        params = None
    try:
        return db.execute_query(query, params)
    except Exception as e:
        st.warning(f"Could not load scout reports: {e}")
        return pd.DataFrame()

def save_scout_report(player_id: int, scout_name: str, overall_rating: int,
                      potential_rating: int, technical_rating: int,
                      physical_rating: int, mental_rating: int,
                      strengths: str, weaknesses: str,
                      recommendation: str, notes: str = "") -> bool:
    """Save a scout report to Lakebase OLTP table (SERIAL PK auto-generated)."""
    insert_query = """
    INSERT INTO scout.scout_reports
    (player_id, scout_name, report_date, overall_rating, technical_rating,
     physical_rating, mental_rating, potential_rating,
     strengths, weaknesses, recommendation, notes)
    VALUES
    (%s, %s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (str(player_id), scout_name, overall_rating, technical_rating,
              physical_rating, mental_rating, potential_rating,
              strengths, weaknesses, recommendation, notes)
    try:
        db.execute_write(insert_query, params)
        return True
    except Exception as e:
        st.error(f"Failed to save scout report: {e}")
        return False

# ============================================================================
# PAGE 1: PLAYER DASHBOARD
# ============================================================================

if page == "🏆 Player Dashboard":
    st.title("⚽ Player Dashboard")
    st.write("*Analytical data from Delta Lake, synced via Lakebase*")

    # Load data
    players_df = load_players_data()
    stats_df = load_season_stats()

    if len(players_df) > 0 and len(stats_df) > 0:
        # Merge data
        dashboard_df = players_df.merge(stats_df, on="player_id", how="left")

        # Sidebar filters
        col1, col2 = st.columns(2)
        with col1:
            selected_team = st.multiselect(
                "Filter by Team",
                options=sorted(dashboard_df["team"].unique()),
                default=None,
            )
        with col2:
            selected_position = st.multiselect(
                "Filter by Position",
                options=sorted(dashboard_df["position"].unique()),
                default=None,
            )

        # Search by name
        search_name = st.text_input("Search by Player Name", "")

        # Apply filters
        filtered_df = dashboard_df.copy()
        if selected_team:
            filtered_df = filtered_df[filtered_df["team"].isin(selected_team)]
        if selected_position:
            filtered_df = filtered_df[filtered_df["position"].isin(selected_position)]
        if search_name:
            filtered_df = filtered_df[
                filtered_df["name"].str.contains(search_name, case=False)
            ]

        # Sort options
        col1, col2, col3 = st.columns(3)
        with col1:
            sort_by = st.selectbox(
                "Sort by",
                ["Goals", "Assists", "xG", "Appearances", "Minutes Played"],
            )
        with col2:
            sort_order = st.radio("Order", ["Descending", "Ascending"])
        with col3:
            limit = st.number_input("Limit results", 5, 200, 20)

        # Apply sorting
        sort_column_map = {
            "Goals": "goals",
            "Assists": "assists",
            "xG": "xg",
            "Appearances": "appearances",
            "Minutes Played": "minutes_played",
        }
        sort_col = sort_column_map[sort_by]
        filtered_df = filtered_df.sort_values(
            sort_col, ascending=(sort_order == "Ascending")
        ).head(limit)

        # Display table
        st.subheader("Player Statistics (2024/25)")
        display_cols = [
            "name", "team", "position", "age", "goals", "assists", "xg",
            "appearances", "yellow_cards", "shots_on_target", "key_passes"
        ]
        st.dataframe(
            filtered_df[display_cols].rename(columns={
                "name": "Player",
                "team": "Team",
                "position": "Position",
                "age": "Age",
                "goals": "Goals",
                "assists": "Assists",
                "xg": "xG",
                "appearances": "Apps",
                "yellow_cards": "YC",
                "shots_on_target": "SoT",
                "key_passes": "KP",
            }),
            use_container_width=True,
            height=500,
        )

        # Top performers charts
        st.divider()
        col1, col2, col3 = st.columns(3)

        with col1:
            st.subheader("🎯 Top 10 Goal Scorers")
            top_goals = (
                filtered_df.nlargest(10, "goals")[["name", "goals"]]
                .reset_index(drop=True)
            )
            fig = px.bar(
                top_goals,
                x="goals",
                y="name",
                orientation="h",
                color="goals",
                color_continuous_scale="Blues",
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🍎 Top 10 Assist Makers")
            top_assists = (
                filtered_df.nlargest(10, "assists")[["name", "assists"]]
                .reset_index(drop=True)
            )
            fig = px.bar(
                top_assists,
                x="assists",
                y="name",
                orientation="h",
                color="assists",
                color_continuous_scale="Greens",
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            st.subheader("⚡ Top 10 by xG")
            top_xg = (
                filtered_df.nlargest(10, "xg")[["name", "xg"]]
                .reset_index(drop=True)
            )
            fig = px.bar(
                top_xg,
                x="xg",
                y="name",
                orientation="h",
                color="xg",
                color_continuous_scale="Reds",
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        # xG vs Goals scatter
        st.divider()
        st.subheader("⚽ Expected Goals vs Actual Goals (Forwards)")
        forwards_df = filtered_df[filtered_df["position"] == "FWD"].copy()
        if len(forwards_df) > 5:
            forwards_df["goal_variance"] = forwards_df["goals"] - forwards_df["xg"]
            forwards_df["variance_type"] = forwards_df["goal_variance"].apply(
                lambda x: "Overperformer" if x > 1 else ("Underperformer" if x < -1 else "Expected")
            )

            fig = px.scatter(
                forwards_df,
                x="xg",
                y="goals",
                hover_name="name",
                hover_data=["team", "appearances"],
                color="variance_type",
                color_discrete_map={
                    "Overperformer": "#2ecc71",
                    "Expected": "#95a5a6",
                    "Underperformer": "#e74c3c",
                },
                size="appearances",
                labels={"xg": "Expected Goals (xG)", "goals": "Actual Goals"},
            )
            fig.add_shape(
                type="line",
                x0=0,
                y0=0,
                x1=forwards_df["xg"].max(),
                y1=forwards_df["xg"].max(),
                line=dict(dash="dash", color="gray", width=1),
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

            st.write("*Dashed line indicates perfect xG/Goals correlation*")

    else:
        st.error("Could not load player data. Check database connection.")

# ============================================================================
# PAGE 2: PLAYER PROFILE
# ============================================================================

elif page == "📊 Player Profile":
    st.title("📊 Player Profile & Scouting")
    st.write("*Combine analytical stats (Delta) with transactional scout data (OLTP)*")

    players_df = load_players_data()

    if len(players_df) > 0:
        # Player selection
        selected_player_name = st.selectbox(
            "Select Player",
            options=sorted(players_df["name"].unique()),
        )

        selected_player_id = players_df[
            players_df["name"] == selected_player_name
        ]["player_id"].values[0]

        # Load player profile
        profile = get_player_profile(selected_player_id)

        if profile is not None:
            # Header with player info
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Team", profile["team"])
            with col2:
                st.metric("Position", profile["position"])
            with col3:
                st.metric("Age", int(profile["age"]))
            with col4:
                market_value_m = profile["market_value_eur"] / 1_000_000
                st.metric("Market Value", f"€{market_value_m:.1f}M")

            st.divider()

            # Stats grid
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("⚽ Goals", int(profile["goals"]) if pd.notna(profile["goals"]) else 0)
            with col2:
                st.metric("🍎 Assists", int(profile["assists"]) if pd.notna(profile["assists"]) else 0)
            with col3:
                st.metric("⚡ xG", round(profile["xg"], 2) if pd.notna(profile["xg"]) else 0)
            with col4:
                st.metric("📊 Apps", int(profile["appearances"]) if pd.notna(profile["appearances"]) else 0)
            with col5:
                st.metric("⏱️ Minutes", int(profile["minutes_played"]) if pd.notna(profile["minutes_played"]) else 0)

            st.divider()

            # Detailed stats table
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Offensive Stats")
                offensive_stats = {
                    "Shots on Target": int(profile["shots_on_target"]) if pd.notna(profile["shots_on_target"]) else 0,
                    "Key Passes": int(profile["key_passes"]) if pd.notna(profile["key_passes"]) else 0,
                    "xG": round(profile["xg"], 2) if pd.notna(profile["xg"]) else 0,
                    "xA": round(profile["xa"], 2) if pd.notna(profile["xa"]) else 0,
                }
                for stat, value in offensive_stats.items():
                    st.write(f"**{stat}**: {value}")

            with col2:
                st.subheader("Defensive Stats")
                defensive_stats = {
                    "Tackles": int(profile["tackles_won"]) if pd.notna(profile["tackles_won"]) else 0,
                    "Interceptions": int(profile["interceptions"]) if pd.notna(profile["interceptions"]) else 0,
                    "Clean Sheets": int(profile["clean_sheets"]) if pd.notna(profile["clean_sheets"]) else 0,
                    "Yellow Cards": int(profile["yellow_cards"]) if pd.notna(profile["yellow_cards"]) else 0,
                }
                for stat, value in defensive_stats.items():
                    st.write(f"**{stat}**: {value}")

            st.divider()

            # Scout Report Section
            st.subheader("📝 Add Scout Report (OLTP Write to Lakebase)")

            with st.form(f"scout_report_form_{selected_player_id}"):
                col1, col2 = st.columns(2)
                with col1:
                    scout_name = st.text_input("Scout Name", value="John Smith")
                with col2:
                    recommendation = st.selectbox(
                        "Recommendation",
                        ["Sign", "Monitor", "Pass", "Loan"],
                    )

                st.write("**Ratings** (1-10)")
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    overall_rating = st.slider("Overall", 1, 10, 7)
                with col2:
                    technical_rating = st.slider("Technical", 1, 10, 7)
                with col3:
                    physical_rating = st.slider("Physical", 1, 10, 7)
                with col4:
                    mental_rating = st.slider("Mental", 1, 10, 7)
                with col5:
                    potential_rating = st.slider("Potential", 1, 10, 8)

                strengths = st.text_area(
                    "Strengths",
                    placeholder="e.g., Excellent positioning, strong left foot, good game reading",
                    height=80,
                )

                weaknesses = st.text_area(
                    "Weaknesses",
                    placeholder="e.g., Occasional lapses in concentration, could improve pace recovery",
                    height=80,
                )

                notes = st.text_area("Additional Notes", placeholder="Any other observations...", height=60)

                submitted = st.form_submit_button(
                    "💾 Save Scout Report",
                    use_container_width=True,
                )

                if submitted:
                    if strengths and weaknesses:
                        success = save_scout_report(
                            player_id=selected_player_id,
                            scout_name=scout_name,
                            overall_rating=overall_rating,
                            potential_rating=potential_rating,
                            technical_rating=technical_rating,
                            physical_rating=physical_rating,
                            mental_rating=mental_rating,
                            strengths=strengths,
                            weaknesses=weaknesses,
                            recommendation=recommendation,
                            notes=notes,
                        )
                        if success:
                            st.success("✅ Scout report saved to Lakebase!")
                        else:
                            st.error("Failed to save scout report")
                    else:
                        st.warning("Please fill in both strengths and weaknesses")

            st.divider()

            # Existing Scout Reports
            st.subheader("📋 Existing Scout Reports for This Player")
            reports = get_scout_reports(selected_player_id)

            if len(reports) > 0:
                for idx, report in reports.iterrows():
                    with st.expander(
                        f"Scout: {report['scout_name']} | Rating: {report['overall_rating']}/10 | Date: {report['report_date']}"
                    ):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Overall Rating", f"{report['overall_rating']}/10")
                        with col2:
                            st.metric("Potential", f"{report['potential_rating']}/10")
                        with col3:
                            st.metric("Recommendation", report['recommendation'].upper())

                        st.write("**Strengths:**")
                        st.write(report['strengths'])

                        st.write("**Weaknesses:**")
                        st.write(report['weaknesses'])
            else:
                st.info("No scout reports yet. Be the first to scout this player!")

    else:
        st.error("Could not load player list")

# ============================================================================
# PAGE 3: SCOUT BOARD
# ============================================================================

elif page == "📋 Scout Board":
    st.title("📋 Scout Board")
    st.write("*Transactional scouting data stored in Lakebase OLTP tables*")

    # Load scout reports
    reports = get_scout_reports()

    if len(reports) > 0:
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            filter_team = st.multiselect(
                "Filter by Team",
                options=sorted(reports["team"].unique()) if "team" in reports.columns else [],
            )
        with col2:
            filter_recommendation = st.multiselect(
                "Filter by Recommendation",
                options=["sign", "watch", "pass"],
            )
        with col3:
            filter_min_rating = st.slider(
                "Minimum Overall Rating",
                1, 10, 1,
            )

        # Apply filters
        filtered_reports = reports.copy()
        if filter_team and "team" in filtered_reports.columns:
            filtered_reports = filtered_reports[filtered_reports["team"].isin(filter_team)]
        if filter_recommendation:
            filtered_reports = filtered_reports[
                filtered_reports["recommendation"].isin(filter_recommendation)
            ]
        if "overall_rating" in filtered_reports.columns:
            filtered_reports = filtered_reports[
                filtered_reports["overall_rating"] >= filter_min_rating
            ]

        # Display table
        st.subheader(f"Scout Reports ({len(filtered_reports)} total)")
        display_cols = [col for col in [
            "name", "team", "scout_name", "report_date", "overall_rating",
            "potential_rating", "recommendation"
        ] if col in filtered_reports.columns]

        st.dataframe(
            filtered_reports[display_cols].rename(columns={
                "name": "Player",
                "team": "Team",
                "scout_name": "Scout",
                "report_date": "Date",
                "overall_rating": "Rating",
                "potential_rating": "Potential",
                "recommendation": "Recommendation",
            }),
            use_container_width=True,
        )

        st.divider()

        # Summary stats
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Reports", len(filtered_reports))
        with col2:
            if "overall_rating" in filtered_reports.columns:
                st.metric(
                    "Avg Overall Rating",
                    f"{filtered_reports['overall_rating'].mean():.1f}/10",
                )
        with col3:
            sign_count = len(
                filtered_reports[filtered_reports["recommendation"].str.lower() == "sign"]
            )
            st.metric("'Sign' Recommendations", sign_count)
        with col4:
            monitor_count = len(
                filtered_reports[filtered_reports["recommendation"].str.lower() == "monitor"]
            )
            st.metric("'Monitor' Recommendations", monitor_count)

        # Recommendation distribution
        st.subheader("Recommendation Distribution")
        if "recommendation" in filtered_reports.columns:
            rec_counts = filtered_reports["recommendation"].value_counts()
            fig = px.pie(
                values=rec_counts.values,
                names=rec_counts.index,
                color_discrete_map={"Sign": "#2ecc71", "Monitor": "#f39c12", "Pass": "#e74c3c", "Loan": "#3498db"},
            )
            st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("No scout reports yet. Create one from the Player Profile page!")

# ============================================================================
# PAGE 4: ANALYTICS HUB
# ============================================================================

elif page == "🔍 Analytics Hub":
    st.title("🔍 Analytics Hub")
    st.write("*Demonstrates Lakebase's power: analytical + transactional data together*")

    players_df = load_players_data()
    stats_df = load_season_stats()
    scout_reports = get_scout_reports()

    if len(players_df) > 0 and len(stats_df) > 0:
        dashboard_df = players_df.merge(stats_df, on="player_id", how="left")

        st.markdown("""
        <div class="highlight">
        <strong>✨ The Lakebase Advantage:</strong><br>
        This page queries data from two different table types in one dashboard:
        <ul>
        <li><strong>OLAP (Synced Tables):</strong> Players & season stats from Delta Lake</li>
        <li><strong>OLTP (Write Tables):</strong> Scout reports written by the app</li>
        </ul>
        In most systems, this would require complex ETL. With Lakebase, it's a single SQL query!
        </div>
        """, unsafe_allow_html=True)

        st.divider()

        # Tab 1: Scouted vs Unscouted
        tab1, tab2, tab3, tab4 = st.tabs([
            "Scouted vs Unscouted",
            "Scout Ratings vs Performance",
            "Team Analysis",
            "Opportunity Board",
        ])

        with tab1:
            st.subheader("Scouted vs Unscouted Comparison")

            if len(scout_reports) > 0:
                scouted_players = set(scout_reports["player_id"].unique())
                dashboard_df["scouted"] = dashboard_df["player_id"].isin(scouted_players)

                comparison = dashboard_df.groupby("scouted").agg({
                    "goals": "mean",
                    "assists": "mean",
                    "xg": "mean",
                    "appearances": "mean",
                    "player_id": "count",
                }).rename(columns={"player_id": "player_count"})

                comparison.index = comparison.index.map({
                    True: "Scouted", False: "Unscouted"
                })

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Scouted Players", int(comparison.loc["Scouted", "player_count"] if "Scouted" in comparison.index else 0))
                with col2:
                    st.metric("Unscouted Players", int(comparison.loc["Unscouted", "player_count"] if "Unscouted" in comparison.index else 0))

                st.write("\n**Average Statistics by Scouting Status:**")
                st.dataframe(comparison.round(2), use_container_width=True)

                # Visualization
                metrics = ["goals", "assists", "xg"]
                fig = go.Figure()

                for metric in metrics:
                    fig.add_trace(
                        go.Bar(
                            name=metric.upper(),
                            x=comparison.index,
                            y=comparison[metric],
                        )
                    )

                fig.update_layout(
                    barmode="group",
                    height=400,
                    title="Average Stats: Scouted vs Unscouted",
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No scout reports yet. Start scouting to see comparisons!")

        with tab2:
            st.subheader("Scout Ratings vs Player Performance")

            if len(scout_reports) > 0:
                # Get latest scout rating for each player
                latest_reports = (
                    scout_reports.sort_values("report_date")
                    .groupby("player_id")
                    .tail(1)
                )

                comparison_df = latest_reports.merge(
                    dashboard_df[["player_id", "goals", "xg", "name", "position"]],
                    on="player_id",
                    how="left",
                )

                if "overall_rating" in comparison_df.columns and len(comparison_df) > 0:
                    fig = px.scatter(
                        comparison_df,
                        x="overall_rating",
                        y="goals",
                        hover_name="name",
                        hover_data=["position", "xg"],
                        size="xg",
                        title="Scout Rating vs Actual Goals",
                        labels={
                            "overall_rating": "Scout Overall Rating (1-10)",
                            "goals": "Actual Goals Scored",
                        },
                    )
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Create scout reports to see this analysis!")

        with tab3:
            st.subheader("Team-Level Analysis")

            team_stats = dashboard_df.groupby("team").agg({
                "goals": "sum",
                "assists": "sum",
                "player_id": "count",
                "yellow_cards": "sum",
            }).rename(columns={"player_id": "players"})

            team_stats = team_stats.sort_values("goals", ascending=False)

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Total Goals by Team")
                fig = px.bar(
                    team_stats.reset_index(),
                    x="team",
                    y="goals",
                    color="goals",
                    color_continuous_scale="Blues",
                )
                fig.update_layout(xaxis_tickangle=-45, height=400)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.subheader("Total Assists by Team")
                fig = px.bar(
                    team_stats.reset_index(),
                    x="team",
                    y="assists",
                    color="assists",
                    color_continuous_scale="Greens",
                )
                fig.update_layout(xaxis_tickangle=-45, height=400)
                st.plotly_chart(fig, use_container_width=True)

        with tab4:
            st.subheader("Opportunity Board")
            st.write("*High-performing but unscored players - Scouting opportunities*")

            if len(scout_reports) > 0:
                scouted_players = set(scout_reports["player_id"].unique())
                unscored_high_performers = dashboard_df[
                    (~dashboard_df["player_id"].isin(scouted_players)) &
                    (dashboard_df["goals"] >= 5) |
                    (dashboard_df["assists"] >= 3)
                ].nlargest(20, "goals")

                if len(unscored_high_performers) > 0:
                    st.dataframe(
                        unscored_high_performers[[
                            "name", "team", "position", "goals", "assists", "xg"
                        ]].rename(columns={
                            "name": "Player",
                            "team": "Team",
                            "position": "Position",
                            "goals": "Goals",
                            "assists": "Assists",
                            "xg": "xG",
                        }),
                        use_container_width=True,
                    )
                else:
                    st.info("All high performers have been scouted!")
            else:
                st.info("Start scouting to unlock the Opportunity Board!")

# ============================================================================
# PAGE 5: SETTINGS
# ============================================================================

elif page == "⚙️ Settings":
    st.title("⚙️ Settings & Information")

    st.subheader("🔌 Database Connection")

    col1, col2 = st.columns(2)
    with col1:
        st.write("**Lakebase Connection Status:**")
        try:
            conn = db.get_connection()
            if conn:
                st.success("✅ Connected to Lakebase")
            else:
                st.error("❌ Not connected")
        except Exception as e:
            st.error(f"❌ Connection error: {e}")

    with col2:
        if st.button("🔄 Test Connection"):
            try:
                test_query = "SELECT COUNT(*) as count FROM pl_data.players_synced"
                result = db.execute_query(test_query)
                st.success(f"✅ Connection working! Found {result.iloc[0]['count']} players.")
            except Exception as e:
                st.error(f"❌ Connection test failed: {e}")

    st.divider()

    st.subheader("📖 About This Demo")

    st.markdown("""
    ## PL Live Scout Hub

    **Purpose**: Demonstrate Databricks Lakebase's unique value as an OLTP+OLAP bridge

    ### Architecture

    **Analytical Layer (OLAP)**
    - Delta Lake tables in Unity Catalog
    - Player stats, match events, historical data
    - Read-only synced to Lakebase
    - Sub-10ms latency via Lakebase caching

    **Transactional Layer (OLTP)**
    - Scout reports, watchlists, transfer recommendations
    - Written directly by the Streamlit app
    - ACID guarantees
    - Real-time availability

    **The Bridge**
    - Single SQL query joins OLAP + OLTP data
    - No ETL pipelines needed
    - Real-time analytics on transactional data

    ### Data Tables

    **Synced (from Delta Lake):**
    - `pl_data.players_synced` (210 real Premier League players)
    - `pl_data.player_season_stats_synced` (2024/25 season stats)
    - `pl_data.match_events_synced` (~2000 match events)

    **OLTP (App-written):**
    - `scout.scout_reports` (Scout observations & ratings)
    - `scout.player_watchlist` (Player monitoring list)
    - `scout.transfer_recommendations` (Transfer suggestions)

    ### Key Features

    1. **Player Dashboard**: Filter, sort, and analyze players using synced OLAP data
    2. **Player Profile**: View detailed stats + add scout reports (OLTP writes)
    3. **Scout Board**: Manage all scout reports across the scouting team
    4. **Analytics Hub**: Cross-layer queries combining analytical + transactional data
    5. **Real-time**: All scout data visible immediately in analytics

    ### Real Player Data

    This demo uses real 2024/25 Premier League players including:
    - Haaland, Salah, Saka, Palmer, Rice, Akanji, van Dijk, Mount
    - All 20 teams with 10-11 players each
    - Realistic stats for the season

    ### Why Lakebase?

    - **No Duplicates**: One source of truth, no data sync nightmares
    - **One Query Language**: SQL works for analytics and transactions
    - **Guaranteed ACID**: Transactions won't corrupt your analytics
    - **<10ms Latency**: Synced tables are fast for app reads
    - **Cost Efficient**: Shared infrastructure, pay once
    """)

    st.divider()

    st.subheader("🚀 Getting Started")

    with st.expander("1. Setup Instructions"):
        st.markdown("""
        1. **Create Delta tables** with `01_setup_delta_tables.py`
        2. **Create a Lakebase instance** in your workspace
        3. **Setup synced tables** with `02_setup_lakebase_sync.py`
        4. **Deploy this app** using Databricks Apps
        5. **Start scouting!**
        """)

    with st.expander("2. Connection Setup"):
        st.markdown("""
        Update `app/db.py` with your Lakebase credentials:

        ```python
        LAKEBASE_HOST = "your-instance.cloud.databricks.com"
        LAKEBASE_PORT = 443
        LAKEBASE_DB = "lakebase_pl_data"
        LAKEBASE_ENDPOINT_ID = "your-endpoint-id"
        ```

        Use Databricks SDK to retrieve your OAuth token automatically.
        """)

    with st.expander("3. Data Refresh"):
        st.markdown("""
        - **Synced Tables**: Auto-refresh based on your Delta table changes
        - **OLTP Tables**: Updates visible immediately
        - **Scout Data**: Syncs back to Delta Lake for analytics overnight
        """)

    st.divider()

    st.subheader("📊 Demo Statistics")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Players", "210")
    with col2:
        st.metric("PL Teams", "20")
    with col3:
        st.metric("Match Events", "~2000")
    with col4:
        st.metric("Season", "2024/25")

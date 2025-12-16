# Main_Project/app/dashboard.py
import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path

# Paths (simple + robust)
PROJECT_DIR = Path(__file__).resolve().parents[1]      # .../Main_Project
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

FACT_PATH = PROCESSED_DIR / "fact_player_game.parquet"
TRENDS_PATH = PROCESSED_DIR / "mart_player_trends.parquet"
PLAYERS_PATH = PROCESSED_DIR / "dim_players.parquet"


@st.cache_data
def load_data():
    fact = pd.read_parquet(FACT_PATH)
    trends = pd.read_parquet(TRENDS_PATH)
    players = pd.read_parquet(PLAYERS_PATH)
    return fact, trends, players


def main():
    st.set_page_config(page_title="NBA Player Dashboard", layout="wide")
    st.title("NBA Player Performance Dashboard (2023–24 & 2024–25)")

    st.caption(
    "This dashboard visualizes NBA player performance using a reproducible "
    )

    # Load
    if not FACT_PATH.exists() or not TRENDS_PATH.exists() or not PLAYERS_PATH.exists():
        st.error("Processed data not found. Run: python Main_Project/app/pipeline.py")
        st.stop()

    fact, trends, players = load_data()

    #ensure datetime
    if "game_date" in fact.columns:
        fact["game_date"] = pd.to_datetime(fact["game_date"], errors="coerce")
    if "game_date" in trends.columns:
        trends["game_date"] = pd.to_datetime(trends["game_date"], errors="coerce")

    #sidebar filters
    st.sidebar.header("Filters")

    player_options = sorted(players["player_name"].unique().tolist())
    default_player = player_options[0] if player_options else None
    selected_player = st.sidebar.selectbox("Player", player_options, index=0)

    season_options = sorted(fact["season"].dropna().unique().tolist())
    selected_seasons = st.sidebar.multiselect("Seasons", season_options, default=season_options)

    metric_map = {
        "Points (PTS)": "pts",
        "Assists (AST)": "ast",
        "Rebounds (REB)": "reb",
        "Steals (STL)": "stl",
        "Blocks (BLK)": "blk",
        "Turnovers (TOV)": "tov",
        "Minutes (MIN)": "min",
        "Plus/Minus": "plus_minus",
    }
    metric_label = st.sidebar.selectbox("Metric", list(metric_map.keys()), index=0)
    metric = metric_map[metric_label]

    use_rolling = st.sidebar.checkbox("Show rolling 10-game average (if available)", value=True)

    #filter data
    fact_f = fact[(fact["player_name"] == selected_player) & (fact["season"].isin(selected_seasons))].copy()
    trends_f = trends[(trends["player_name"] == selected_player) & (trends["season"].isin(selected_seasons))].copy()

    #date range (based on available)
    if not fact_f.empty and "game_date" in fact_f.columns:
        min_d = fact_f["game_date"].min()
        max_d = fact_f["game_date"].max()
        date_range = st.sidebar.date_input("Date range", (min_d.date(), max_d.date()))
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        fact_f = fact_f[(fact_f["game_date"] >= start) & (fact_f["game_date"] <= end)]
        trends_f = trends_f[(trends_f["game_date"] >= start) & (trends_f["game_date"] <= end)]

    #tabs
    tab1, tab2, tab3 = st.tabs(["Player Trends", "Season Summary", "Recent Form"])

    #TAB 1: Trends
    with tab1:
        st.subheader(f"{selected_player}: {metric_label} over time")

        if fact_f.empty:
            st.warning("No data for current filters.")
        else:
            fact_f = fact_f.sort_values("game_date")

            fig = px.line(
                fact_f,
                x="game_date",
                y=metric if metric in fact_f.columns else None,
                hover_data=[c for c in ["season", "matchup", "wl", "min", "pts", "reb", "ast"] if c in fact_f.columns],
            )
            st.plotly_chart(fig, use_container_width=True)

            #rolling average overlay
            roll_col = f"{metric}_roll10"
            if use_rolling and roll_col in trends_f.columns:
                trends_f = trends_f.sort_values("game_date")
                fig2 = px.line(trends_f, x="game_date", y=roll_col)
                st.caption("Rolling average (10 games)")
                st.plotly_chart(fig2, use_container_width=True)

        st.markdown("---")
        st.subheader("Raw game log (filtered)")
        st.dataframe(fact_f, use_container_width=True, height=350)

    #TAB 2:Season Summary
    with tab2:
        st.subheader("Per-season averages (selected player)")

        if fact_f.empty:
            st.warning("No data for current filters.")
        else:
            cols = [c for c in ["pts", "reb", "ast", "stl", "blk", "tov", "min", "plus_minus"] if c in fact.columns]
            season_summary = (
                fact_f.groupby("season")[cols]
                .mean(numeric_only=True)
                .round(2)
                .reset_index()
            )
            st.dataframe(season_summary, use_container_width=True)

        st.markdown("---")
        st.subheader("Leaderboard (selected seasons)")
        #League table restricted to your 10 players since thats what i wanted to look into first for this project
        league_f = fact[fact["season"].isin(selected_seasons)].copy()
        cols = [c for c in ["pts", "reb", "ast", "stl", "blk", "tov", "min"] if c in league_f.columns]
        leaderboard = (
            league_f.groupby(["player_name", "season"])[cols]
            .mean(numeric_only=True)
            .round(2)
            .reset_index()
            .sort_values(["season", "pts"], ascending=[True, False])
        )
        st.dataframe(leaderboard, use_container_width=True, height=450)

    #TAB 3: Recent Form
    with tab3:
        st.subheader("Last 10 games vs season average")

        if fact_f.empty or "game_date" not in fact_f.columns or metric not in fact_f.columns:
            st.warning("Not enough data for recent-form view.")
        else:
            df = fact_f.sort_values("game_date").copy()
            last10 = df.tail(10)

            season_avg = df[metric].mean()
            last10_avg = last10[metric].mean()

            c1, c2, c3 = st.columns(3)
            c1.metric("Season avg", f"{season_avg:.2f}")
            c2.metric("Last 10 avg", f"{last10_avg:.2f}")
            c3.metric("Delta", f"{(last10_avg - season_avg):.2f}")

            fig = px.bar(last10, x="game_date", y=metric, hover_data=[c for c in ["matchup", "wl"] if c in last10.columns])
            st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()

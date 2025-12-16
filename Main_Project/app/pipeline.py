# Main_Project/app/pipeline.py
import time
from typing import Dict, List

import pandas as pd
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import playergamelog, teamgamelog

from config import RAW_DIR, PROCESSED_DIR, PLAYER_NAMES, SEASONS, SEASON_TYPE


#Helpers functions
def _safe_sleep(seconds: float = 0.7) -> None:
    """Simple rate-limit to avoid NBA stats throttling"""
    time.sleep(seconds)


def resolve_player_ids(player_names: List[str]) -> pd.DataFrame:
    """
    Resolve player full names to player_id using nba_api static registry
    Returns a dim_players table
    """
    rows = []
    for name in player_names:
        matches = players.find_players_by_full_name(name)
        if not matches:
            raise ValueError(f"Could not find player: {name}")

        #choosing the best match: prefer active player if multiple
        matches_sorted = sorted(matches, key=lambda x: x.get("is_active", False), reverse=True)
        p = matches_sorted[0]

        rows.append(
            {
                "player_id": p["id"],
                "player_name": name,
                "full_name_registry": p["full_name"],
                "is_active": p.get("is_active", None),
            }
        )

    return pd.DataFrame(rows).drop_duplicates(subset=["player_id"])


def extract_player_gamelogs(dim_players: pd.DataFrame, seasons: List[str]) -> pd.DataFrame:
    """
    Extract player game logs for each player_id * season.
    """
    all_logs = []
    for season in seasons:
        for _, row in dim_players.iterrows():
            pid = int(row["player_id"])
            pname = row["player_name"]

            try:
                _safe_sleep()
                df = playergamelog.PlayerGameLog(
                    player_id=pid,
                    season=season,
                    season_type_all_star=SEASON_TYPE
                ).get_data_frames()[0]

                if df.empty:
                    continue

                df["season"] = season
                df["player_id"] = pid
                df["player_name"] = pname
                all_logs.append(df)

            except Exception as e:
                # Keep moving tonight; we’ll log failures
                print(f"Error: Failed gamelog for {pname} ({pid}) {season}: {e}")

    if not all_logs:
        return pd.DataFrame()

    return pd.concat(all_logs, ignore_index=True)


def extract_teams_dim() -> pd.DataFrame:
    """
    Extract team metadata (which is static).
    """
    t = teams.get_teams()
    df = pd.DataFrame(t)
    #common fields: id, full_name, abbreviation, nickname, city, state, year_founded
    df = df.rename(columns={"id": "team_id"})
    return df


def extract_team_gamelogs(seasons: List[str]) -> pd.DataFrame:
    """
    Extract team game logs for all teams * season.
    """
    dim_teams = extract_teams_dim()
    all_logs = []

    for season in seasons:
        for _, row in dim_teams.iterrows():
            tid = int(row["team_id"])
            tname = row["full_name"]

            try:
                _safe_sleep()
                df = teamgamelog.TeamGameLog(
                    team_id=tid,
                    season=season,
                    season_type_all_star=SEASON_TYPE
                ).get_data_frames()[0]

                if df.empty:
                    continue

                df["season"] = season
                df["team_id"] = tid
                df["team_name"] = tname
                all_logs.append(df)

            except Exception as e:
                print(f"Error: Failed teamlog for {tname} ({tid}) {season}: {e}")

    if not all_logs:
        return pd.DataFrame()

    return pd.concat(all_logs, ignore_index=True)


#transforming the data
def clean_player_games(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    #making column names consistent, simple, and predictable using a lightweight version of snake_case
    out.columns = [c.lower() for c in out.columns]
    #remove duplicate column labels (e.g., player_id)
    if out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated()].copy()
    #dates
    if "game_date" in out.columns:
        out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")

    #deduplicate
    #GAME_ID is unique per game so  (player_id, game_id, season) should be unique
    key_cols = [c for c in ["player_id", "game_id", "season"] if c in out.columns]
    if key_cols:
        out = out.drop_duplicates(subset=key_cols)

    # Keep a tight set of columns for dashboard (you can add later)
    keep = [c for c in out.columns if c in {
        "season", "player_id", "player_name",
        "game_id", "game_date", "matchup", "wl",
        "min", "pts", "reb", "ast", "stl", "blk", "tov",
        "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "plus_minus"
    }]
    # keep anything if schema differs; but prefer curated list
    out = out[keep] if keep else out

    return out


def clean_team_games(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out.columns = [c.lower() for c in out.columns]

    if "game_date" in out.columns:
        out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")

    key_cols = [c for c in ["team_id", "game_id", "season"] if c in out.columns]
    if key_cols:
        out = out.drop_duplicates(subset=key_cols)

    keep = [c for c in out.columns if c in {
        "season", "team_id", "team_name",
        "game_id", "game_date", "matchup", "wl",
        "pts", "reb", "ast", "tov", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "plus_minus"
    }]
    out = out[keep] if keep else out
    return out


def make_player_trends(player_games: pd.DataFrame) -> pd.DataFrame:
    """
    Simple mart table: rolling 10-game averages per player per season.
    """
    if player_games.empty:
        return player_games

    df = player_games.copy()
    if "game_date" not in df.columns:
        return df

    df = df.sort_values(["player_id", "season", "game_date"])
    for col in ["pts", "reb", "ast"]:
        if col in df.columns:
            df[f"{col}_roll10"] = (
                df.groupby(["player_id", "season"])[col]
                  .transform(lambda s: s.rolling(10, min_periods=3).mean())
            )

    return df


#Load
def save_parquet(df: pd.DataFrame, path) -> None:
    df.to_parquet(path, index=False)


def run():
    print("1) Resolve players")
    dim_players = resolve_player_ids(PLAYER_NAMES)
    save_parquet(dim_players, RAW_DIR / "raw_players.parquet")

    print("2) Extract player game logs")
    raw_player_games = extract_player_gamelogs(dim_players, SEASONS)
    save_parquet(raw_player_games, RAW_DIR / "raw_player_gamelogs.parquet")

    print("3) Transform to processed tables")
    fact_player_game = clean_player_games(raw_player_games)
    player_trends = make_player_trends(fact_player_game)

    save_parquet(dim_players, PROCESSED_DIR / "dim_players.parquet")
    save_parquet(fact_player_game, PROCESSED_DIR / "fact_player_game.parquet")
    save_parquet(player_trends, PROCESSED_DIR / "mart_player_trends.parquet")

    print("All Done :p")
    print(
        f"Players: {len(dim_players)} | "
        f"Player-games: {len(fact_player_game)}"
    )

if __name__ == "__main__":
    run()

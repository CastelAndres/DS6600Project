# Main_Project/app/config.py
from pathlib import Path

# Seasons must match nba_api format
SEASONS = ["2023-24", "2024-25"]
SEASON_TYPE = "Regular Season"  # keep simple for tonight

# 10 top performers (names -> we'll resolve to player_id via nba_api)
PLAYER_NAMES = [
    "Nikola Jokic",
    "Shai Gilgeous-Alexander",
    "Luka Doncic",
    "Giannis Antetokounmpo",
    "Jayson Tatum",
    "Anthony Edwards",
    "Stephen Curry",
    "LeBron James",
    "Kevin Durant",
    "Joel Embiid",
]

# Paths
BASE_DIR = Path(__file__).resolve().parents[1]          # .../Main_Project/app
PROJECT_DIR = BASE_DIR.parent                           # .../Main_Project
DATA_DIR = PROJECT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

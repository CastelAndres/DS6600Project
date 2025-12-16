from pathlib import Path

SEASONS = ["2023-24", "2024-25"]
SEASON_TYPE = "Regular Season"

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

#Always anchor paths to the Main_Project folder
PROJECT_DIR = Path(__file__).resolve().parents[1]  # .../Main_Project
DATA_DIR = PROJECT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


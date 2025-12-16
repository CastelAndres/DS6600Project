This is where I will be completing my data engineering final project. The project involves building a data pipeline to extract, transform, and load (ETL) data from various sources of the NBA, including player statistics, game results, and team information. The goal is to create a comprehensive database that can be used for analysis and visualization of NBA performance metrics. I will be looking at the 2023–24 + 2024–25 NBA seasons. Alongisde with looking at 10 different players as well to look at their performance over the two seasons. So overall I will be looking at a multi-source NBA player performance database.Combining player metadata and game-level statistics across the 2023-24 and 2024-25 NBA season.
I would like to not that exploratory analysis and validation were performed in Jupyter notebooks, while the production data pipeline is implemented in Python scripts to ensure reproducibility. 

## Data Sources
-  NBA statistics registry (player metadata)
-  NBA player game logs (game-level performance)
- from nba_api.stats.static import players
- from nba_api.stats.endpoints import playergamelog

## Pipeline Overview
1. Extract raw data from multiple NBA endpoints
2. Store raw datasets for reproducibility (in the data/raw folder)
3. Clean and transform data into a consistent format (standarized column names, data types)
4. Generated rolling performance metrics
5. Fed this processed data into an interactive dashboard (using Streamlit)

## Outputs:
- dim_players.parquet
- fact_player_game.parquet
- mart_player_trends.parquet

## What the dashboard does:
Used the streamlit library to create an interactive dashboard for visualizing NBA player performance trends. Which includes:
- Exploratory player trends over time
- Compared season-level performance
- Analyze recent form versus season averages
  
## How to actually get that dashboard up and running:
1. Ensure you have Python and Streamlit installed.
2. Run the following:
   ```bash
   python Main_Project/app/pipeline.py
   streamlit run Main_Project/app/dashboard.py
   ```
import psycopg2
import pandas as pd
import os
import logging

# PostgreSQL connection details (loaded from environment variables)
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "dwh"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
}

# CSV file paths
CSV_FILES = {
    "teams": "/opt/airflow/include/data_sample/data/nba_teams.csv",
    "players": "/opt/airflow/include/data_sample/data/nba_players.csv",
    "games": "/opt/airflow/include/data_sample/data/nba_games.csv"
}

# PostgreSQL table mappings
TABLES = {
    "teams": "dim_team",
    "players": "dim_player",
    "games": "fact_game"
}

def ingest_csv_to_postgres(entity_name):
    """
    Reads a CSV file and inserts the data into the corresponding PostgreSQL table.

    Args:
        entity_name (str): The name of the entity (teams, players, games).
    """
    file_path = CSV_FILES.get(entity_name)
    table_name = TABLES.get(entity_name)

    if not file_path or not table_name:
        logging.error(f"Invalid entity name '{entity_name}'. No CSV file or table mapping found.")
        return

    logging.info(f"Starting CSV ingestion for {entity_name} into table {table_name}.")

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Read CSV file
        df = pd.read_csv(file_path)

        # Insert data row by row
        for _, row in df.iterrows():
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
            cur.execute(insert_query, tuple(row))

        # Commit and close connection
        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"Successfully ingested {len(df)} records into {table_name}.")

    except Exception as e:
        logging.error(f"Error ingesting {entity_name} data: {e}")
        raise

def ingest_all_csv():
    """Runs the CSV ingestion process for teams, players, and games."""
    for entity in ["teams", "players", "games"]:
        ingest_csv_to_postgres(entity)

if __name__ == "__main__":
    ingest_all_csv()

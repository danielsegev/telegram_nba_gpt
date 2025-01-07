import os
import psycopg2
import pandas as pd

# Database configuration
db_config = {
    "dbname": "dwh",
    "user": "airflow",
    "password": "airflow",
    "host": "192.168.64.1",
    "port": "5432"
}

# Directory containing CSV files for games
games_csv_directory = "data_sample/data/nba_season_data"

# Mapping CSV fields to database table columns
column_mapping = {
    "status": "status",
    "order": "order",
    "personId": "personid",
    "starter": "starter",
    "oncourt": "oncourt",
    "played": "played",
    "statistics.assists": "statistics_assists",
    "statistics.blocks": "statistics_blocks",
    "statistics.blocksReceived": "statistics_blocksreceived",
    "statistics.fieldGoalsAttempted": "statistics_fieldgoalsattempted",
    "statistics.fieldGoalsMade": "statistics_fieldgoalsmade",
    "statistics.foulsOffensive": "statistics_foulsoffensive",
    "statistics.foulsDrawn": "statistics_foulsdrawn",
    "statistics.foulsPersonal": "statistics_foulspersonal",
    "statistics.foulsTechnical": "statistics_foulstechnical",
    "statistics.freeThrowsAttempted": "statistics_freethrowsattempted",
    "statistics.freeThrowsMade": "statistics_freethrowsmade",
    "statistics.minus": "statistics_minus",
    "statistics.minutes": "statistics_minutes",
    "statistics.minutesCalculated": "statistics_minutescalculated",
    "statistics.plus": "statistics_plus",
    "statistics.plusMinusPoints": "statistics_plusminuspoints",
    "statistics.points": "statistics_points",
    "statistics.pointsFastBreak": "statistics_pointsfastbreak",
    "statistics.pointsInThePaint": "statistics_pointsinthepaint",
    "statistics.pointsSecondChance": "statistics_pointssecondchance",
    "statistics.reboundsDefensive": "statistics_reboundsdefensive",
    "statistics.reboundsOffensive": "statistics_reboundsoffensive",
    "statistics.reboundsTotal": "statistics_reboundstotal",
    "statistics.steals": "statistics_steals",
    "statistics.threePointersAttempted": "statistics_threepointersattempted",
    "statistics.threePointersMade": "statistics_threepointersmade",
    "statistics.turnovers": "statistics_turnovers",
    "statistics.twoPointersAttempted": "statistics_twopointersattempted",
    "statistics.twoPointersMade": "statistics_twopointersmade",
    "game_id": "game_id",
    "game_date": "game_date"
}

# Columns to exclude
excluded_columns = [
    "jerseyNum", "position", "name", "nameI", "firstName", "familyName",
    "statistics.twoPointersPercentage", "season", "statistics.fieldGoalsPercentage",
    "statistics.freeThrowsPercentage", "statistics.threePointersPercentage"
]

# Columns that should be integers (to prevent float errors in PostgreSQL)
int_columns = [
    "statistics_minus", "statistics_plus", "statistics_plusminuspoints",
    "statistics_points", "statistics_pointsfastbreak", "statistics_pointsinthepaint",
    "statistics_pointssecondchance", "statistics_fieldgoalsattempted",
    "statistics_fieldgoalsmade", "statistics_foulsdrawn", "statistics_foulspersonal",
    "statistics_freethrowsattempted", "statistics_freethrowsmade",
    "statistics_reboundsdefensive", "statistics_reboundsoffensive",
    "statistics_reboundstotal", "statistics_steals", "statistics_threepointersattempted",
    "statistics_threepointersmade", "statistics_turnovers", "statistics_twopointersattempted",
    "statistics_twopointersmade"
]


def preprocess_csv(csv_file, column_mapping, excluded_columns):
    try:
        print(f"🔄 Reading CSV file: {csv_file}")

        # Read CSV, explicitly setting dtype=str to prevent automatic type conversion
        df = pd.read_csv(csv_file, dtype=str, on_bad_lines='skip')

        print(f"📊 Columns in {csv_file}: {list(df.columns)}")

        # Drop excluded columns
        df = df.drop(columns=[col for col in excluded_columns if col in df.columns], errors="ignore")

        # Rename columns based on mapping
        df = df.rename(columns=column_mapping)

        # Keep only mapped columns
        df = df[list(column_mapping.values())]

        # Convert numeric columns to integers, handling errors
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Save cleaned CSV
        temp_csv_file = "processed_games.csv"
        df.to_csv(temp_csv_file, index=False)
        print(f"✅ CSV preprocessed and saved to {temp_csv_file}.")
        return temp_csv_file

    except Exception as e:
        print(f"❌ Error preprocessing CSV file {csv_file}: {e}")
        return None


def load_csv_to_table(table_name, csv_file, connection):
    try:
        cursor = connection.cursor()

        # Explicitly list column names
        column_names = ", ".join(f'"{col}"' for col in column_mapping.values())

        # Construct the COPY command
        copy_sql = f"""
        COPY {table_name} ({column_names})
        FROM STDIN WITH CSV HEADER DELIMITER ',';
        """

        with open(csv_file, 'r') as file:
            cursor.copy_expert(copy_sql, file)

        connection.commit()
        print(f"✅ Data loaded successfully from {csv_file} into {table_name}.")
    
    except Exception as e:
        print(f"❌ Error loading data from {csv_file} into {table_name}: {e}")
        connection.rollback()
    
    finally:
        if cursor:
            cursor.close()


def load_all_games_files(directory, table_name, connection):
    csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]
    if not csv_files:
        print(f"⚠️ No CSV files found in directory: {directory}")
        return

    for csv_file in csv_files:
        file_path = os.path.join(directory, csv_file)
        print(f"🔄 Processing file: {file_path}")
        processed_csv = preprocess_csv(file_path, column_mapping, excluded_columns)
        if processed_csv:
            load_csv_to_table(table_name, processed_csv, connection)


def main():
    try:
        conn = psycopg2.connect(**db_config)

        # Load all CSV files in the directory into the nba_games table
        load_all_games_files(games_csv_directory, "nba_games", conn)

    except Exception as e:
        print(f"❌ Error connecting to the database: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

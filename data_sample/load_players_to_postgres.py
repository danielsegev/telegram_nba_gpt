import psycopg2
import pandas as pd

# Database configuration
db_config = {
    "dbname": "dwh",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Path to the CSV file
csv_file = "data_sample/nba_players.csv"  # Update with the correct path

# Mapping CSV fields to database table columns
column_mapping = {
    "id": "id",
    "full_name": "full_name",
    "BIRTHDATE": "birthdate",
    "SCHOOL": "school",
    "COUNTRY": "country",
    "LAST_AFFILIATION": "last_affiliation",
    "HEIGHT": "height",
    "WEIGHT": "weight",
    "SEASON_EXP": "season_exp",
    "JERSEY": "jersey",
    "POSITION": "position",
    "ROSTERSTATUS": "roster_status",
    "GAMES_PLAYED_CURRENT_SEASON_FLAG": "games_played_current_season_flag",
    "TEAM_ID": "team_id",
    "TEAM_NAME": "team_name",
    "DLEAGUE_FLAG": "dleague_flag",
    "NBA_FLAG": "nba_flag",
    "GAMES_PLAYED_FLAG": "games_played_flag",
    "DRAFT_YEAR": "draft_year",
    "DRAFT_ROUND": "draft_round",
    "DRAFT_NUMBER": "draft_number",
    "GREATEST_75_FLAG": "greatest_75_flag",
    "is_active": "is_active"
}


def preprocess_csv(csv_file, column_mapping):
    try:
        # Load the CSV into a DataFrame with specified encoding
        df = pd.read_csv(csv_file, encoding='latin1')  # Use 'latin1' or 'windows-1252' if 'utf-8' doesn't work

        # Rename the columns to match the database table schema
        df = df.rename(columns=column_mapping)

        # Retain only the columns needed for the database
        df = df[list(column_mapping.values())]

        # Save the processed CSV back to a temporary file
        temp_csv_file = "processed_nba_players.csv"
        df.to_csv(temp_csv_file, index=False, encoding='utf-8')  # Save in utf-8 for compatibility with PostgreSQL

        print(f"CSV preprocessed and saved to {temp_csv_file}.")
        return temp_csv_file
    except Exception as e:
        print(f"Error preprocessing CSV file: {e}")
        return None



def load_csv_to_table(table_name, csv_file, connection):
    try:
        cursor = connection.cursor()

        # Construct the COPY command
        copy_sql = f"""
        COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ',';
        """
        with open(csv_file, 'r') as file:
            cursor.copy_expert(copy_sql, file)

        connection.commit()
        print(f"Data loaded successfully from {csv_file} into {table_name}.")
    except Exception as e:
        print(f"Error loading data from {csv_file} into {table_name}: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()


def main():
    try:
        conn = psycopg2.connect(**db_config)

        # Preprocess the CSV file
        processed_csv = preprocess_csv(csv_file, column_mapping)
        if processed_csv:
            # Load the processed CSV into the players table
            load_csv_to_table("nba_players", processed_csv, conn)

    except Exception as e:
        print(f"Error connecting to the database: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

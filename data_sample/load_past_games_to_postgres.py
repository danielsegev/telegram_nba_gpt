import os
import psycopg2

# Database configuration
db_config = {
    "dbname": "dwh",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432"
}

# Directory containing CSV files for games
games_csv_directory = "data_sample/data/nba_season_data"  # Update with your directory path

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
    finally:
        if cursor:
            cursor.close()


def load_all_games_files(directory, table_name, connection):
    csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]
    if not csv_files:
        print(f"No CSV files found in directory: {directory}")
        return

    for csv_file in csv_files:
        file_path = os.path.join(directory, csv_file)
        print(f"Loading file: {file_path}")
        load_csv_to_table(table_name, file_path, connection)


def main():
    try:
        conn = psycopg2.connect(**db_config)

        # Load all CSV files in the directory into the nba_games table
        load_all_games_files(games_csv_directory, "nba_games", conn)

    except Exception as e:
        print(f"Error connecting to the database: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

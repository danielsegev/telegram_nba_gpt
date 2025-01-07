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

# Path to the CSV file
csv_file = "data_sample/data/nba_teams.csv"  # Update with the correct path

# Mapping CSV fields to database table columns
column_mapping = {
    "id": "team_id",
    "full_name": "full_name",
    "city": "city",
    "state": "state",
    "abbreviation": "abbreviation",
    "year_founded": "year_founded"
}


def preprocess_csv(csv_file, column_mapping):
    try:
        # Load the CSV into a DataFrame
        df = pd.read_csv(csv_file)

        # Rename the columns to match the database table schema
        df = df.rename(columns=column_mapping)

        # Retain only the columns needed for the database
        df = df[list(column_mapping.values())]

        # Save the processed CSV back to a temporary file
        temp_csv_file = "processed_nba_teams.csv"
        df.to_csv(temp_csv_file, index=False)

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
            # Load the processed CSV into the teams table
            load_csv_to_table("nba_teams", processed_csv, conn)

    except Exception as e:
        print(f"Error connecting to the database: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

import psycopg2
from psycopg2.extras import RealDictCursor

# Database configuration
db_config = {
    "dbname": "dwh",  # Replace with your database name
    "user": "postgres",  # Replace with your database username
    "password": "postgres",  # Replace with your database password
    "host": "localhost",  # Replace with your database host (use IP or domain if remote)
    "port": "5432"  # Replace with your database port
}

# Function to query the nba_teams table
def query_teams():
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # SQL query to fetch all records from the nba_teams table
        query = "SELECT * FROM nba_games;"
        cursor.execute(query)

        # Fetch all rows from the executed query
        teams = cursor.fetchall()

        # Print the results
        for team in teams:
            print(team)

    except psycopg2.Error as e:
        print(f"Error querying database: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    query_teams()

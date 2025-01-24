import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

base_config = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}

new_db_config = {
    "dbname": "dwh",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}


# SQL scripts to create tables
CREATE_TABLE_QUERIES = [
    """
    CREATE TABLE IF NOT EXISTS dim_team (
        team_id INT PRIMARY KEY,
        abbreviation VARCHAR(100),
        city VARCHAR(100),
        full_name VARCHAR(100),
        state VARCHAR(100),
        year_founded INT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_player (
        id INT PRIMARY KEY,
        full_name VARCHAR(100),
        birthdate DATE,
        school VARCHAR(100),
        country VARCHAR(50),
        last_affiliation VARCHAR(100),
        height VARCHAR(10),
        weight VARCHAR(10),
        season_exp INT,
        jersey VARCHAR(10),
        position VARCHAR(50),
        roster_status VARCHAR(50),
        games_played_current_season_flag VARCHAR(10),
        team_id INT,
        team_name VARCHAR(100),
        dleague_flag VARCHAR(1),
        nba_flag VARCHAR(1),
        games_played_flag VARCHAR(1),
        draft_year VARCHAR(10),
        draft_round VARCHAR(10),
        draft_number VARCHAR(10),
        greatest_75_flag VARCHAR(1),
        is_active BOOLEAN  -- New field added
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_game (
        status VARCHAR(20),
        "order" INT,
        personId INT,
        starter VARCHAR(2),
        oncourt VARCHAR(2),
        played VARCHAR(2),
        statistics_assists INT,
        statistics_blocks INT,
        statistics_blocksreceived INT,
        statistics_fieldgoalsattempted INT,
        statistics_fieldgoalsmade INT,
        statistics_foulsoffensive INT,
        statistics_foulsdrawn INT,
        statistics_foulspersonal INT,
        statistics_foulstechnical INT,
        statistics_freethrowsattempted INT,
        statistics_freethrowsmade INT,
        statistics_minus INT,
        statistics_minutes VARCHAR(50),
        statistics_minutescalculated VARCHAR(20),
        statistics_plus INT,
        statistics_plusminuspoints INT,
        statistics_points INT,
        statistics_pointsfastbreak INT,
        statistics_pointsinthepaint INT,
        statistics_pointssecondchance INT,
        statistics_reboundsdefensive INT,
        statistics_reboundsoffensive INT,
        statistics_reboundstotal INT,
        statistics_steals INT,
        statistics_threepointersattempted INT,
        statistics_threepointersmade INT,
        statistics_turnovers INT,
        statistics_twopointersattempted INT,
        statistics_twopointersmade INT,
        game_id VARCHAR(20),
        game_date DATE NOT NULL
        --PRIMARY KEY (game_id, personId) -- Assuming each game_id/personId combination is unique
    );
    """
]

def create_database():
    try:
        conn = psycopg2.connect(**base_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'dwh';")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE dwh;")
            print("Database 'dwh' created successfully.")
        else:
            print("Database 'dwh' already exists.")
    except Exception as e:
        print("Error creating database:", e)
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

def create_tables():
    try:
        conn = psycopg2.connect(**new_db_config)
        cursor = conn.cursor()
        for query in CREATE_TABLE_QUERIES:
            try:
                cursor.execute(query)
                print(f"Successfully executed query: {query}")
            except Exception as e:
                print(f"Error executing query: {query}\n{e}")
        conn.commit()
    except Exception as e:
        print("Error creating tables:", e)
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    create_database()
    create_tables()

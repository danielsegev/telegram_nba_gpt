import psycopg2
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_postgres_db():
    try:
        # Fetch environment variables with defaults
        dbname = os.getenv("POSTGRES_DB", "dwh")
        user = os.getenv("POSTGRES_USER", "airflow")
        password = os.getenv("POSTGRES_PASSWORD", "airflow")
        host = os.getenv("POSTGRES_HOST", "postgres")
        port = os.getenv("POSTGRES_PORT", "5432")

        # Log connection details (avoid logging sensitive info like passwords)
        logger.info(f"Connecting to database {dbname} at {host}:{port} as {user}...")

        # Establish connection
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        cur = conn.cursor()

        # Create tables
        create_table_queries = [
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
                game_id VARCHAR(20)
                ,game_date DATE NOT NULL
                --PRIMARY KEY (game_id, personId) -- Assuming each game_id/personId combination is unique
            );
            """
        ]

        for query in create_table_queries:
            logger.info(f"Executing query: {query}")
            cur.execute(query)

        # Commit changes and close connection
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database tables created successfully.")

    except Exception as e:
        logger.error("Failed to create database tables.", exc_info=True)
        raise

if __name__ == "__main__":
    create_postgres_db()

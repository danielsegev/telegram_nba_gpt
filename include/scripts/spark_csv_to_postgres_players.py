from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("CSVToPostgres_Players") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# PostgreSQL connection details
db_url = "jdbc:postgresql://192.168.64.1:5432/dwh"
db_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Path to the CSV file
players_csv = "/opt/airflow/data_sample/data/nba_players.csv"

# Load and process CSV data
players_df = spark.read.csv(players_csv, header=True, inferSchema=True)
players_df = players_df.selectExpr(
    "id", "full_name", "BIRTHDATE as birthdate", "SCHOOL as school", "COUNTRY as country", 
    "LAST_AFFILIATION as last_affiliation", "HEIGHT as height", "WEIGHT as weight", 
    "SEASON_EXP as season_exp", "JERSEY as jersey", "POSITION as position", "ROSTERSTATUS as roster_status", 
    "GAMES_PLAYED_CURRENT_SEASON_FLAG as games_played_current_season_flag", "TEAM_ID as team_id", "TEAM_NAME as team_name", 
    "DLEAGUE_FLAG as dleague_flag", "NBA_FLAG as nba_flag", "GAMES_PLAYED_FLAG as games_played_flag", 
    "DRAFT_YEAR as draft_year", "DRAFT_ROUND as draft_round", "DRAFT_NUMBER as draft_number", 
    "GREATEST_75_FLAG as greatest_75_flag", "is_active"
)

# Write to PostgreSQL
players_df.write.jdbc(url=db_url, table="nba_players", mode="overwrite", properties=db_properties)

print("Players data successfully ingested into PostgreSQL.")

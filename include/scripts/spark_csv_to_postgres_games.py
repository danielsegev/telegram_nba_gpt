from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("CSVToPostgres_Games") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# PostgreSQL connection details
db_url = "jdbc:postgresql://192.168.64.1:5432/dwh"
db_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Path to the CSV directory
games_dir = "/opt/airflow/data_sample/data/nba_season_data/"

games_files = [os.path.join(games_dir, f) for f in os.listdir(games_dir) if f.endswith(".csv")]
if not games_files:
    print("No game CSV files found. Exiting.")
    exit(1)

# Load and process multiple CSV files
games_df = spark.read.csv(games_files, header=True, inferSchema=True)

# Write to PostgreSQL
games_df.write.jdbc(url=db_url, table="nba_games", mode="overwrite", properties=db_properties)

print("Games data successfully ingested into PostgreSQL.")

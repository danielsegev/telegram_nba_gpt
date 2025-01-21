from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("CSVToPostgres_Teams") \
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
teams_csv = "/opt/airflow/data_sample/data/nba_teams.csv"

# Load and process CSV data
teams_df = spark.read.csv(teams_csv, header=True, inferSchema=True)
teams_df = teams_df.selectExpr("id as team_id", "full_name", "city", "state", "abbreviation", "year_founded")

# Write to PostgreSQL
teams_df.write.jdbc(url=db_url, table="dim_team", mode="overwrite", properties=db_properties)

print("Teams data successfully ingested into PostgreSQL.")

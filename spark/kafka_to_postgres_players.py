from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import psycopg2

# Initialize Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("KafkaToPostgres_Players") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "kafka-learn:9092"
kafka_topic = "players"

# Define schema for the Kafka messages
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("BIRTHDATE", StringType(), True),
    StructField("SCHOOL", StringType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("LAST_AFFILIATION", StringType(), True),
    StructField("HEIGHT", StringType(), True),
    StructField("WEIGHT", StringType(), True),
    StructField("SEASON_EXP", IntegerType(), True),
    StructField("JERSEY", StringType(), True),
    StructField("POSITION", StringType(), True),
    StructField("ROSTERSTATUS", StringType(), True),
    StructField("GAMES_PLAYED_CURRENT_SEASON_FLAG", StringType(), True),
    StructField("TEAM_ID", IntegerType(), True),
    StructField("TEAM_NAME", StringType(), True),
    StructField("DLEAGUE_FLAG", StringType(), True),
    StructField("NBA_FLAG", StringType(), True),
    StructField("GAMES_PLAYED_FLAG", StringType(), True),
    StructField("DRAFT_YEAR", StringType(), True),
    StructField("DRAFT_ROUND", StringType(), True),
    StructField("DRAFT_NUMBER", StringType(), True),
    StructField("GREATEST_75_FLAG", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

# Read data from Kafka topic
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the binary "value" field into JSON string and apply schema
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Rename columns and cast fields
json_df = json_df.withColumnRenamed("BIRTHDATE", "birthdate") \
    .withColumnRenamed("SCHOOL", "school") \
    .withColumnRenamed("COUNTRY", "country") \
    .withColumnRenamed("LAST_AFFILIATION", "last_affiliation") \
    .withColumnRenamed("HEIGHT", "height") \
    .withColumnRenamed("WEIGHT", "weight") \
    .withColumnRenamed("SEASON_EXP", "season_exp") \
    .withColumnRenamed("JERSEY", "jersey") \
    .withColumnRenamed("POSITION", "position") \
    .withColumnRenamed("ROSTERSTATUS", "roster_status") \
    .withColumnRenamed("GAMES_PLAYED_CURRENT_SEASON_FLAG", "games_played_current_season_flag") \
    .withColumnRenamed("TEAM_ID", "team_id") \
    .withColumnRenamed("TEAM_NAME", "team_name") \
    .withColumnRenamed("DLEAGUE_FLAG", "dleague_flag") \
    .withColumnRenamed("NBA_FLAG", "nba_flag") \
    .withColumnRenamed("GAMES_PLAYED_FLAG", "games_played_flag") \
    .withColumnRenamed("DRAFT_YEAR", "draft_year") \
    .withColumnRenamed("DRAFT_ROUND", "draft_round") \
    .withColumnRenamed("DRAFT_NUMBER", "draft_number") \
    .withColumnRenamed("GREATEST_75_FLAG", "greatest_75_flag") \
    .withColumn("birthdate", col("birthdate").cast(DateType())) \
    .withColumn("weight", col("weight").cast(IntegerType()))  # Cast weight to IntegerType

# Function to write data to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    valid_rows = batch_df.filter(col("id").isNotNull())

    if valid_rows.count() == 0:
        print(f"Batch {batch_id}: No valid rows to insert.")
        return

    db_config = {
        "dbname": "dwh",
        "user": "postgres",
        "password": "postgres",
        "host": "192.168.64.1",
        "port": "5432"
    }

    insert_query = """
    INSERT INTO nba_players (id, full_name, birthdate, school, country, last_affiliation, height, weight, season_exp,
    jersey, position, roster_status, games_played_current_season_flag, team_id, team_name, dleague_flag, nba_flag,
    games_played_flag, draft_year, draft_round, draft_number, greatest_75_flag, is_active)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    data_to_insert = valid_rows.collect()
    data_list = [
        (
            row["id"], row["full_name"], row["birthdate"], row["school"], row["country"], row["last_affiliation"],
            row["height"], row["weight"], row["season_exp"], row["jersey"], row["position"], row["roster_status"],
            row["games_played_current_season_flag"], row["team_id"], row["team_name"], row["dleague_flag"],
            row["nba_flag"], row["games_played_flag"], row["draft_year"], row["draft_round"], row["draft_number"],
            row["greatest_75_flag"], row["is_active"]
        )
        for row in data_to_insert
    ]

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.executemany(insert_query, data_list)
        conn.commit()
        print(f"Batch {batch_id}: Successfully inserted {len(data_list)} rows into 'nba_players' table.")

    except Exception as e:
        print(f"Batch {batch_id}: Error inserting data into PostgreSQL:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

query = json_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
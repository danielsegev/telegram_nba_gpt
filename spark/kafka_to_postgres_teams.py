from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2

# Initialize Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("KafkaToPostgres_Teams") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "kafka-learn:9092"
kafka_topic = "teams"

# Define schema for the Kafka messages
schema = StructType([
    StructField("id", IntegerType(), True),  # Adjust schema to match Kafka
    StructField("abbreviation", StringType(), True),
    StructField("city", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("year_founded", IntegerType(), True)
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

# Rename column 'id' to 'team_id' to match PostgreSQL schema
json_df = json_df.withColumnRenamed("id", "team_id")

# Function to write data to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    # Filter out rows where 'team_id' is NULL
    valid_rows = batch_df.filter(col("team_id").isNotNull())

    if valid_rows.count() == 0:
        print(f"Batch {batch_id}: No valid rows to insert.")
        return

    # PostgreSQL connection details
    db_config = {
        "dbname": "dwh",
        "user": "postgres",
        "password": "postgres",
        "host": "192.168.64.1",  # Use the host machine's IP
        "port": "5432"
    }

    # SQL query for inserting data
    insert_query = """
    INSERT INTO nba_teams (team_id, abbreviation, city, full_name, state, year_founded)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (team_id) DO NOTHING;  -- Prevent duplicate inserts
    """

    # Convert Spark DataFrame to list of tuples
    data_to_insert = valid_rows.collect()
    data_list = [
        (
            row["team_id"], row["abbreviation"], row["city"], 
            row["full_name"], row["state"], row["year_founded"]
        )
        for row in data_to_insert
    ]

    conn = None
    cursor = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Insert data
        cursor.executemany(insert_query, data_list)

        # Commit the transaction
        conn.commit()
        print(f"Batch {batch_id}: Successfully inserted {len(data_list)} rows into 'nba_teams' table.")

    except Exception as e:
        print(f"Batch {batch_id}: Error inserting data into PostgreSQL:", e)

    finally:
        # Close the cursor and connection if they were created
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Stream the data to PostgreSQL
query = json_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()

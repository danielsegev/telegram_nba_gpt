from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2

# Initialize Spark session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("KafkaToPostgres_Teams") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "kafka-learn:9092"
kafka_topic = "teams"

# Define schema for the Kafka messages
schema = StructType([
    StructField("id", IntegerType(), True),
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

# Convert binary "value" field into JSON string and apply schema
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumnRenamed("id", "team_id")

# Function to write data to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    valid_rows = batch_df.filter(col("team_id").isNotNull())

    if valid_rows.count() == 0:
        print(f"Batch {batch_id}: No valid rows to insert.")
        return

    db_config = {
        "dbname": "dwh",
        "user": "airflow",
        "password": "airflow",
        "host": "postgres",  # Docker service name
        "port": "5432"
    }

    insert_query = """
    INSERT INTO nba_teams (team_id, abbreviation, city, full_name, state, year_founded)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (team_id) DO NOTHING;
    """

    try:
        data_list = list(valid_rows.toPandas().itertuples(index=False, name=None))

        if not data_list:
            print(f"Batch {batch_id}: No valid rows to insert.")
            return

        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_query, data_list)
                conn.commit()
                print(f"Batch {batch_id}: Successfully inserted {len(data_list)} rows into 'nba_teams'.")

    except Exception as e:
        print(f"Batch {batch_id}: Error inserting data into PostgreSQL: {e}")

# Stream the data to PostgreSQL
query = json_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination(timeout=300)  # Timeout after 5 minutes

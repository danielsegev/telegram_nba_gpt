from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructField, StructType, StringType, IntegerType, FloatType
)

# Initialize Spark session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("KafkaToPostgres_Games") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "kafka-learn:9092"
kafka_topic = "games"

# Define schema for Kafka messages (added game_date)
schema = StructType([
    StructField("status", StringType(), True),
    StructField("order", IntegerType(), True),
    StructField("personId", IntegerType(), True),
    StructField("starter", StringType(), True),
    StructField("oncourt", StringType(), True),
    StructField("played", StringType(), True),
    StructField("statistics.assists", IntegerType(), True),
    StructField("statistics.blocks", IntegerType(), True),
    StructField("statistics.blocksReceived", IntegerType(), True),
    StructField("statistics.fieldGoalsAttempted", IntegerType(), True),
    StructField("statistics.fieldGoalsMade", IntegerType(), True),
    StructField("statistics.foulsOffensive", IntegerType(), True),
    StructField("statistics.foulsDrawn", IntegerType(), True),
    StructField("statistics.foulsPersonal", IntegerType(), True),
    StructField("statistics.foulsTechnical", IntegerType(), True),
    StructField("statistics.freeThrowsAttempted", IntegerType(), True),
    StructField("statistics.freeThrowsMade", IntegerType(), True),
    StructField("statistics.minus", FloatType(), True),
    StructField("statistics.minutes", StringType(), True),
    StructField("statistics.minutesCalculated", StringType(), True),
    StructField("statistics.plus", FloatType(), True),
    StructField("statistics.plusMinusPoints", FloatType(), True),
    StructField("statistics.points", IntegerType(), True),
    StructField("statistics.pointsFastBreak", IntegerType(), True),
    StructField("statistics.pointsInThePaint", IntegerType(), True),
    StructField("statistics.pointsSecondChance", IntegerType(), True),
    StructField("statistics.reboundsDefensive", IntegerType(), True),
    StructField("statistics.reboundsOffensive", IntegerType(), True),
    StructField("statistics.reboundsTotal", IntegerType(), True),
    StructField("statistics.steals", IntegerType(), True),
    StructField("statistics.threePointersAttempted", IntegerType(), True),
    StructField("statistics.threePointersMade", IntegerType(), True),
    StructField("statistics.turnovers", IntegerType(), True),
    StructField("statistics.twoPointersAttempted", IntegerType(), True),
    StructField("statistics.twoPointersMade", IntegerType(), True),
    StructField("game_id", StringType(), True),
    StructField("game_date", StringType(), True)  # ✅ Added game_date field
])

# Read data from Kafka topic
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON payload
parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data"))

# Select fields explicitly to handle dot notation (added game_date)
selected_df = parsed_df.select(
    col("data.status").alias("status"),
    col("data.order").alias("order"),
    col("data.personId").alias("personid"),
    col("data.starter").alias("starter"),
    col("data.oncourt").alias("oncourt"),
    col("data.played").alias("played"),
    col("data.`statistics.assists`").alias("statistics_assists"),
    col("data.`statistics.blocks`").alias("statistics_blocks"),
    col("data.`statistics.blocksReceived`").alias("statistics_blocksreceived"),
    col("data.`statistics.fieldGoalsAttempted`").alias("statistics_fieldgoalsattempted"),
    col("data.`statistics.fieldGoalsMade`").alias("statistics_fieldgoalsmade"),
    col("data.`statistics.foulsOffensive`").alias("statistics_foulsoffensive"),
    col("data.`statistics.foulsDrawn`").alias("statistics_foulsdrawn"),
    col("data.`statistics.foulsPersonal`").alias("statistics_foulspersonal"),
    col("data.`statistics.foulsTechnical`").alias("statistics_foulstechnical"),
    col("data.`statistics.freeThrowsAttempted`").alias("statistics_freethrowsattempted"),
    col("data.`statistics.freeThrowsMade`").alias("statistics_freethrowsmade"),
    col("data.`statistics.minus`").alias("statistics_minus"),
    col("data.`statistics.minutes`").alias("statistics_minutes"),
    col("data.`statistics.minutesCalculated`").alias("statistics_minutescalculated"),
    col("data.`statistics.plus`").alias("statistics_plus"),
    col("data.`statistics.plusMinusPoints`").alias("statistics_plusminuspoints"),
    col("data.`statistics.points`").alias("statistics_points"),
    col("data.`statistics.pointsFastBreak`").alias("statistics_pointsfastbreak"),
    col("data.`statistics.pointsInThePaint`").alias("statistics_pointsinthepaint"),
    col("data.`statistics.pointsSecondChance`").alias("statistics_pointssecondchance"),
    col("data.`statistics.reboundsDefensive`").alias("statistics_reboundsdefensive"),
    col("data.`statistics.reboundsOffensive`").alias("statistics_reboundsoffensive"),
    col("data.`statistics.reboundsTotal`").alias("statistics_reboundstotal"),
    col("data.`statistics.steals`").alias("statistics_steals"),
    col("data.`statistics.threePointersAttempted`").alias("statistics_threepointersattempted"),
    col("data.`statistics.threePointersMade`").alias("statistics_threepointersmade"),
    col("data.`statistics.turnovers`").alias("statistics_turnovers"),
    col("data.`statistics.twoPointersAttempted`").alias("statistics_twopointersattempted"),
    col("data.`statistics.twoPointersMade`").alias("statistics_twopointersmade"),
    col("data.game_id").alias("game_id"),
    col("data.game_date").alias("game_date")  # ✅ Added game_date
)

# Write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    db_url = "jdbc:postgresql://192.168.64.1:5432/dwh"
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    try:
        if batch_df.isEmpty():
            print(f"Batch {batch_id}: No data to write.")
            return

        batch_df.write.jdbc(url=db_url, table="nba_games", mode="append", properties=db_properties)
        print(f"Batch {batch_id}: Successfully written to PostgreSQL")

    except Exception as e:
        print(f"Batch {batch_id}: Error writing to PostgreSQL: {e}")

# Start the streaming query
query = None
try:
    query = selected_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .trigger(once=True)  \
        .start()

    query.awaitTermination(timeout=300)

except Exception as e:
    print(f"Error starting or awaiting termination of streaming query: {e}")
finally:
    if query is not None:
        query.stop()
        print("Streaming query stopped explicitly.")

print("Script finished")

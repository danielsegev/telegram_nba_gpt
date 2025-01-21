from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("InspectKafkaGamesData") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Kafka configuration
KAFKA_BROKERS = "kafka-learn:9092"
KAFKA_TOPIC = "games"

# Define schema for JSON messages
schema = StructType([
    StructField("status", StringType(), True),
    StructField("order", IntegerType(), True),
    StructField("personId", IntegerType(), True),
    StructField("jerseyNum", StringType(), True),
    StructField("position", StringType(), True),
    StructField("starter", StringType(), True),
    StructField("oncourt", StringType(), True),
    StructField("played", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nameI", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("familyName", StringType(), True),
    StructField("statistics.assists", IntegerType(), True),
    StructField("statistics.blocks", IntegerType(), True),
    StructField("statistics.blocksReceived", IntegerType(), True),
    StructField("statistics.fieldGoalsAttempted", IntegerType(), True),
    StructField("statistics.fieldGoalsMade", IntegerType(), True),
    StructField("statistics.fieldGoalsPercentage", DoubleType(), True),
    StructField("statistics.foulsOffensive", IntegerType(), True),
    StructField("statistics.foulsDrawn", IntegerType(), True),
    StructField("statistics.foulsPersonal", IntegerType(), True),
    StructField("statistics.foulsTechnical", IntegerType(), True),
    StructField("statistics.freeThrowsAttempted", IntegerType(), True),
    StructField("statistics.freeThrowsMade", IntegerType(), True),
    StructField("statistics.freeThrowsPercentage", DoubleType(), True),
    StructField("statistics.minus", IntegerType(), True),
    StructField("statistics.minutes", StringType(), True),
    StructField("statistics.minutesCalculated", StringType(), True),
    StructField("statistics.plus", IntegerType(), True),
    StructField("statistics.plusMinusPoints", IntegerType(), True),
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
    StructField("statistics.threePointersPercentage", DoubleType(), True),
    StructField("statistics.turnovers", IntegerType(), True),
    StructField("statistics.twoPointersAttempted", IntegerType(), True),
    StructField("statistics.twoPointersMade", IntegerType(), True),
    StructField("statistics.twoPointersPercentage", DoubleType(), True),
    StructField("game_id", StringType(), True),
    StructField("game_date", StringType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write parsed data to the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

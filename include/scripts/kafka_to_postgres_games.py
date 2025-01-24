from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToPostgres_Games") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Kafka configuration
KAFKA_BROKERS = "kafka-learn:9092"
KAFKA_TOPIC = "games"

# PostgreSQL configuration
POSTGRES_URL = "jdbc:postgresql://postgres:5432/dwh"
POSTGRES_TABLE = "fact_game"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"

# Schema matching Kafka's flat JSON structure
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
    StructField("statistics.turnovers", IntegerType(), True),
    StructField("statistics.twoPointersAttempted", IntegerType(), True),
    StructField("statistics.twoPointersMade", IntegerType(), True),
    StructField("game_id", StringType(), True),
    StructField("game_date", StringType(), True)
])

# Read Kafka messages in batch mode
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse and transform data
df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))

df_flat = df_parsed.select(
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
    to_date(col("data.game_date"), "yyyy-MM-dd").alias("game_date")
)

# Write to PostgreSQL in batch mode
df_flat.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", POSTGRES_TABLE) \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("✅ Finished processing Kafka messages. Exiting.")

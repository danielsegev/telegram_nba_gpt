from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaToPostgres")

# 🛠️ Initialize Spark Session
spark = SparkSession.builder \
    .appName("Ingest NBA Games Data from Kafka to Postgres") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.pyspark.python", "/usr/bin/python3") \
    .config("spark.pyspark.driver.python", "/usr/bin/python3") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# ✅ Kafka Configuration
KAFKA_TOPIC = "games"
KAFKA_BROKERS = "kafka-learn:9092"

# ✅ Read data from Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# ✅ Schema Definitions
statistics_schema = StructType([
    StructField("assists", IntegerType(), True),
    StructField("blocks", IntegerType(), True),
    StructField("blocksReceived", IntegerType(), True),
    StructField("fieldGoalsAttempted", IntegerType(), True),
    StructField("fieldGoalsMade", IntegerType(), True),
    StructField("foulsOffensive", IntegerType(), True),
    StructField("foulsDrawn", IntegerType(), True),
    StructField("foulsPersonal", IntegerType(), True),
    StructField("foulsTechnical", IntegerType(), True),
    StructField("freeThrowsAttempted", IntegerType(), True),
    StructField("freeThrowsMade", IntegerType(), True),
    StructField("minus", IntegerType(), True),
    StructField("minutes", StringType(), True),
    StructField("minutesCalculated", StringType(), True),
    StructField("plus", IntegerType(), True),
    StructField("plusMinusPoints", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("pointsFastBreak", IntegerType(), True),
    StructField("pointsInThePaint", IntegerType(), True),
    StructField("pointsSecondChance", IntegerType(), True),
    StructField("reboundsDefensive", IntegerType(), True),
    StructField("reboundsOffensive", IntegerType(), True),
    StructField("reboundsTotal", IntegerType(), True),
    StructField("steals", IntegerType(), True),
    StructField("threePointersAttempted", IntegerType(), True),
    StructField("threePointersMade", IntegerType(), True),
    StructField("turnovers", IntegerType(), True),
    StructField("twoPointersAttempted", IntegerType(), True),
    StructField("twoPointersMade", IntegerType(), True),
])

schema = StructType([
    StructField("status", StringType(), True),
    StructField("order", IntegerType(), True),
    StructField("personId", IntegerType(), True),
    StructField("starter", StringType(), True),
    StructField("onCourt", StringType(), True),
    StructField("played", StringType(), True),
    StructField("statistics", statistics_schema, True),
    StructField("gameId", StringType(), True),
    StructField("gameDate", StringType(), True)
])

# ✅ Parse and Transform Data
df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))
df_final = df_parsed.select(
    col("data.status"),
    col("data.order"),
    col("data.personId").alias("personid"),
    col("data.starter"),
    col("data.onCourt").alias("oncourt"),
    col("data.played"),
    col("data.statistics.assists").alias("statistics_assists"),
    col("data.statistics.blocks").alias("statistics_blocks"),
    col("data.statistics.blocksReceived").alias("statistics_blocksreceived"),
    col("data.statistics.fieldGoalsAttempted").alias("statistics_fieldgoalsattempted"),
    col("data.statistics.fieldGoalsMade").alias("statistics_fieldgoalsmade"),
    col("data.statistics.foulsOffensive").alias("statistics_foulsoffensive"),
    col("data.statistics.foulsDrawn").alias("statistics_foulsdrawn"),
    col("data.statistics.foulsPersonal").alias("statistics_foulspersonal"),
    col("data.statistics.foulsTechnical").alias("statistics_foulstechnical"),
    col("data.statistics.freeThrowsAttempted").alias("statistics_freethrowsattempted"),
    col("data.statistics.freeThrowsMade").alias("statistics_freethrowsmade"),
    col("data.statistics.minus").alias("statistics_minus"),
    col("data.statistics.minutes").alias("statistics_minutes"),
    col("data.statistics.minutesCalculated").alias("statistics_minutescalculated"),
    col("data.statistics.plus").alias("statistics_plus"),
    col("data.statistics.plusMinusPoints").alias("statistics_plusminuspoints"),
    col("data.statistics.points").alias("statistics_points"),
    col("data.statistics.pointsFastBreak").alias("statistics_pointsfastbreak"),
    col("data.statistics.pointsInThePaint").alias("statistics_pointsinthepaint"),
    col("data.statistics.pointsSecondChance").alias("statistics_pointssecondchance"),
    col("data.statistics.reboundsDefensive").alias("statistics_reboundsdefensive"),
    col("data.statistics.reboundsOffensive").alias("statistics_reboundsoffensive"),
    col("data.statistics.reboundsTotal").alias("statistics_reboundstotal"),
    col("data.statistics.steals").alias("statistics_steals"),
    col("data.statistics.threePointersAttempted").alias("statistics_threepointersattempted"),
    col("data.statistics.threePointersMade").alias("statistics_threepointersmade"),
    col("data.statistics.turnovers").alias("statistics_turnovers"),
    col("data.statistics.twoPointersAttempted").alias("statistics_twopointersattempted"),
    col("data.statistics.twoPointersMade").alias("statistics_twopointersmade"),
    col("data.gameId").alias("game_id"),
    to_date(col("data.gameDate"), "yyyy-MM-dd").alias("game_date")
)

# ✅ Filter and Validate Data
df_filtered = df_final.filter(col("game_date").isNotNull())
if df_filtered.rdd.isEmpty():
    logger.info("No valid data to write to PostgreSQL. Exiting job.")
else:
    # ✅ PostgreSQL Connection Config
    POSTGRES_URL = "jdbc:postgresql://postgres:5432/dwh"
    POSTGRES_TABLE = "nba_games"
    POSTGRES_USER = "airflow"
    POSTGRES_PASSWORD = "airflow"

    try:
        # ✅ Write to PostgreSQL
        df_filtered.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        logger.info("✅ Data written to PostgreSQL.")
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {e}")

# Stop Spark Session
spark.stop()

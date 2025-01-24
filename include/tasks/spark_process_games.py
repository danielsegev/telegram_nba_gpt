from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamefinder, commonplayerinfo
from nba_api.live.nba.endpoints import boxscore

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaToPostgres")

def ingest_games_kafka_to_postgres():
    """
    Ingest NBA games data from Kafka topic 'games', transform it using Spark,
    and load it into PostgreSQL table 'nba_games'.
    """
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

    try:
        # ✅ Kafka Configuration
        KAFKA_TOPIC = "games"
        KAFKA_BROKERS = "kafka:9092"

        # ✅ Read data from Kafka
        logger.info("Reading data from Kafka...")
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
        logger.info("Parsing and transforming data...")
        df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))
        df_final = df_parsed.select(
            col("data.status"),
            col("data.order"),
            col("data.personId").alias("personid"),
            col("data.starter"),
            col("data.onCourt").alias("oncourt"),
            col("data.played"),
            col("data.statistics.points").alias("statistics_points"),
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
            POSTGRES_USER = "postgres"
            POSTGRES_PASSWORD = "postgres"

            # ✅ Write to PostgreSQL
            logger.info("Writing data to PostgreSQL...")
            df_filtered.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", POSTGRES_TABLE) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            logger.info("✅ Data successfully written to PostgreSQL.")
    except Exception as e:
        logger.error(f"Error in Kafka-to-Postgres ingestion: {e}")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    ingest_games_kafka_to_postgres()

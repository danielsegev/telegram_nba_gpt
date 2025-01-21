from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaToPostgres")

# Initialize Spark session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("KafkaToPostgres_Games") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Kafka configuration
KAFKA_BROKERS = "kafka-learn:9092"
KAFKA_TOPIC = "games"

# PostgreSQL configuration
DB_CONFIG = {
    "dbname": "dwh",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}
POSTGRES_TABLE = "fact_game"

# Schema for the Kafka messages
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

# Read data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse and transform data
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

# Function to write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()

            insert_query = """
            INSERT INTO fact_game (status, "order", personid, starter, oncourt, played, statistics_assists, statistics_blocks,
                                   statistics_blocksreceived, statistics_fieldgoalsattempted, statistics_fieldgoalsmade,
                                   statistics_foulsoffensive, statistics_foulsdrawn, statistics_foulspersonal,
                                   statistics_foulstechnical, statistics_freethrowsattempted, statistics_freethrowsmade,
                                   statistics_minus, statistics_minutes, statistics_minutescalculated, statistics_plus,
                                   statistics_plusminuspoints, statistics_points, statistics_pointsfastbreak,
                                   statistics_pointsinthepaint, statistics_pointssecondchance, statistics_reboundsdefensive,
                                   statistics_reboundsoffensive, statistics_reboundstotal, statistics_steals,
                                   statistics_threepointersattempted, statistics_threepointersmade, statistics_turnovers,
                                   statistics_twopointersattempted, statistics_twopointersmade, game_id, game_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id, personid) DO UPDATE SET
                status = EXCLUDED.status,
                "order" = EXCLUDED."order";
            """

            data_list = list(batch_df.toPandas().itertuples(index=False, name=None))
            cursor.executemany(insert_query, data_list)
            connection.commit()
            logger.info(f"Batch {batch_id}: Successfully inserted {len(data_list)} rows into {POSTGRES_TABLE}.")

        except Exception as e:
            logger.error(f"Batch {batch_id}: Error writing to PostgreSQL: {e}")

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

# Write stream to PostgreSQL
df_final.filter(col("game_date").isNotNull()) \
    .writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start() \
    .awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 🛠️ Initialize Spark Session
spark = SparkSession.builder \
    .appName("Ingest NBA Games Data from Kafka to Postgres") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# ✅ Kafka Configuration
KAFKA_TOPIC = "games"
KAFKA_BROKERS = "kafka-learn:9092"

# ✅ Read data from Kafka (Batch Mode)
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# ✅ Debugging: Print Kafka Schema to Confirm Field Names
print("🔥 Kafka Raw Data Schema:")
df.printSchema()

# ✅ Corrected Schema Matching Kafka JSON
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
    StructField("statistics", statistics_schema, True),  # ✅ Fix: Ensure proper JSON structure
    StructField("gameId", StringType(), True),
    StructField("gameDate", StringType(), True)  # ✅ Fix: Use camelCase from Kafka
])

# ✅ Parse JSON from Kafka Messages
df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))

# ✅ Debugging: Print Parsed Schema
print("🔥 Parsed JSON Schema:")
df_parsed.printSchema()

# ✅ Extract and Flatten Data (Manually Mapping to snake_case for PostgreSQL)
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
    to_date(col("data.gameDate"), "yyyy-MM-dd").alias("game_date")  # ✅ Convert to DateType
)

# 🚨 Debug: Count NULL game_date records
null_game_date_count = df_final.filter(col("game_date").isNull()).count()
if null_game_date_count > 0:
    print(f"❌ Warning: {null_game_date_count} rows have NULL game_date values!")

# 🚀 Filter out records with NULL game_date
df_final = df_final.filter(col("game_date").isNotNull())

# ✅ PostgreSQL Database Connection Config
POSTGRES_URL = "jdbc:postgresql://postgres:5432/dwh"
POSTGRES_TABLE = "nba_games"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"

# ✅ Write Data to PostgreSQL in **Batch Mode**
df_final.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", POSTGRES_TABLE) \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("✅ Batch Job Successfully Written Data to PostgreSQL.")

# Stop Spark session when job is done
spark.stop()

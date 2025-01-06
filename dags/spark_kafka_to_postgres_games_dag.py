from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
}

# Define the DAG
with DAG(
    dag_id="spark_kafka_to_postgres_games_dag",
    default_args=default_args,
    description="Submit Spark job to process games data from Kafka to PostgreSQL",
    schedule_interval=None,  # Set a schedule if needed
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    submit_spark_games_job = SparkSubmitOperator(
        task_id="submit_spark_games_job",
        conn_id="spark_default",
        application="/usr/local/airflow/include/scripts/scripts/kafka_to_postgres_games.py",  # Path to your games script
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0",
        executor_memory="2G",
        driver_memory="1G",
        name="SparkKafkaToPostgresGames",
        conf={
            "spark.executor.extraJavaOptions": "-Dlog4j.configuration=log4j.properties",
            "spark.driverEnv.SPARK_HOME": "/opt/spark",
            "spark.executorEnv.SPARK_HOME": "/opt/spark",
            "spark.driverEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
            "spark.executorEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
        },
    )

    submit_spark_games_job >> []  # Empty list as terminal task
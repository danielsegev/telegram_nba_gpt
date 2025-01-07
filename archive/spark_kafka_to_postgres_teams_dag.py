from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
}

# Function to save data to PostgreSQL
def save_to_postgres():
    # Implement your logic to connect to PostgreSQL and save data
    # processed by the Spark job
    # ... (your code to connect and save data)
    pass

# Define the DAG
with DAG(
    dag_id="spark_kafka_to_postgres_dag",
    default_args=default_args,
    description="Submit Spark job and save Kafka data to PostgreSQL",
    schedule_interval=None,  # Set a schedule if needed
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id="submit_spark_job",
        conn_id="spark_default",
        application="/usr/local/airflow/include/scripts/scripts/kafka_to_postgres_teams.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0",
        executor_memory="2G",
        driver_memory="1G",
        name="SparkKafkaToPostgres",
        conf={
            "spark.executor.extraJavaOptions": "-Dlog4j.configuration=log4j.properties",
            "spark.driverEnv.SPARK_HOME": "/opt/spark",  # Set SPARK_HOME for driver
            "spark.executorEnv.SPARK_HOME": "/opt/spark",  # Set SPARK_HOME for executors
            "spark.driverEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",  # Set PATH for driver
            "spark.executorEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",  # Set PATH for executors
        },
        # Remove env_vars section
    )

    # Task to save data to PostgreSQL only after Spark processing completes
    save_to_postgres_task = PythonOperator(
        task_id="save_to_postgres",
        python_callable=save_to_postgres,
        trigger_rule="all_success",  # Ensures this runs only if Spark job succeeds
    )

    submit_spark_job >> save_to_postgres_task
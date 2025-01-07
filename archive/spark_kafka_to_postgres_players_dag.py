from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
}

# Dummy function (replace with actual logic if needed)
def dummy_python_task():
    pass

# Define the DAG
with DAG(
    dag_id="spark_kafka_to_postgres_players_dag",
    default_args=default_args,
    description="Submit Spark job to process players data from Kafka to PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    submit_spark_players_job = SparkSubmitOperator(
        task_id="submit_spark_players_job",
        conn_id="spark_default",
        application="/usr/local/airflow/include/scripts/scripts/kafka_to_postgres_players.py",  # Path to your players script
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0",
        executor_memory="2G",
        driver_memory="1G",
        name="SparkKafkaToPostgresPlayers",
        conf={
            "spark.executor.extraJavaOptions": "-Dlog4j.configuration=log4j.properties",
            "spark.driverEnv.SPARK_HOME": "/opt/spark",
            "spark.executorEnv.SPARK_HOME": "/opt/spark",
            "spark.driverEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
            "spark.executorEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
        },
    )

    #Dummy Python Operator for now, since the writing is in the spark script
    save_to_postgres_players_task = PythonOperator(
        task_id="save_to_postgres_players",
        python_callable=dummy_python_task,
        trigger_rule="all_success",
    )

    submit_spark_players_job >> save_to_postgres_players_task
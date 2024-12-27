from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from create_tables import create_database, create_tables  # Import your functions

# Define the DAG
with DAG(
    "initialize_postgres_db",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Initialize PostgreSQL Database and Tables",
    schedule_interval=None,
    start_date=datetime(2023, 12, 27),
    catchup=False,
) as dag:

    # Task to create the database
    create_db_task = PythonOperator(
        task_id="create_database",
        python_callable=create_database,  # Use the create_database function
    )

    # Task to create the tables
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,  # Use the create_tables function
    )

    # Task dependencies
    create_db_task >> create_tables_task

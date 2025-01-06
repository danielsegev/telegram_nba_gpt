from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import paramiko

def run_spark_job():
    """
    Function to SSH into the `dev_env` container and execute the Spark job.
    """
    remote_host = "dev_env"  # Host for the `dev_env` container
    remote_port = 22         # SSH port mapping in docker-compose
    remote_user = "developer"  # SSH user
    remote_password = "developer"  # SSH password
    spark_script_path = "/spark_jobs/kafka_to_postgres_teams.py"  # Path to your Spark script inside the container
    spark_command = (
        f"/usr/local/bin/spark-submit "
        "--master local[*] "
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.6.0 "
        f"{spark_script_path}"
    )

    # Set up SSH client
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Establish the SSH connection
        print("Connecting to SSH...")
        ssh_client.connect(
            hostname=remote_host,
            port=remote_port,
            username=remote_user,
            password=remote_password
        )
        print("SSH connection established.")

        # Execute the Spark job
        print("Executing Spark job...")
        stdin, stdout, stderr = ssh_client.exec_command(spark_command, get_pty=True, timeout=3600)

        # Capture output
        stdout_lines = stdout.readlines()
        stderr_lines = stderr.readlines()

        # Print and log outputs
        if stdout_lines:
            print("Spark job output:")
            for line in stdout_lines:
                print(line.strip())

        if stderr_lines:
            print("Spark job errors:")
            for error_line in stderr_lines:
                print(error_line.strip())

        # Check exit status
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            raise RuntimeError(f"Spark job failed with exit status {exit_status}")

        print("Spark job completed successfully.")

    except Exception as e:
        print(f"Error during Spark job execution: {str(e)}")
        raise e

    finally:
        ssh_client.close()
        print("SSH connection closed.")

# Define the Airflow DAG
with DAG(
    "spark_kafka_to_postgres_teams",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    description="Load Kafka 'teams' data into PostgreSQL using Spark on a remote server",
    schedule_interval=None,
    start_date=datetime(2023, 12, 29),
    catchup=False,
    max_active_runs=1,  # Ensures only one instance of the DAG runs at a time
) as dag:

    spark_job_task = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
    )

    spark_job_task

import paramiko
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2
from psycopg2 import pool

# PostgreSQL connection pool
connection_pool = None

# PostgreSQL connection details
db_config = {
    "dbname": "dwh",
    "user": "airflow",
    "password": "airflow",
    "host": "192.168.64.1",  # Use the host machine's IP
    "port": "5432"
}

def write_to_postgres(batch_df, batch_id):
    """
    Function to write data to PostgreSQL from a Spark DataFrame.
    """
    print(f"Processing batch {batch_id}...")

    # Filter out rows where 'team_id' is NULL
    valid_rows = batch_df.filter(col("team_id").isNotNull())

    row_count = valid_rows.count()
    if row_count == 0:
        print(f"Batch {batch_id}: No valid rows to insert.")
        return

    # SQL query for inserting data
    insert_query = """
    INSERT INTO nba_teams (team_id, abbreviation, city, full_name, state, year_founded)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (team_id) DO NOTHING;  -- Prevent duplicate inserts
    """

    try:
        # Fetch a connection from the pool
        conn = connection_pool.getconn()
        cursor = conn.cursor()

        # Convert Spark DataFrame to list of tuples
        data_list = [
            (
                row["team_id"], row["abbreviation"], row["city"], 
                row["full_name"], row["state"], row["year_founded"]
            )
            for row in valid_rows.collect()
        ]

        # Insert data
        cursor.executemany(insert_query, data_list)
        conn.commit()
        print(f"Batch {batch_id}: Successfully inserted {row_count} rows into 'nba_teams' table.")

    except Exception as e:
        print(f"Batch {batch_id}: Error inserting data into PostgreSQL: {e}")

    finally:
        # Close the cursor and return the connection to the pool
        if cursor:
            cursor.close()
        if conn:
            connection_pool.putconn(conn)

def execute_spark_job_via_ssh():
    """
    Function to execute a Spark job via SSH and load data from Kafka to PostgreSQL.
    """
    global connection_pool
    remote_host = "localhost"  # Replace with the SSH hostname or IP
    remote_port = 22022  # SSH port
    remote_user = "developer"  # Replace with SSH username
    remote_password = "developer"  # Replace with SSH password
    remote_spark_job_path = "/opt/spark_jobs/kafka_to_postgres_teams.py"  # Path where the Spark script will be on the remote machine
    local_spark_job_path = "/Users/danielsegev/Library/Mobile Documents/com~apple~CloudDocs/Desktop/Projects/telegram_nba_gpt/spark_jobs/kafka_to_postgres_teams.py"  # Path to your Spark script locally
    spark_submit_command = (
        f"/usr/local/bin/spark-submit "
        "--master local[*] "
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.6.0 "
        f"{remote_spark_job_path}"
    )

    # Kafka configuration
    kafka_bootstrap_servers = "kafka-learn:9092"
    kafka_topic = "teams"

    # Schema for Kafka messages
    schema = StructType([
        StructField("id", IntegerType(), True),  # Adjust schema to match Kafka
        StructField("abbreviation", StringType(), True),
        StructField("city", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("year_founded", IntegerType(), True)
    ])

    try:
        # Establish the PostgreSQL connection pool
        connection_pool = pool.SimpleConnectionPool(1, 10, **db_config)

        # Establish the SSH connection
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=remote_host,
            port=remote_port,
            username=remote_user,
            password=remote_password
        )
        print("SSH connection established.")

        # Transfer the Spark job script to the remote machine
        sftp_client = ssh_client.open_sftp()
        sftp_client.put(local_spark_job_path, remote_spark_job_path)
        sftp_client.close()
        print(f"Transferred {local_spark_job_path} to {remote_host}:{remote_spark_job_path}")

        # Execute the Spark job via spark-submit
        stdin, stdout, stderr = ssh_client.exec_command(spark_submit_command, get_pty=True)

        # Print output line by line
        print("Starting Spark job execution...")
        for line in iter(stdout.readline, ""):
            print(f"STDOUT: {line.strip()}")
        for error_line in iter(stderr.readline, ""):
            print(f"STDERR: {error_line.strip()}")

        # Wait for the command to complete
        exit_status = stdout.channel.recv_exit_status()
        if exit_status == 0:
            print("Spark job completed successfully.")
        else:
            print(f"Spark job failed with exit status {exit_status}")

    except Exception as e:
        print(f"Error during SSH or Spark execution: {e}")

    finally:
        # Close SSH connection
        ssh_client.close()
        print("SSH connection closed.")

        # Close the PostgreSQL connection pool
        if connection_pool:
            connection_pool.closeall()
            print("PostgreSQL connection pool closed.")

if __name__ == "__main__":
    execute_spark_job_via_ssh()

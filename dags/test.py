import psycopg2

try:
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="localhost",
        port="5432"
    )
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
finally:
    if 'conn' in locals():
        conn.close()


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
import os

API_KEY = os.getenv("WEATHERSTACK_API_KEY")

def extract_and_load():
    url = f"http://api.weatherstack.com/current?access_key={API_KEY}&query=Nairobi"
    response = requests.get(url)
    data = response.json()

    conn = psycopg2.connect(
        host="postgres",
        dbname="weather_dw",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.weather (
            city TEXT,
            temperature INT,
            humidity INT,
            wind_speed INT,
            observation_time TIMESTAMP,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        INSERT INTO raw.weather (city, temperature, humidity, wind_speed, observation_time)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        data["location"]["name"],
        data["current"]["temperature"],
        data["current"]["humidity"],
        data["current"]["wind_speed"],
        datetime.strptime(data["current"]["observation_time"], "%I:%M %p")
    ))

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id="weather_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    extract_load_task = PythonOperator(
        task_id="extract_and_load_weather",
        python_callable=extract_and_load
    )

    extract_load_task

#This code won't write any file locally. All will be on s3 bucket

import json
import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import timedelta, datetime
from io import StringIO

# Function to convert temperature from Kelvin to Fahrenheit
def kelvin_to_fahrenheit(temperature_in_kelvin):
    return (temperature_in_kelvin - 273.15) * (9/5) + 32

# Function to transform data and upload CSV file to AWS S3
def transform_data(**kwargs):
    ti = kwargs['ti']  # Get the task instance
    data = ti.xcom_pull(task_ids="extract_weather_data")
    
    if not data:
        raise ValueError("No data found in XCom for task 'extract_weather_data'")
    
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'])

    # Prepare transformed data
    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_fahrenheit,
        "Feels like (F)": feels_like_fahrenheit,
        "Min Temp (F)": min_temp_fahrenheit,
        "Max Temp (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record (UTC)": time_of_record,
        "Sunrise Time (UTC)": sunrise_time,
        "Sunset Time (UTC)": sunset_time
    }
    
    # Convert to DataFrame
    df_data = pd.DataFrame([transformed_data])

    # Convert DataFrame to CSV in-memory
    csv_buffer = StringIO()
    df_data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Ensure the buffer is at the beginning

    # AWS S3 configuration
    s3_client = boto3.client('s3')
    bucket_name = 'mazziairflowbucket'

    # Filename with timestamp
    now = datetime.now()
    dt_string = now.strftime("%d-%m-%Y_%H-%M-%S")
    filename = f'Current_Weather_Data_Dallas_{dt_string}.csv'

    # Upload the CSV to AWS S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    print(f"File {filename} uploaded to bucket {bucket_name}.")

# Default arguments for Airflow DAG
default_args = {
    'owner': 'mazzi_airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}

# Define the DAG
with DAG(
    'Weather_Dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag: 

    # Check if the weather API is ready
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weather_api',  # Connection ID configured in Airflow
        endpoint='/data/2.5/weather?q=Dallas&appid=b174e84b8aeba9682168bc54cda2995f',
        poke_interval=10,
        timeout=600,
    )

    # Extract weather data
    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weather_api',
        endpoint='/data/2.5/weather?q=Dallas&appid=b174e84b8aeba9682168bc54cda2995f',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    # Transform and load the data
    transform_and_load_weather_data = PythonOperator(
        task_id='transform_loaded_weather_data',
        python_callable=transform_data,
        provide_context=True,  # Ensure context is provided for the PythonOperator
    )

    # Set task dependencies
    is_weather_api_ready >> extract_weather_data >> transform_and_load_weather_data

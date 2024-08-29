# Airflow DAG for Weather Data Extraction

This repository contains an Apache Airflow DAG that extracts weather data from an API, transforms the data, and uploads it to an s3 bucket.

## Features

- Checks if the weather API is ready from https://openweathermap.org.
- Extracts weather data for Dallas.
- Transforms the data and uploads it to an s3 bucket as a CSV file without writing any file locally.

## How to Use

1. Ensure you have Airflow and necessary dependencies installed in a virtual env.
2. Configure Airflow to use this DAG.
3. Get your API key from https://openweathermap.org. after account creation.
4. Set up your own AWS credentials and s3 bucket - get the s3 bucket name.

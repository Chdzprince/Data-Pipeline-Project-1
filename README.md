# Airflow DAG for Weather Data Extraction

This repository contains an Apache Airflow DAG that extracts weather data from an API, transforms the data, and uploads it to an S3 bucket.

## Features

- Checks if the weather API is ready.
- Extracts weather data for Dallas.
- Transforms the data and uploads it to an S3 bucket as a CSV file without writing any file locally.

## How to Use

1. Ensure you have Airflow and necessary dependencies installed in a virtual env.
2. Configure Airflow to use this DAG.
3. Set up your own AWS credentials and S3 bucket.

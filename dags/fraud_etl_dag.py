"""Airflow DAG definition for the fraud detection ETL pipeline.

This DAG orchestrates extraction, transformation, model training and
loading of credit card transactions into a PostgreSQL database. It is
scheduled to run once daily and consists of four sequential tasks:

1. ``extract`` – reads the raw CSV from the mounted data directory and
   writes it to a temporary location.
2. ``transform`` – cleans the extracted data and adds engineered
   features, persisting the result to a temporary file.
3. ``fraud_model`` – trains a logistic regression model and appends
   fraud predictions to the dataset.
4. ``load`` – loads the predicted dataset into the configured
   PostgreSQL database table.

The DAG uses Airflow's ``LocalExecutor`` and thus runs all tasks in
the same container. Temporary files are written to ``/tmp``, which is
shared between tasks within the container. Should you wish to scale
out to a distributed executor, consider persisting intermediates to
object storage or a shared volume instead.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# Add the scripts directory to the Python path so that imports work
dag_dir = os.path.dirname(os.path.abspath(__file__))
scripts_dir = os.path.normpath(os.path.join(dag_dir, os.pardir, 'scripts'))
if scripts_dir not in sys.path:
    sys.path.append(scripts_dir)


def extract_to_temp(**context) -> None:
    """Extract the raw dataset and persist it to a temporary CSV file.

    This function reads the credit card transactions using the
    ``extract`` module and writes the DataFrame to
    ``/tmp/extracted.csv``. The destination file is then available to
    subsequent tasks within the same container.
    """
    from scripts.extract import extract

    df = extract()
    temp_path = '/tmp/extracted.csv'
    df.to_csv(temp_path, index=False)


def transform_to_temp(**context) -> None:
    """Transform the extracted data and write it to a temporary file.

    Reads ``/tmp/extracted.csv``, applies cleaning and feature
    engineering via the ``transform`` module, then writes the
    transformed DataFrame to ``/tmp/transformed.csv``.
    """
    import pandas as pd
    from scripts.transform import transform

    extracted_path = '/tmp/extracted.csv'
    df = pd.read_csv(extracted_path)
    transformed_df = transform(df)
    transformed_df.to_csv('/tmp/transformed.csv', index=False)


def model_to_temp(**context) -> None:
    """Train the fraud model and append predictions.

    Reads the transformed dataset from ``/tmp/transformed.csv``,
    trains a logistic regression model via the ``fraud_model``
    module and writes the resulting DataFrame with a
    ``FraudPrediction`` column to ``/tmp/predicted.csv``.
    """
    import pandas as pd
    from scripts.fraud_model import train_and_predict

    transformed_path = '/tmp/transformed.csv'
    df = pd.read_csv(transformed_path)
    predicted_df = train_and_predict(df)
    predicted_df.to_csv('/tmp/predicted.csv', index=False)


def load_to_db(**context) -> None:
    """Load the fraud predictions into the PostgreSQL database.

    Reads the predicted dataset from ``/tmp/predicted.csv`` and
    invokes the ``load`` function from the ``load`` module to
    persist the DataFrame into the target database. The default
    destination table name is ``transactions`` and the default
    database connection points at the Postgres service defined in
    ``docker-compose.yml``.
    """
    import pandas as pd
    from scripts.load import load

    predicted_path = '/tmp/predicted.csv'
    df = pd.read_csv(predicted_path)
    load(df)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
}


with DAG(
    dag_id='fraud_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    description='ETL pipeline for credit card fraud detection',
    catchup=False,
) as dag:
    # Define the individual ETL tasks using PythonOperator
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_to_temp,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_to_temp,
    )

    model_task = PythonOperator(
        task_id='fraud_model',
        python_callable=model_to_temp,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_to_db,
    )

    # Set the execution order
    extract_task >> transform_task >> model_task >> load_task
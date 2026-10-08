"""
This is a DAG for running the DOT!
It will:

    1. Loop through a list of Postgres objects (tables/views) in the data source
       database and copy them to the DOT database
    2. Run DOT
"""
import json
from os import system
import glob
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
# from airflow.models import DAG  # pylint: disable=import-error
# from airflow.operators.python import PythonOperator  # pylint: disable=import-error
# from airflow.operators.bash_operator import BashOperator  # pylint: disable=import-error
# from airflow.hooks.postgres_hook import PostgresHook  # pylint: disable=import-error
# from airflow.hooks.base import BaseHook  # pylint: disable=import-error
# from airflow.models import Variable  # pylint: disable=import-error
# from sqlalchemy import create_engine

def get_bigquery_client(bigquery_project_id):

    path_to_key_file = "./secrets/*.json"
    key_file = glob.glob(path_to_key_file)[0]

    if not key_file:
        raise FileNotFoundError(f"No Google Cloud key found in {path_to_key_file}")

    client = bigquery.Client.from_service_account_json(
        key_file,
        project= bigquery_project_id
    )
    return client

# print(glob.glob('./secrets/*.json'))

bigquery_project_id = 'staging-udts-01'
client = get_bigquery_client(bigquery_project_id= bigquery_project_id)


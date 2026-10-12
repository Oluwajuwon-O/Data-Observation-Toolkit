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

# from docker.airflow.dags.run_dot_project import object_name
# from airflow.models import DAG  # pylint: disable=import-error
# from airflow.operators.python import PythonOperator  # pylint: disable=import-error
# from airflow.operators.bash_operator import BashOperator  # pylint: disable=import-error
# from airflow.hooks.postgres_hook import PostgresHook  # pylint: disable=import-error
# from airflow.hooks.base import BaseHook  # pylint: disable=import-error
# from airflow.models import Variable  # pylint: disable=import-error
# from sqlalchemy import create_engine

def get_bigquery_client(bigquery_project_id):

    path_to_key_file = "./secrets/*.json"
    key_files = glob.glob(path_to_key_file)

    if not key_files or len(key_files) == 0:
        raise FileNotFoundError(f"No Google Cloud key found in {path_to_key_file}")

    key_file = key_files[0]
    client = bigquery.Client.from_service_account_json(
        key_file,
        project= bigquery_project_id
    )
    return client

# print(glob.glob('./secrets/*.json'))

# client = get_bigquery_client(bigquery_project_id= bigquery_project_id)

def get_object(
        object_name_in,
        earliest_date_to_sync,
        source_conn_in,
        source_schema_in,
        ):
    """

    Extracts data from object in source Postgres DB and saves to target DOT database in data schema.

    Parameters
    ----------
    object_name_in: String
       The Postgres db object to sync to DOT DB
    earliest_date_to_sync: String
        Only sync data after this date
    date_field: String
        Date field on each record for this object. Set None if one wasn't provided for DB object
    source_conn_in: String
       Airflow connection ID where data lives.
       Note, the connection name must exactly equal the db name.
    columns_to_exclude: Array
        A list of names to exclude from the sync
    """

    sql_stmt = (
            "SELECT * FROM "
            + "`" 
            + source_conn_in
            + "."
            + source_schema_in 
            + "."
            + object_name_in
            + "`"
    )
    print(sql_stmt)

    client = get_bigquery_client(bigquery_project_id= source_conn_in)
    rows = client.query(sql_stmt)
    result = rows.result()
    data = result.to_dataframe()

    bq_to_pg_types = {
    "STRING": "text",
    "INT64": "bigint",
    "FLOAT64": "double precision",
    "NUMERIC": "numeric",
    "INTEGER" : "bigint",
    "BIGNUMERIC": "numeric",
    "BOOL": "boolean",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "DATETIME": "timestamp",
    "TIMESTAMP": "timestamptz",
    "BYTES": "bytea",
    "JSON": "jsonb"
    }

    schema = result.schema
    column_list = [col.name for col in schema]
    type_list = [bq_to_pg_types[col.field_type] for col in schema]

    return data, column_list, type_list







config = json.loads(open("./dot_projects.json").read())

for project in config['dot_projects']:
    earliest_date_to_sync = project['earliest_date_to_sync']
    source_conn_in = project['source_connid']
    source_schema_in = project['source_schema']
    objects_to_sync = project['objects']
    for object in objects_to_sync:
        object_name_in = object['object']
        get_object(
                object_name_in = object_name_in,
                earliest_date_to_sync = earliest_date_to_sync,
                source_conn_in = source_conn_in,
                source_schema_in = source_schema_in
                )




    # pg_hook = PostgresHook(postgres_conn_id=source_conn_in, schema=source_conn_in)
    # pg_conn = pg_hook.get_conn()
    # cursor = pg_conn.cursor()
    # cursor.execute(sql_stmt)
    # data = cursor.fetchall()

    # sql_stmt = (
    #         "SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"
    #         + object_name_in
    #         + "'"
    #         + "AND table_schema = '"
    #         + source_schema_in
    #         + "' "
    #         + " ORDER BY ordinal_position "
    # )
    # print(sql_stmt)
    # pg_hook = PostgresHook(postgres_conn_id=source_conn_in, schema=source_conn_in)
    # pg_conn = pg_hook.get_conn()
    # cursor = pg_conn.cursor()
    # cursor.execute(sql_stmt)
    # columns = cursor.fetchall()

    # # Fail back to views
    # if len(columns) == 0:
    #     sql_stmt = (
    #             "SELECT a.attname as \"column_name\","
    #             + " pg_catalog.format_type(a.atttypid, a.atttypmod) as \"data_type\" "
    #             + " FROM pg_attribute a "
    #             + "  JOIN pg_class t on a.attrelid = t.oid "
    #             + "  JOIN pg_namespace s on t.relnamespace = s.oid "
    #             + " WHERE a.attnum > 0 "
    #             + "  AND NOT a.attisdropped "
    #             + " AND t.relname = '" + object_name_in + "' "
    #             + " AND s.nspname = '" + source_schema_in + "' "
    #             + " ORDER BY a.attnum; "
    #     )
    #     print(sql_stmt)
    #     pg_hook = PostgresHook(postgres_conn_id=source_conn_in, schema=source_conn_in)
    #     pg_conn = pg_hook.get_conn()
    #     cursor = pg_conn.cursor()
    #     cursor.execute(sql_stmt)
    #     columns = cursor.fetchall()

    # # Convert to a clean list (Tuples are duplicated)
    # column_list = []
    # cols = [a[0] for a in columns]
    # for col in list(cols):
    #     if "(" not in col:
    #         column_list.append(col)

    # type_list = []
    # types = [a[1] for a in columns]
    # for type in list(types):
    #     if "(" not in col:
    #         type_list.append(type)

    # # Remove any PII columns as set in JSON file 'columns_to_exclude' for the entity.
    # data = pd.DataFrame(data=data, columns=column_list)
    # data.drop(columns_to_exclude, inplace=True, axis=1)
    # indices = []
    # for i in range(0, len(column_list)):
    #     if column_list[i] in columns_to_exclude:
    #         indices.append(i)

    # for i in reversed(indices):
    #     del column_list[i]
    #     del type_list[i]

    # print(column_list)
    # print(type_list)
    # print(data)

    # return data, column_list, type_list



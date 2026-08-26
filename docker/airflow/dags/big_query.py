import os 
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./staging-udts-01-26edf3092bf7.json"

client = bigquery.Client(project="staging-udts-01")

query = """
SELECT 1 as test_col
"""
query_job = client.query(query)
result = query_job.result()

for row in result:
    print(row.test_col)
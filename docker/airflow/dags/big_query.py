import os 
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./staging-udts-01-26edf3092bf7.json"

client = bigquery.Client(project="staging-udts-01")
sql = "SELECT * FROM `staging-udts-01.test_spa_connection.course`"
rows = client.query(sql).result()
data = rows.to_dataframe()

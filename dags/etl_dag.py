from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from script.extract_data import etl
from script.load_from_db_to_s3 import load_from_db_to_s3

# def etl():
#     pass

dag = DAG(
    'project_databricks_etl_dag', start_date=datetime(2023,1 ,1),
    schedule_interval=timedelta(hours=1),
    catchup = False, description="ETL process from api to database",
    tags = ['data_science']
)

etl_from_api_to_db = PythonOperator(
    task_id = 'etl_data_from_api_to_db',
    python_callable =etl,
    dag = dag
)

load_from_db_to_s3 = PythonOperator(
    task_id = 'load_from_db_to_s3',
    python_callable = load_from_db_to_s3,
    dag = dag
)

etl_from_api_to_db >> load_from_db_to_s3
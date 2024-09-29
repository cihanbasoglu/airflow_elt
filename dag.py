from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
import ingest_task

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'retries': 3,
}

def load_file(filepath):
    with open(filepath, 'r') as file:
        return file.read()

with DAG('bigquery_data_pipeline_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    ingest_data_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_task,
    )

    sql_task = load_file('sql_task.sql')
    sql_staging_task = BigQueryInsertJobOperator(
        task_id='stage_data',
        configuration={
            "query": {
                "query": sql_task,
                "useLegacySql": False
            }
        },
        location='US'
    )

    # lineage
    ingest_data_task >> sql_staging_task

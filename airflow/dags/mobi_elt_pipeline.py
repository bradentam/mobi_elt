from os import remove
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

import sys
sys.path.append('/opt/airflow/modules/')
import mobi_functions as mf


"""
DAG to extract mobi data, load into AWS S3, and copy to AWS Redshift
"""


# Run our DAG monthly and ensures DAG run will kick off
# once Airflow is started, as it will try to "catch up"
schedule_interval = "@monthly"
start_date = datetime(2024, 1, 1)

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id="elt_mobi_pipeline",
    description="Mobi ELT",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["MobiETL"],
) as dag:

    extract_mobi_data = PythonOperator(
        task_id="extract_mobi_data",
        python_callable=mf.download_link,
        dag=dag,
    )

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=mf.upload_to_s3,
        dag=dag,
    )
extract_mobi_data >> upload_to_s3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator   # ✅ FIXED
from datetime import datetime

# ✅ Imports
from src.etl_scripts.extract import extract_sales_data
from src.etl_scripts.transform import transform_sales_data
from src.etl_scripts.load import run_load_pipeline
from src.etl_scripts.data_quality import (
    check_staged_data_quality,
    check_loaded_data_quality
)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="ecom_etl_pipeline",
    default_args=default_args,
    schedule="@daily",   # ✅ updated
    catchup=False
) as dag:

    # START
    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # EXTRACT
    extract_task = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_sales_data
    )

    # PRE-LOAD DATA QUALITY
    check_staged_data = PythonOperator(
        task_id="check_staged_data_quality",
        python_callable=check_staged_data_quality
    )

    # TRANSFORM
    transform_task = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform_sales_data
    )

    # LOAD
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=run_load_pipeline
    )

    # POST-LOAD DATA QUALITY
    check_loaded_data = PythonOperator(
        task_id="check_loaded_data_quality",
        python_callable=check_loaded_data_quality
    )

    # END
    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # FLOW
    start_pipeline >> extract_task >> check_staged_data >> transform_task >> load_task >> check_loaded_data >> end_pipeline
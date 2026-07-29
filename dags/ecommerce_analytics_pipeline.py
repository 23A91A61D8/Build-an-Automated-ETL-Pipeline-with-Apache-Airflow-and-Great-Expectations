import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import modular ETL tasks from the etl_scripts package
from etl_scripts import (
    download_raw_data,
    process_bronze_layer,
    validate_bronze,
    process_silver_layer,
    validate_silver,
    load_to_analytical_store
)

# Define standard default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Fetch the raw dataset source URL from environment config
DATA_URL = os.environ.get(
    'DATA_URL',
    'https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv'
)

# Instantiate the DAG
with DAG(
    dag_id='ecommerce_analytics_pipeline',
    default_args=default_args,
    description='Automated ETL Pipeline with Apache Airflow and Great Expectations',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 1. Download Raw Data Task
    download_raw_data_task = PythonOperator(
        task_id='download_raw_data',
        python_callable=download_raw_data,
        op_kwargs={
            'url': DATA_URL,
            'output_path': 'data/raw/raw_data.csv'
        }
    )

    # 2. Bronze Processing Task
    bronze_layer_processing_task = PythonOperator(
        task_id='bronze_layer_processing',
        python_callable=process_bronze_layer,
        op_kwargs={
            'input_path': 'data/raw/raw_data.csv',
            'output_dir': 'data/bronze'
        }
    )

    # 3. Bronze Data Validation Task
    bronze_data_validation_task = PythonOperator(
        task_id='bronze_data_validation',
        python_callable=validate_bronze,
        op_kwargs={
            'data_dir': 'data/bronze'
        }
    )

    # 4. Silver Transformation Task
    silver_layer_transformation_task = PythonOperator(
        task_id='silver_layer_transformation',
        python_callable=process_silver_layer,
        op_kwargs={
            'input_dir': 'data/bronze',
            'output_dir': 'data/silver'
        }
    )

    # 5. Silver Data Validation Task
    silver_data_validation_task = PythonOperator(
        task_id='silver_data_validation',
        python_callable=validate_silver,
        op_kwargs={
            'data_dir': 'data/silver'
        }
    )

    # 6. Load to Serving Store Task
    load_to_analytical_store_task = PythonOperator(
        task_id='load_to_analytical_store',
        python_callable=load_to_analytical_store,
        op_kwargs={
            'input_dir': 'data/silver',
            'db_path': 'data/analytics.db'
        }
    )

    # Enforce task sequencing
    (
        download_raw_data_task
        >> bronze_layer_processing_task
        >> bronze_data_validation_task
        >> silver_layer_transformation_task
        >> silver_data_validation_task
        >> load_to_analytical_store_task
    )

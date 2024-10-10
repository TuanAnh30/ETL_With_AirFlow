from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import sys


default_args={
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_polars():
    import polars 
    print(f'polar version: {polars.__version__}')
def get_pandas():
    import pandas  # type: ignore
    print(f'pandas version: {pandas.__version__}')

def get_ijson():
    import ijson  # type: ignore
    print(f'ijson version: {ijson.__version__}')
def print_python_version():
    print(f"Python version: {sys.version}")
with DAG(
    default_args=default_args,
    dag_id='dag_with_python_dependencies_v3',
    start_date=datetime(2024,10,1),
    schedule='@daily'
) as dag:
    get_polars = PythonOperator(
        task_id='get_polars',
        python_callable=get_polars
    )
    get_pandas = PythonOperator(
        task_id='get_pandas',
        python_callable=get_pandas
    )
    get_ijson = PythonOperator(
        task_id='get_ijson',
        python_callable=get_ijson
    )
    get_python = PythonOperator(
        task_id="print_version",
        python_callable=print_python_version
    )
    get_pandas, get_polars, get_ijson, get_python
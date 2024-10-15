from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/dags/ETLProject')
from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from main_def import check, process_gzip, transfrom_data, create_table, insert_data

default_args ={
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag =  DAG(
    dag_id='dag_ETL_JsonData',
    default_args=default_args,
    start_date=datetime(2024,10,10),
    schedule='@daily'
)
# dag_10minutes = DAG(
#     dag_id='dag_ETL_JsonData',
#     default_args=default_args,
#     start_date=datetime(2024, 10, 12),
#     schedule='*/10 * * * *'
# )
Check_folder = PythonOperator(
    task_id='Check_Folder',
    python_callable=check,
    provide_context=True,
    dag=dag
)
Extract_File_Step = PythonOperator(
    task_id='Extract_Gzip_To_Json',
    python_callable=process_gzip,
    # op_kwargs={'file' : gzip_path},
    provide_context=True,
    dag = dag
)
Transfrom_Data_Step = PythonOperator(
    task_id='Transfrom_Data_To_DataFrame',
    python_callable=transfrom_data,
    provide_context=True,
    dag = dag
)
create_table1 = PostgresOperator(
    task_id='Create_Table',
    postgres_conn_id='postgres_localhost',
    sql=create_table(),
    dag=dag
)
Load_Data_Step = PythonOperator(
    task_id='Load_Data',
    python_callable=insert_data,
    provide_context=True,
    dag=dag
)
# unzip_gzipfile >> process_data >> load_data
Check_folder 
# >> Extract_File_Step >> Transfrom_Data_Step >> Load_Data_Step
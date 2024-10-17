from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/dags/ETLProject')
from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.operators.python import PythonOperator
from main_def import check, process_gzip, transfrom_data, create_table, insert_data, save_file
import pendulum

local_timezone = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args ={
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
dag =  DAG(
    dag_id='dag_ETL_JsonData',
    default_args=default_args,
    start_date=datetime(2024, 10, 15, tzinfo=local_timezone),
    end_date=datetime(2024, 10, 17, tzinfo=local_timezone),
    schedule='10 05 * * *',
)
Check_folder = PythonOperator(
    task_id='Check_Folder',
    python_callable=check,
    provide_context=True,
    dag=dag
)
Extract_File_Step = PythonOperator(
    task_id='Extract_Gzip_To_Json',
    python_callable=process_gzip,
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
Save_file = PythonOperator(
    task_id='Save_File',
    python_callable=save_file,
    provide_context=True,
    dag=dag
)
Check_folder >> Extract_File_Step >> [Transfrom_Data_Step, Save_file]

# >> Load_Data_Step 
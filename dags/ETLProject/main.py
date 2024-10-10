from datetime import datetime, timedelta
import gzip
import polars as pl
import ijson
import json
import sys
sys.path.append('/opt/airflow/dags/ETLProject')
import process_vietnamese
from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from psycopg2 import sql
from airflow.models import Variable
from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
import os
import re

# Unzip Def
def process_gzip(ti, file):
    try:
        with gzip.open(file, 'rb') as f:
            json_item = list(ijson.items(f, '', multiple_values=True))
        ti.xcom_push(key='json_item',value=json_item)
    except Exception as e:
        print(f'Lỗi khi giải nén file:{e}')

# Process File Json    
def transfrom_data(ti):
    try:
        json_item = ti.xcom_pull(task_ids='Extract_Gzip_To_Json', key='json_item')
        if not json_item: 
            print("Không tìm thấy dữ liệu")
            return
        data = []
        for item in json_item:
            # Kiểm tra xem item có chứa các trường mong đợi không
            if isinstance(item, dict):
                fluentd_time = item.get('fluentd_time')
                if fluentd_time:
                    try:
                        dt_with_tz = datetime.strptime(fluentd_time, '%Y-%m-%d %H:%M:%S %z')
                        dt_with_tz = dt_with_tz.replace(hour=0, minute=0, second=0)
                        # Chuyển đổi sang định dạng ISO 8601
                        parsed_time = dt_with_tz.isoformat()  # Lấy ngày
                    except Exception as e:
                        print(e)
                        parsed_time = ''
                else:
                    parsed_time = None
                search_term = item.get('search_term', '')
                if search_term:
                    search_term = process_vietnamese.normalize_diacritics(search_term)
                # Xây dựng record
                record = {
                    'date': parsed_time,
                    'track_id': item.get('track_id', ''),
                    'page_id': item.get('page_id', ''),
                    'block_id': item.get('block_id', ''),
                    'search_term': search_term,
                    'position': item.get('position', ''),
                    'region': item.get('region', 0),
                    'platform': item.get('platform', ''),
                    'count_event': 0,
                    'max_position': 0
                }
                data.append(record)
            else:
                print('không chứa item cần lấy')
        df = process_df(convert_to_df(data))
        print(df)
        df = df.to_dicts()
        ti.xcom_push(key='df_processed', value=df)
    except Exception as e:
        print(f"Lỗi: {e}")
        return []

#Convert File To DataFrame
def convert_to_df(data_json):
    try:
        df = pl.DataFrame(data_json)
        return df
    except Exception as e:
        print(f"Lỗi khi tạo DataFrame: {e}")
        return None

#Process DataFrame
def process_df(df):
    result_df = df.group_by(['date', 'track_id', 'page_id', 'search_term', 'block_id', 'region', 'platform']).agg([
        pl.col('position').max().alias('max_position'),
        pl.len().alias('count_event')
    ])
    return result_df

#Process Final DataFrame
def process_df_final(df):
    list_df=[]
    if df is not None:
        list_df.append(df)
    if list_df:
        result_df = pl.concat(list_df)
        final_df = result_df.group_by(['date', 'track_id', 'page_id', 'search_term', 'block_id', 'region', 'platform']).agg([
            pl.col('max_position').max(),
            pl.sum('count_event'),
        ])
        return final_df
    else:
        return None

#Load data in to database 
def insert_data(ti):
    table = 'test1'
    dict_df = ti.xcom_pull(task_ids='Transfrom_Data_To_DataFrame', key='df_processed')
    records = pl.DataFrame(dict_df)

    if records.is_empty() == True:
        print("Không có data")
        return
    
    print(f"Số lượng bản ghi: {records.shape[0]}")
    print(records)
    columns = [col.lower() for col in records.columns]
    print("Insert các cột: ", columns)
    print("Table: ", table)

    value_placeholders = ", ".join(["%s"] * len(columns))
    insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({value_placeholders}) "
    try:
        values = []
        for row in records.iter_rows():
            converted_row = []
            for i, value in enumerate(row):
                col_name = columns[i]
                if col_name == ['max_position', 'count_event']:
                    try:
                        converted_row.append(int(value) if value is not None else 0)
                    except ValueError:
                        print(f"Giá trị không hợp lệ cho cột '{col_name}': {value}")
                        converted_row.append(0)
                else:
                    converted_row.append(value)
            values.append(tuple(converted_row))
        postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_query, values)
                conn.commit()
        print("Đã insert dữ liệu thành công")
    except Exception as e:
        print("Insert thất bại")
        print(e)

#Create table
def create_table():
    table = 'test1'
    key_table = ['date', 'track_id', 'page_id', 'block_id',
                'search_term', 'position', 'region',
                'platform', 'max_position', 'count_event']
    try:
        columns = {}
        for column in key_table:
            if column in ['max_position', 'count_event', 'region']:
                columns[column] = 'INTEGER'
            elif column in ['date']:
                columns[column] = 'TIMESTAMP WITH TIME ZONE'
            else:
                columns[column] = 'VARCHAR'  # Hoặc kiểu dữ liệu mặc định khác
        column_definitions = ", ".join(
            f"{col_name} {col_type}" for col_name, col_type in columns.items()
        )
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table} ({column_definitions})"
        return create_table_query
    except Exception as e:
        print("Lỗi: ",e)

PROCESSED_FILES = []
def check_folder_new(folder_path, **kwargs):
    today = datetime.now().strftime('%Y%m%d')
    print(f'Check for files from today: {today}')
    day='20240830'
    file_pattern = r'og_item_impression\.(\d{8})_\d{4}_\d\.json\.gz'
    files = os.listdir(folder_path)
    unprocessed_files = []
    for file_name in files:
        match = re.match(file_pattern, file_name)
        if match and file_name not in PROCESSED_FILES:
            file_date = match.group(1)
            if file_date == day:
                file_path = os.path.join(folder_path, file_name)
                unprocessed_files.append(file_path)
                print(f'File to process: {file_path}')
            else:
                print(f'File {file_name} is not {day}. Skip ...')
        else:
            print(f'File {file_name} đã được xử lý hoặc không phù hợp. Skip...')
    kwargs['ti'].xcom_push(key='unprocessed_files', value=unprocessed_files)

    today_files = [f for f in files if re.match(file_pattern, f) and re.match(file_pattern, f).group(1) == day]
    kwargs['ti'].xcom_push(key='file_list', value=today_files)
    print(f"File in folder '{day}': {today_files}")
path="dags/sqllab_orderuserbystep_20240923T021501.csv"
gzip_path = "dags/og_item_impression.20240830_2030_3.json.gz"
folder = "dags/ETLProject/og_item_impression.20240830_"

processed_list_path = "dag/processed_list.txt"
file_path = Path(path).resolve()
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
Extract_File_Step = PythonOperator(
    task_id='Extract_Gzip_To_Json',
    python_callable=process_gzip,
    op_kwargs={'file' : gzip_path},
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
check_file = PythonOperator(
    task_id='check_folder',
    python_callable=check_folder_new,
    op_kwargs={
        'folder_path': folder,
    },
    provide_context=True,
    dag=dag
)
def test_process2(file_path):
    print(f'Process {file_path}...')
    pass
def test_process(**kwargs):
    unprocessed_files = kwargs['ti'].xcom_pull(task_ids='check_folder', key='file_list')
    if not unprocessed_files: 
        print('No files to process')
        return
    for file_path in unprocessed_files:
        print(f'Processing file: {file_path}')
        test_process2(file_path)
        file_name = os.path.basename(file_path)
        PROCESSED_FILES.append(file_name)
        print(f'Add {file_name} to processed files')
    kwargs['ti'].xcom_push(key='list_processed_file', value=PROCESSED_FILES)
test1 = PythonOperator(
    task_id='test1',
    python_callable=test_process,
    provide_context=True,
    dag=dag
)
# unzip_gzipfile >> process_data >> load_data
check_file >> test1
from datetime import datetime, timedelta
import gzip
import polars as pl
import ijson
import sys
sys.path.append('/opt/airflow/dags/ETLProject')
import process_vietnamese
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
import os
import pendulum

#Check folder by real time 
def check(**kwargs):
    # Take time of airflow
    current_time = kwargs['ts_nodash']  # 'YYYYMMDDTHHMMSS'
    dt = pendulum.parse(current_time, tz='UTC').in_tz('Asia/Ho_Chi_Minh')
    formatted_time = dt.format('YYYYMMDD_HHmm')
    file_list = []
    # Check all file in folder
    for filename in os.listdir(folder):
        # Check file
        if filename.endswith('.json.gz') and (formatted_time) in filename:
            filepath = os.path.join(folder, filename)
            print(f'File to process: {filepath}')
            file_list.append(filepath)
    if not file_list: 
        print(f'Not file need process at {formatted_time}')
    kwargs['ti'].xcom_push(key='file_to_process', value=file_list)
    print('Complete check file.')

#Check folder by list file processed 
def check_new(**kwargs):
    files_in_parquet = set(os.path.splitext(file)[0] for file in os.listdir(folder_parquet))
    # Check if the files in folder1 exist in folder2
    list_file = []
    for file in os.listdir(folder):
        # Lấy phần tên file bỏ cả hai phần mở rộng
        file_name_without_ext = os.path.splitext(os.path.splitext(file)[0])[0]
        
        #Get the file name and remove both extensions
        if file_name_without_ext not in files_in_parquet:
            # Append full path to list_file
            full_path = os.path.join(folder, file)
            list_file.append(full_path)
    if list_file:
        print(f'List file plan to process: {list_file}')
    else: 
        print('No file need process')
    # Print list file not in parquet_folder
    kwargs['ti'].xcom_push(key='file_to_process', value=list_file)
# Unzip Def
def process_gzip(**kwargs):
    # Pull list file from XCom
    files = kwargs['ti'].xcom_pull(key='file_to_process', task_ids='Check_Folder')  
    if not files:
        print('No file need process')
        return
    all_json_items = []
    # Read all file in list 
    for file in files:  
        print(f'Processing file: {file}')
        with gzip.open(file, 'rb') as f:
            # Read items JSON from file
            json_items = list(ijson.items(f, '', multiple_values=True))  
            # Merge data into a consolidated list
            all_json_items.extend(json_items)  
            print(f'Complete process: {file}')
    # Push merge data to XCom
    kwargs['ti'].xcom_push(key='all_json_items', value=all_json_items)
    print(f'Total number of merged JSON items: {len(all_json_items)}')

def save_file(**kwargs): 
    list_file = kwargs['ti'].xcom_pull(key='file_to_process', task_ids='Check_Folder')
    folder_parquet = "dags/ETLProject/parquet_file"
    if not list_file:
        print('No file need process')
        return
    # Read all file in list
    for file in list_file:
        print(f'Processing {file} ...')
        with gzip.open(file, 'rb') as f:
            #Transfrom file to json item
            json_items = list(ijson.items(f, '', multiple_values=True))
            #Load into dataframe polars
            df = pl.from_records(json_items)
            #Process data
            df = df.with_columns(
                pl.col("fluentd_time").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S %z")
                .dt.replace_time_zone("Asia/Ho_Chi_Minh")
                .dt.date()
                .cast(pl.Datetime)
                .alias("date")
            )
            df = df.with_columns(
                pl.col('region').cast(pl.Int8)
            )
            #Reduce data
            df = df.drop(['HTTP_HOST', 'event_name', 'fluentd_time',
                            'utm_term', 'parent_dish_id'])
            df = df.fill_null('')
            #Determine the file name
            name_file = os.path.splitext(os.path.splitext(file)[0])[0]
            name_file = os.path.basename(name_file)
            parquet_file_name = f"{name_file}.parquet" 
            #Determine the file path
            file_path_parquet = os.path.join(folder_parquet, f"{name_file}.parquet")
            print(file_path_parquet)
            #Save data to file parquet
            if not os.path.exists(file_path_parquet):
                df.write_parquet(file_path_parquet)
                print(f'Save as: {parquet_file_name}')
            else:
                print(f'File already exists: {parquet_file_name}')

# Process File Json    
def transfrom_data(**kwargs):
    json_item = kwargs['ti'].xcom_pull(task_ids='Extract_Gzip_To_Json', key='all_json_items')
    if not json_item: 
        print("No data found")
        return
    data = []
    for item in json_item:
        # Check if item contains expected fields
        if isinstance(item, dict):
            fluentd_time = item.get('fluentd_time')
            if fluentd_time:
                try:
                    dt_with_tz = datetime.strptime(fluentd_time, '%Y-%m-%d %H:%M:%S %z')
                    dt_with_tz = dt_with_tz.replace(hour=0, minute=0, second=0)
                    # Transform to ISO 8601
                    parsed_time = dt_with_tz.isoformat()
                except Exception as e:
                    print(e)
                    parsed_time = ''
            else:
                parsed_time = None
            search_term = item.get('search_term', '')
            if search_term:
                search_term = process_vietnamese.normalize_diacritics(search_term)
            # Build record
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
            print('Not contain the item to be retrieved')
    df = pl.DataFrame(data)
    df = process_df(df)
    df = df.group_by(['date', 'track_id', 'page_id', 'search_term', 'block_id', 'region', 'platform']).agg([
        pl.col('max_position').max(),
        pl.sum('count_event'),
    ])
    print(df)
    df = df.to_dicts()
    kwargs['ti'].xcom_push(key='df_processed', value=df)

#Process DataFrame
def process_df(df):
    result_df = df.group_by(['date', 'track_id', 'page_id', 'search_term', 'block_id', 'region', 'platform']).agg([
        pl.col('position').max().alias('max_position'),
        pl.len().alias('count_event')
    ])
    return result_df
    
#Load data in to database 
def insert_data(**kwargs):
    table = 'test1'
    dict_df = kwargs['ti'].xcom_pull(task_ids='Transfrom_Data_To_DataFrame', key='df_processed')
    records = pl.DataFrame(dict_df)

    if records.is_empty() == True:
        print("No data found")
        return
    
    print(f"Number of record: {records.shape[0]}")
    print(records)
    columns = [col.lower() for col in records.columns]
    print("Insert columns: ", columns)
    print("Table: ", table)

    value_placeholders = ", ".join(["%s"] * len(columns))
    insert_query = f"""
    INSERT INTO {table} ({', '.join(columns)}) 
    VALUES ({value_placeholders}) 
    ON CONFLICT (date, track_id, page_id, search_term, block_id, region, platform) 
    DO UPDATE SET max_position = GREATEST({table}.max_position, EXCLUDED.max_position),
                  count_event = {table}.count_event + EXCLUDED.count_event"""
    values = []
    for row in records.iter_rows():
        converted_row = []
        for i, value in enumerate(row):
            col_name = columns[i]
            if col_name == ['max_position', 'count_event']:
                try:
                    converted_row.append(int(value) if value is not None else 0)
                except ValueError:
                    print(f"Invalid value for column '{col_name}': {value}")
                    converted_row.append(0)
            else:
                converted_row.append(value)
        values.append(tuple(converted_row))
    postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, values)
            conn.commit()
    print("Success!!")

#Create table
def create_table():
    table = 'test1'
    key_table = ['date', 'track_id', 'page_id', 'block_id',
                'search_term', 'position', 'region',
                'platform', 'max_position', 'count_event']
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

folder = "dags/ETLProject/og_item_impression.20240830_"
folder_parquet = "dags/ETLProject/parquet_file"

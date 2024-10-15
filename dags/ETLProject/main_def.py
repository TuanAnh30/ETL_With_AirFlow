from datetime import datetime, timedelta
import gzip
import polars as pl
import ijson
import sys
sys.path.append('/opt/airflow/dags/ETLProject')
import process_vietnamese
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
import os

#Check folder
def check(**kwargs):
    execution_date = kwargs['ds']  # Biến ds sẽ có định dạng YYYY-MM-DD
    today = execution_date.replace('-', '')  # Chuyển thành định dạng YYYYMMDD
    for filename in os.listdir(folder):
        if filename.endswith('.json.gz') and today in filename:  # Chỉ kiểm tra các file chứa ngày hôm nay
            filepath = os.path.join(folder, filename)
            print(f'File mới cần xử lý: {filepath}')
            kwargs['ti'].xcom_push(key='file_to_process', value=filepath)
            return  # Sau khi tìm thấy file cần xử lý, kết thúc hàm
    print('Không có file cần xử lý cho ngày hôm nay')
    kwargs['ti'].xcom_push(key='file_to_process', value=None)
#Load list processed file
def load_list(): 
    today = datetime.today()
    if os.path.exists(processed_file_list):
        with open(processed_file_list, 'r') as f: 
            return set(line.strip() for line in f)
    return set()
#Update list processed file
def update_list_file(file):
    with open(processed_file_list, 'a') as f: 
        f.write(f'{file}\n')

# Unzip Def
def process_gzip(**kwargs):
    try:
        file = kwargs['ti'].xcom_pull(key='file_to_process', task_ids='Check_Folder')
        print(file)
        if file:
            print(f'Process: {file}')
        else:
            print('Ko có file')        
        with gzip.open(file, 'rb') as f:
            json_item = list(ijson.items(f, '', multiple_values=True))
        kwargs['ti'].xcom_push(key='json_item',value=json_item)
    except Exception as e:
        print(f'Lỗi khi giải nén file:{e}')

# Process File Json    
def transfrom_data(**kwargs):
    try:
        json_item = kwargs['ti'].xcom_pull(task_ids='Extract_Gzip_To_Json', key='json_item')
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
        kwargs['ti'].xcom_push(key='df_processed', value=df)
    except Exception as e:
        print(f"Lỗi: {e}")
        return []

def process_multifile(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='Transfrom_Data_To_DataFrame', key='df_processed')
    list_df=[]
    if df is not None:
        list_df.append(df)
    if list_df:
        result_df = pl.concat(list_df)
        final_df = result_df.group_by(['date', 'track_id', 'page_id', 'search_term', 'block_id', 'region', 'platform']).agg([
            pl.col('max_position').max(),
            pl.sum('count_event'),
        ])
        kwargs['ti'].xcom_push(key='df_processed', value=final_df)
    else:
        kwargs['ti'].xcom_push(key='df_processed', value=None)
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

# def update_database():
    
#Load data in to database 
def insert_data(**kwargs):
    table = 'test1'
    dict_df = kwargs['ti'].xcom_pull(task_ids='Transfrom_Data_To_DataFrame', key='df_processed')
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
    insert_query = f"""
    INSERT INTO {table} ({', '.join(columns)}) 
    VALUES ({value_placeholders}) 
    ON CONFLICT (date, track_id, page_id, search_term, block_id, region, platform) 
    DO UPDATE SET max_position = GREATEST({table}.max_position, EXCLUDED.max_position),
                  count_event = {table}.count_event + EXCLUDED.count_event"""
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
        file = kwargs['ti'].xcom_pull(key='file_to_process', task_ids='Check_Folder')
        update_list_file(file)
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
folder = "dags/ETLProject/og_item_impression.20240830_"
processed_file_list = os.path.join(folder, 'list_processed_file.txt')
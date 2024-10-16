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
    # Lấy thời gian hiện tại mà Airflow chạy
    current_time = kwargs['ts_nodash']  # 20241016T022236
    formatted_time = f"{current_time[:8]}_{current_time[9:13]}"  # có định dạng YYYYMMDDTHHMMSS
    print(formatted_time)
    file_list = []
    # Duyệt qua các file trong folder
    for filename in os.listdir(folder):
        # Kiểm tra file có định dạng đúng và thuộc ngày hiện tại
        if filename.endswith('.json.gz') and (formatted_time) in filename:
            filepath = os.path.join(folder, filename)
            print(f'File mới cần xử lý: {filepath}')
            file_list.append(filepath)
    if not file_list: 
        print(f'Không có file cần thực hiện lúc {formatted_time}')
    kwargs['ti'].xcom_push(key='file_to_process', value=file_list)
    print('Hoàn thành kiểm tra file.')
#Update list processed file
def update_list_file(file):
    with open(processed_file_list, 'a') as f: 
        f.write(f'{file}\n')

# Unzip Def
def process_gzip(**kwargs):
    # Lấy danh sách file từ XCom
    files = kwargs['ti'].xcom_pull(key='file_to_process', task_ids='Check_Folder')  
    if not files:  # Kiểm tra nếu không có file
        print('Không có file để xử lý')
        return
    all_json_items = []  # Tạo danh sách để chứa tất cả các mục JSON từ các file
    for file in files:  # Lặp qua từng file trong danh sách
        try:
            print(f'Processing file: {file}')
            with gzip.open(file, 'rb') as f:
                json_items = list(ijson.items(f, '', multiple_values=True))  # Đọc các item JSON từ file
                all_json_items.extend(json_items)  # Gộp dữ liệu vào danh sách tổng hợp
                print(f'Đã giải nén file: {file}')
        except Exception as e:
            print(f'Lỗi khi giải nén file {file}: {e}')
    
    # Đẩy dữ liệu tổng hợp từ tất cả các file lên XCom
    kwargs['ti'].xcom_push(key='all_json_items', value=all_json_items)
    print(f'Tổng số item JSON đã gộp: {len(all_json_items)}')

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
        df = df.group_by(['date', 'track_id', 'page_id', 'search_term', 'block_id', 'region', 'platform']).agg([
            pl.col('max_position').max(),
            pl.sum('count_event'),
        ])
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
        # file = kwargs['ti'].xcom_pull(key='file_to_process', task_ids='Check_Folder')
        # update_list_file(file)
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
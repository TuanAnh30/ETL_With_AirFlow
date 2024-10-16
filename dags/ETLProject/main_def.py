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

#Check folder
def check(**kwargs):
    # Lấy thời gian hiện tại mà Airflow chạy
    current_time = kwargs['ts_nodash']  # 'YYYYMMDDTHHMMSS'
    dt = pendulum.parse(current_time, tz='UTC').in_tz('Asia/Ho_Chi_Minh')
    formatted_time = dt.format('YYYYMMDD_HHmm')
    # có định dạng YYYYMMDDTHHMMSS
    # formatted_time = f"{current_time[:8]}_{current_time[9:13]}"
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

# Unzip Def
def process_gzip(**kwargs):
    # Lấy danh sách file từ XCom
    files = kwargs['ti'].xcom_pull(key='file_to_process', task_ids='Check_Folder')  
    if not files:
        print('Không có file để xử lý')
        return
    all_json_items = []
    # Lặp qua từng file trong danh sách
    for file in files:  
        try:
            print(f'Processing file: {file}')
            with gzip.open(file, 'rb') as f:
                # Đọc các item JSON từ file
                json_items = list(ijson.items(f, '', multiple_values=True))  
                # Gộp dữ liệu vào danh sách tổng hợp
                all_json_items.extend(json_items)  
                print(f'Đã giải nén file: {file}')
        except Exception as e:
            print(f'Lỗi khi giải nén file {file}: {e}')
    # Đẩy dữ liệu tổng hợp từ tất cả các file lên XCom
    kwargs['ti'].xcom_push(key='all_json_items', value=all_json_items)
    print(f'Tổng số item JSON đã gộp: {len(all_json_items)}')

def save_file(**kwargs): 
    list_file = kwargs['ti'].xcom_pull(key='file_to_process', task_ids='Check_Folder')
    folder_parquet = "dags/ETLProject/parquet_file"
    if not list_file:
        print('Không có file để xử lý')
        return
    # Lặp qua từng file trong danh sách
    for file in list_file:
        print(f'Processing {file} ...')
        with gzip.open(file, 'rb') as f:
            json_items = list(ijson.items(f, '', multiple_values=True))
            df = pl.from_records(json_items)
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
            df = df.drop(['HTTP_HOST', 'event_name', 'fluentd_time',
                            'utm_term', 'parent_dish_id'])
            df = df.fill_null('')
            name_file = os.path.splitext(os.path.splitext(file)[0])[0]
            name_file = os.path.basename(name_file)
            print('Name file: ',name_file)
            parquet_file_name = f"{name_file}.parquet"
            print(parquet_file_name)
            file_path_parquet = os.path.join(folder_parquet, f"{name_file}.parquet")
            print(file_path_parquet)
            if not os.path.exists(file_path_parquet):
                df.write_parquet(file_path_parquet)
                print(f'Đã lưu vào file: {parquet_file_name}')
            else:
                print(f'Đã tồn tại file: {parquet_file_name}')

# Process File Json    
def transfrom_data(**kwargs):
    json_item = kwargs['ti'].xcom_pull(task_ids='Extract_Gzip_To_Json', key='all_json_items')
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
                    parsed_time = dt_with_tz.isoformat()
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

#Process multi file to save record
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

from psycopg2 import sql
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore

def create_table(table_name, table):
    try:
        columns = {}
        for column in table:
            if column in ['max_position', 'count_event', 'region']:
                columns[column] = 'INTEGER'
            elif column in ['date']:
                columns[column] = 'TIMESTAMP WITH TIME ZONE'
            else:
                columns[column] = 'VARCHAR'  # Hoặc kiểu dữ liệu mặc định khác
        column_definitions = ", ".join(
            f"{col_name} {col_type}" for col_name, col_type in columns.items()
        )
        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {table_name} ({column})"
        ).format(
            table_name=sql.Identifier(table_name),
            column=sql.SQL(column_definitions)
        )
        return create_table_query
    except Exception as e:
        print(e)

def insert_data(records, table):
    if records.is_empty() == True:
        print("Không có data")
        return
    column = records.columns
    columns = [col.lower() for col in column]
    print("Insert các cột: ", columns)
    print("Table: ", table)
    value_placeholders = ", ".join(["%s"] * len(columns))
    insert_query = sql.SQL(
        """INSERT INTO {table_name} ({columns}) VALUES ({value_placeholders}) 
        
        """
    ).format(
        table_name=sql.Identifier(table),
        columns=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
        value_placeholders=sql.SQL(value_placeholders)
    )
    try:
        values = []
        for row in records.iter_rows():
            converted_row = []
            for i, value in enumerate(row):
                col_name = columns[i]
                if col_name == 'max_position' or col_name == 'count_event':
                    try:
                        converted_row.append(int(value) if value is not None else 0)
                    except ValueError:
                        print(f"Giá trị không hợp lệ cho cột '{col_name}': {value}")
                        converted_row.append(0)
                else:
                    converted_row.append(value)
            values.append(tuple(converted_row))
        print("Đã insert dữ liệu thành công")
    except Exception as e:
        print("Insert thất bại")
        print(e)
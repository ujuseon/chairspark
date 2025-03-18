from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import avg
from pyspark.sql.types import IntegerType
import clickhouse_connect
import matplotlib.pyplot as plt
import pandas as pd
import requests
import os
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

CSV_URL = "https://www.dropbox.com/scl/fi/7c84e7tww7p95nxxq50go/russian_houses.csv?rlkey=m74egrnzvg84xbmyqdwv14ywo&st=gs3klw1p&dl=1"
SHARED_VOLUME = "/opt/airflow/shared" 
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "real_estate_db"
CLICKHOUSE_TABLE = 'buildings'

# Функция для подключения к ClickHouse
def get_clickhouse_connect():
    try:
        return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    except Exception as e:
        print(f'Ошибка во время подключения к Clickhouse: {e}')
        return None

# Функция загрузки CSV
def download_csv():
    response = requests.get(CSV_URL)
    response.raise_for_status()
    os.makedirs(SHARED_VOLUME, exist_ok=True) 
    file_path = os.path.join(SHARED_VOLUME, "data.csv")
    with open(file_path, "wb") as f:
        f.write(response.content)
    print(f"Файл загружен в {file_path}")

# Функция обработки данных
def process_data():
    spark = SparkSession.builder \
        .appName("Houses analytics") \
        .getOrCreate()

    # Загрузка данных в DataFrame PySpark
    file_path = os.path.join(SHARED_VOLUME, "data.csv")
    df = spark.read.csv(file_path, header=True, inferSchema=True, encoding='UTF-8', sep=',')

    # Количество строк
    row_count = df.count()
    print(f"\n{'-' * 40}\n\n\tКоличество строк: {row_count}\n\n{'-' * 40}\n")

    # Удаление строк с пропусками
    df = df.dropna()
    print(f"\n{'-' * 80}\n\n\tКоличество строк после удаления строк с пропущенными значениями: {df.count()}\n\n{'-' * 80}\n")

    # Функция для извлечения года из строки
    def extract_year(value):
        if value is None or value.startswith("до"):
            return None

        year_match = re.search(r'\b(1[5-9]\d{2}|20[0-2]\d)\b', value)

        if year_match:
            year = int(year_match.group(0))
            if year <= 2024:
                return year
        return None

    extract_year_udf = F.udf(extract_year, IntegerType())

    df = df.withColumn('standard_year', extract_year_udf(F.col('maintenance_year')))
    df_norm = df.drop('maintenance_year')
    df_norm = df_norm.na.drop(subset=['standard_year'])
    df_norm = df_norm.withColumnRenamed('standard_year', 'maintenance_year')

    # Преобразование типов
    df_norm = df_norm.withColumn("square", F.regexp_replace(F.col("square"), " ", "").cast("double"))
    df_norm = df_norm.withColumn("population", F.regexp_replace(F.col("population"), " ", "").cast("integer"))
    df_norm = df_norm.withColumn("communal_service_id", F.regexp_replace(F.col("communal_service_id"), " ", "").cast("integer"))

    df_norm = df_norm.dropna(subset=["communal_service_id"])
    df_norm = df_norm.dropna(subset=["square"])
    df_norm = df_norm.dropna(subset=["population"])

    # Средний и медианный год постройки зданий
    average_year = round(df_norm.agg(avg('maintenance_year')).collect()[0][0])
    print(f"\n{'-' * 40}\n\n\tСредний год постройки: {average_year}\n\n{'-' * 40}\n")

    median_year = round(df_norm.approxQuantile('maintenance_year', [0.5], 0.01)[0])
    print(f"\n{'-' * 40}\n\n\tМедианный год постройки: {median_year}\n\n{'-' * 40}\n")

    # Топ-10 областей
    region_counts = df_norm.groupBy('region').count()
    top_regions = region_counts.orderBy('count', ascending=False).limit(10)

    top_regions_pd = top_regions.toPandas()
    top_output_str = f"\n{'-' * 60}\n\nТоп-10 областей и городов с наибольшим количеством объектов:\n\n"
    top_output_str += top_regions_pd.to_string(index=False)
    print(top_output_str)

    # График
    plt.figure(figsize=(8, 5))
    plt.bar(top_regions_pd['region'], top_regions_pd['count'], color='coral')
    plt.title('Топ-10 регионов по количеству объектов')
    plt.xlabel('Регионы')
    plt.ylabel('Количество объектов')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    pic_path1 = os.path.join(SHARED_VOLUME, "top_regions.png")
    plt.savefig(pic_path1)
    plt.close()
    print(f"\n{'-' * 100}\n\nГрафик 'Топ-10 регионов по количеству объектов' загружен по пути {pic_path1}\n\n{'-' * 100}\n")

    # Максимальная и минимальная площадь в каждой области
    total_rows_region = df_norm.groupBy('region').count().count()

    min_max_squares = df_norm.groupBy("region").agg(
        F.min("square").alias("min_square"),
        F.max("square").alias("max_square")
    )

    # Форматируем вывод
    min_max_output_str = f"\n{'-' * 70}\n\nЗдания с максимальной и минимальной площадью в рамках каждой области:\n\n"
    min_max_output_str += "+--------------------------------------+---------------------+----------------------+\n"
    min_max_output_str += "| Регион                               | Минимальная площадь | Максимальная площадь |\n"
    min_max_output_str += "+--------------------------------------+---------------------+----------------------+\n"

    for row in min_max_squares.limit(total_rows_region).collect():
        min_max_output_str += f"| {row['region']:<37} | {row['min_square']:>19} | {row['max_square']:>19} |\n"

    min_max_output_str += "+--------------------------------------+---------------------+----------------------+\n\n"
    print(min_max_output_str)

    # Количество зданий по десятилетиям
    decade_df = (
        df_norm.withColumn(
            "period",
            F.when((F.col("maintenance_year") >= 1900) & (F.col("maintenance_year") < 2000),
                (F.floor(F.col("maintenance_year") / 10) * 10).cast("integer"))
            .when((F.col("maintenance_year") >= 1500) & (F.col("maintenance_year") < 1900),
                F.lit(1500))  # Группируем 1500-1899 в один период
            .when(F.col("maintenance_year") >= 2000,
                (F.floor(F.col("maintenance_year") / 10) * 10).cast("integer"))  # Группируем 2000+ по десятилетиям
            .otherwise(F.col("maintenance_year"))
        )
        .withColumn("period_str",
                    F.when(F.col("period") == 1500, F.lit("16-19 вв"))  # Изменение метки для 1500-1899
                    .when(F.col("period") >= 1900,
                        F.concat(F.col("period").cast("string"), F.lit("-е")))
                    .otherwise(F.concat(F.col("period").cast("string"), F.lit("-е гг.")))
        )
        .groupBy("period_str")  # Группировка по строке периода
        .agg(F.count("*").alias("count"))  # Подсчет количества зданий
        .orderBy("period_str")  # Сортировка по периоду
    )

    total_count_decade_df = decade_df.count()

    # Форматируем вывод
    output_str = f"\n\nКоличество зданий по десятилетиям:\n\n"
    output_str += "+-------------+-------------+\n"
    output_str += "| Десятилетие | Количество  |\n"
    output_str += "+-------------+-------------+\n"

    for row in decade_df.limit(total_count_decade_df).collect():
        output_str += f"| {row['period_str']:<11} | {row['count']:>11} |\n"

    output_str += "+-------------+-------------+\n"
    print(output_str)

    # График
    period_pd  = decade_df.toPandas()
    plt.figure(figsize=(8, 5))
    plt.bar(period_pd ['period_str'], period_pd ['count'], color='#6495ED')
    plt.title('Количество объектов по десятилетиям')
    plt.xlabel('Периоды')
    plt.ylabel('Количество объектов')
    plt.xticks(rotation=45)
    pic_path2 = os.path.join(SHARED_VOLUME, "buildings_by_decades.png")
    plt.savefig(pic_path2)
    plt.close()
    print(f"\n{'-' * 100}\n\nГрафик 'Количество объектов по десятилетиям' загружен по пути {pic_path2}\n\n{'-' * 100}\n")
    
    # Загрузка данных в Clickhouse
    df_pandas = df_norm.toPandas()
    columns_to_convert = ['region', 'locality_name', 'address', 'full_address', 'description']
    df_pandas[columns_to_convert] = df_pandas[columns_to_convert].astype('string')

    client = get_clickhouse_connect()
    if client:
        client.insert(f'{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}', df_pandas)
        print('Данные загружены в Clickhouse') 

    spark.stop()

# Функция для создания БД и таблица
def create_database_and_table():
    client = get_clickhouse_connect()
    if client:
        client.command(f'CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}')
        client.command(f'DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}')
        client.command(f'''
                    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
                        house_id	Int64,
                        latitude	Float64,
                        longitude	Float64,
                        square		Float64,
                        population	Int64,
                        region		String,
                        locality_name	String,
                        address		String,
                        full_address	String,
                        communal_service_id Int64,
                        description 	String,
                        maintenance_year Int64
                        ) ENGINE = MergeTree()
                        ORDER BY house_id
                       ''')
        print('База данных и таблица созданы')

# Функция для выполнения SQL-запроса в ClickHouse
def query_clickhouse():
    client = get_clickhouse_connect()
    if client:
        query = f"""
            SELECT * FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
            WHERE square > 60
            ORDER BY square DESC
            LIMIT 25
        """
        query_result = client.query(query)
        df = pd.DataFrame(query_result.result_rows, columns=query_result.column_names)
        query_output_str = f"\n\n{'-' * 40}\nТоп-25 домов с площадью более 60 кв.м:\n\n{df.to_string(index=False)}"
        print(query_output_str)

# DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "process_dag",
    default_args=default_args,
    description="DAG для обработки данных",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 1),
    catchup=False,
)

# Задачи DAG
download_task = PythonOperator(
    task_id="download_csv",
    python_callable=download_csv,
    dag=dag,
)

create_task = PythonOperator(
    task_id="create_database_and_table",
    python_callable=create_database_and_table,
    dag=dag,
)

process_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag,
)

query_task = PythonOperator(
    task_id="query_clickhouse",
    python_callable=query_clickhouse,
    dag=dag,
)

# Последовательность задач
download_task >> create_task >> process_task >> query_task

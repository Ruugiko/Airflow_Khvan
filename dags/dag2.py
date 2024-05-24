import os
import requests
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from transform_script import transform

DAG_ID = 'Khvan_Ok_khi_2'
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 4, 5, tz=pendulum.timezone("Europe/Moscow")),
    'retries': 3,
    'retry_delay': timedelta(seconds=60),
    'description': 'ETL DAG for monthly calculation of customer activity based on transactions.',
    'max_active_runs': 1,
    'catchup': False,
}

data_dir = '/tmp/airflow/data/' 
os.makedirs(data_dir, exist_ok=True)

def download_data(date):
    url = 'https://drive.usercontent.google.com/download?id=1hkkOIxnYQTa7WD1oSIDUFgEoBoWfjxK2&export=download&authuser=0&confirm=t&uuid=af8f933c-070d-4ea5-857b-2c31f2bad050&at=APZUnTVuHs3BtcrjY_dbuHsDceYr:1716219233729'
    output_path = os.path.join(data_dir, f'profit_table_{date}.csv')

    response = requests.get(url)
    response.raise_for_status()

    with open(output_path, 'wb') as file:
        file.write(response.content)
    print(f"Файл успешно загружен и сохранен в {output_path}")
    return output_path

def process_data(product, date, **context):
    ti = context['ti']
    output_path = ti.xcom_pull(task_ids='download_data')
    df = pd.read_csv(output_path)

    product_df = transform(df, date, product)
    product_output_path = os.path.join(data_dir, f'profit_table_{product}_{date}.csv')
    product_df.to_csv(product_output_path, index=False)
    print(f"Данные продукта {product} успешно обработаны и сохранены в {product_output_path}")

with DAG(
    DAG_ID,
    default_args=default_args,
    description=default_args['description'],
    start_date=default_args['start_date'],
    schedule_interval='0 0 5 * *',
    catchup=default_args['catchup'],
    max_active_runs=default_args['max_active_runs']
) as dag:

    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
        op_kwargs={'date': '{{ ds }}'},
    )

    product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    for product in product_list:
        process_task = PythonOperator(
            task_id=f'process_data_{product}',
            python_callable=process_data,
            op_kwargs={'product': product, 'date': '{{ ds }}'},
            provide_context=True,
        )
        download_task >> process_task
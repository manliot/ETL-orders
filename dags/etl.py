import os
import json
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}


def extract_from_file(**kwargs):
    """read a json or csv file and save data in list format (in XCOM)"""
    file_path = os.path.join(os.path.dirname(
        __file__), 'utils', 'outputFiles', kwargs['file_name'])
    type = kwargs['type']

    df = pd.DataFrame()
    if (type == 'json'):
        data = {}
        with open(file_path, 'r') as file:
            data = json.load(file)
        df = pd.json_normalize(data['data'])
    elif (type == 'csv'):
        df = pd.read_csv(file_path)

    return [df.columns.values.tolist()] + df.values.tolist()


def transform_user_info_data(**kwargs):
    # retrive user info data
    ti = kwargs['ti']
    df_list = ti.xcom_pull(task_ids='extract_user_info')
    df = pd.DataFrame(df_list)

    # change column names
    df.columns = ['Document', 'Name', 'BirthDay', 'Gender', 'Email', 'Phone']
    df = df.iloc[1:]
    df.head()

    # normalize gender column
    df['Gender'] = df['Gender'].replace({'Male': 'M', 'f': 'F'})

    # format bithday column
    df['BirthDay'] = df['BirthDay'].str.split('T').str[0]

    return [df.columns.values.tolist()] + df.values.tolist()


dag = DAG(
    'ETL',
    description='This is an example of a etl process',
    default_args=default_args,
    schedule_interval='0 12 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ETL']
)


# ---------- EXTRACT tasks ---------- #
extract_orders = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_from_file,
    dag=dag,
    provide_context=True,
    op_kwargs={'type': 'csv', 'file_name': 'orders.csv'}
)

extract_products = PythonOperator(
    task_id='extract_products',
    python_callable=extract_from_file,
    dag=dag,
    provide_context=True,
    op_kwargs={'type': 'csv',
               'file_name': 'products.csv'}
)

extract_users = PythonOperator(
    task_id='extract_users',
    python_callable=extract_from_file,
    dag=dag,
    provide_context=True,
    op_kwargs={'type': 'csv', 'file_name': 'users.csv'}
)

extract_user_info = PythonOperator(
    task_id='extract_user_info',
    python_callable=extract_from_file,
    dag=dag,
    provide_context=True,
    op_kwargs={'type': 'json',
               'file_name': 'user_info.json'}
)

# ---------- TRANSFORM tasks ---------- #
transform_user_info = PythonOperator(
    task_id='transform_user_info',
    python_callable=transform_user_info_data,
    dag=dag,
    provide_context=True,
)


# ---------- TASK DEPENDENCIES ---------- #
[extract_orders,
 extract_products,
 extract_users,
 extract_user_info] >> transform_user_info

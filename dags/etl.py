from datetime import datetime, timedelta

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

dag = DAG(
  'ETL',
  description='This is an example of a etl process',
  default_args=default_args,
  schedule_interval='0 12 * * *',
  start_date=datetime(2024, 1, 1),
  catchup=False,
  tags=['ETL']
)

tarea1=BashOperator(
  task_id='print_date',
  bash_command='echo "La fecha es $(date)"',
  dag=dag
)

tarea1
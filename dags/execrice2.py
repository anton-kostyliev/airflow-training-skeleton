from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Anton Kostyliev',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise_2',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)

def print_execution_date(execution_date, **context):
    print("Execution date: {{ execution_date }}")

with dag as dag:
    execution_date = PythonOperator(task_id="print_execution_date", python_callable=print_execution_date)
    sleep1 = BashOperator(task_id='sleep1', bash_command="sleep 1")
    sleep5 = BashOperator(task_id='sleep5', bash_command="sleep 5")
    sleep10 = BashOperator(task_id='sleep10', bash_command="sleep 10")
    end = DummyOperator(task_id="finish_task")

execution_date >> [sleep1, sleep5, sleep10] >> end

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import Pendulum

args = {
    'owner': 'Anton Kostyliev',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise_3',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)

def print_execution_date(execution_date : Pendulum, **context):
    print("Execution date: " + execution_date.to_iso8601_string())

def date_to_person(execution_date: Pendulum, **context):
    mapping = {
        0: 'bob',
        1: 'joe',
        2: 'alice',
        3: 'bob',
        4: 'joe',
        5: 'alice',
        6: 'bob',
    }
    return mapping[execution_date.day_of_week]


with dag as dag:
    execution_date = PythonOperator(
        task_id="print_execution_date",
        python_callable=print_execution_date,
        provide_context=True
    )

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=date_to_person,
        provide_context=True
    )

    bob = DummyOperator(task_id="bob")
    alice = DummyOperator(task_id="alice")
    joe = DummyOperator(task_id="joe")
    end = DummyOperator(task_id="finish_task", trigger_rule=TriggerRule.NONE_FAILED)

execution_date >> branching >> [bob, alice, joe] >> end

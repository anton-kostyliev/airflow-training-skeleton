from datetime import timedelta

import airflow
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from pendulum import Pendulum

args = {
    'owner': 'Anton Kostyliev',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise_4',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)

with dag as dag:
    copy_task = PostgresToGoogleCloudStorageOperator(
        postgres_conn_id='gdd',
        sql='SELECT * FROM land_registry_price_paid_uk WHERE postcode = \'TQ1 1RY\'',
        bucket='gdd-tetetetetetetete'
    )

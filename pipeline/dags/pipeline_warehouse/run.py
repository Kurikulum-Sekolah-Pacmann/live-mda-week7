from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from pipeline_data_lake.task.main import consume_and_store
from helper.callbacks.slack_notifier import slack_notifier

# For slack alerting
default_args = {
    'on_failure_callback': slack_notifier
}

# Define the DAG with its properties
@dag(
    dag_id='pipeline_warehouse',
    description='Streaming Data to Warehouse',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False,
    default_args=default_args
)
def pipeline_warehouse():
    """
    Streaming Data to Warehouse
    """
    consume_and_store()

# Instantiate the DAG
pipeline_warehouse()
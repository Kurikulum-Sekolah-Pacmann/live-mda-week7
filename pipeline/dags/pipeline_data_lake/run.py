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
    dag_id='pipeline_datalake',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule_interval='* * * * *',  # every minute
    catchup=False,
    default_args=default_args,
    tags=["streaming","data_lake"]
)
def pipeline_data_lake():
    """
    DAG function to extract data and load it into data lake.
    It also triggers the next DAG
    """
    # # Define the task to trigger the next DAG
    trigger_warehouse = TriggerDagRunOperator(
        task_id='trigger_warehouse',
        trigger_dag_id="pipeline_warehouse",
        trigger_rule="none_failed"
    )

    # Define the task dependencies
    consume_and_store() >> trigger_warehouse

# Instantiate the DAG
pipeline_data_lake()
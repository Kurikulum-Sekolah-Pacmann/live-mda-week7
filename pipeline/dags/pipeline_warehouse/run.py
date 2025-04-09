from airflow.decorators import dag
from airflow.models import Variable
from pendulum import datetime
from pipeline_warehouse.task.main import main_etl
from helper.callbacks.slack_notifier import slack_notifier

# For slack alerting
default_args = {
    'on_failure_callback': slack_notifier
}

# Define the DAG with its properties
@dag(
    dag_id='pipeline_warehouse',
    description='Streaming data and load into warehouse area',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule_interval=None,  # No schedule, will run continuously
    catchup=False,
    default_args=default_args,
    tags=["streaming", "warehouse"]
)
def etl_warehouse():
    """
    DAG function to Extract data, transform and load into warehouse area.
    """

    # Create the main task group with ETL tasks
    main_etl()

# Instantiate the DAG
dag = etl_warehouse()
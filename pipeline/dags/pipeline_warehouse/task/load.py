from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from sqlalchemy import create_engine
from pangres import upsert
import pandas as pd

class Load:
    """
    Class to handle loading data to validation table.
    """
    
    @staticmethod
    def load(data: pd.DataFrame,table_name: str, primary_key, **kwargs) -> None:
        """
        Load validation summary data into the PostgreSQL validation table.

        :param data: Dataframe containing validation summary result.
        :param schema: Database schema name.
        :param table_name: Table name in the database.
        :param kwargs: Additional keyword arguments.
        """
        schema = 'public'
        ti_all = kwargs['ti']
        ti = ti_all['task_instance']
        execution_date = ti_all['execution_date']
        extract_info = ti.xcom_pull(key=f"extract_info-{schema}.{table_name}")
        
        # Skip if no new data
        if extract_info and extract_info.get("status") == "skipped":
            raise AirflowSkipException(f"There is no data for '{schema}.{table_name}'. Skipped...")
        
        # Check if validation data is empty
        if data.empty:
            print(f"No validation errors for '{schema}.{table_name}'. Nothing to load.")
            raise AirflowSkipException(f"There is no data for '{schema}.{table_name}'. Skipped...")

        # Create PostgreSQL engine
        postgres_uri = PostgresHook(postgres_conn_id='warehouse').get_uri()
        engine = create_engine(postgres_uri)


        # Set primary key as index
        data = data.set_index(primary_key)

        # Upsert data into PostgreSQL table
        upsert(con=engine, df=data, table_name=table_name, schema='public', if_row_exists='update')

        # Dispose the engine
        engine.dispose()

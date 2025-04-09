from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd

from pipeline_warehouse.task.extract import Extract
from pipeline_warehouse.task.extract_transform import Transform
from pipeline_warehouse.task.load import Load

def transform_wrapper(task_info, incremental, **context):
    """
    Wrapper for transform function to handle XCom pushing.
    
    Args:
        task_info (list): [table_name, transform_function, table_extract]
        incremental (bool): Whether to use incremental mode
        context: Airflow context
    
    Returns:
        dict: Transformed data as a dictionary
    """
    table_name = task_info[0]
    transform_func = task_info[1]
    table_extract = task_info[2]
    
    # Call the transform function with the proper context
    df = transform_func(**context)
    
    if df is not None and not df.empty:
        # Convert DataFrame to dictionary for XCom serialization
        # Handle timestamp conversion
        df_for_xcom = df.copy()
        for col in df_for_xcom.select_dtypes(include=['datetime64']).columns:
            df_for_xcom[col] = df_for_xcom[col].astype(str)
            
        return df_for_xcom.to_dict(orient='records')
    return None

def load_wrapper(table_name, primary_key, **context):
    """
    Wrapper for load function to handle XCom pulling.
    
    Args:
        table_name (str): Target table name
        primary_key (list): Primary key columns
        context: Airflow context
    """
    ti = context['ti']
    # Pull data from transform task
    data = ti.xcom_pull(task_ids=f"etl.transform_group.transform_{table_name}")
    
    if data:
        # Convert dictionary back to DataFrame
        df = pd.DataFrame(data)
        
        # Convert string columns back to datetime if needed
        for col in ['created_at', 'updated_at']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                
        # Call the load function
        Load.load(data=df, table_name=table_name, primary_key=primary_key, ti=context)
    else:
        print(f"No data available for {table_name}")

@task_group(group_id='etl')
def main_etl():
    """
    Main task group for ETL process.
    
    Args:
        incremental (bool): Whether to use incremental mode
    """
    @task_group(group_id='transform_group')
    def transform_group():
        """Transform task group to handle all data transformations."""
        tasks = [
            ['dim_product', Transform._dim_product],
            ['dim_customer', Transform._dim_customer],
            ['dim_store', Transform._dim_store],
            ['dim_staff', Transform._dim_staff],
            ['fact_order', Transform._fact_order]
        ]
        
        transform_tasks = {}
        
        for task_info in tasks:
            table_name = task_info[0]
            transform_tasks[table_name] = PythonOperator(
                task_id=f"transform_{table_name}",
                python_callable=transform_wrapper,
                op_kwargs={
                    'task_info': task_info
                },
                provide_context=True
            )
            
        return transform_tasks
    
    @task_group(group_id='load_group')
    def load_group(transform_tasks):
        """Load task group to handle all data loading."""
        try:
            list_table = {
                'dim_product': 'product_nk',
                'dim_customer': 'customer_nk',
                'dim_store': 'store_nk',
                'dim_staff': 'staff_nk',
                'fact_order': ['order_nk', 'item_nk']
            }
        except Exception as e:
            print(f"Error parsing TABLE_LIST variable: {e}")
            list_table = {}
        
        load_tasks = {}
        
        for table_name, transform_task in transform_tasks.items():
            load_tasks[table_name] = PythonOperator(
                task_id=f"load_{table_name}",
                python_callable=load_wrapper,
                op_kwargs={
                    'table_name': table_name,
                    'primary_key': list_table.get(table_name, []),
                },
                provide_context=True
            )
            
            # Set dependencies
            transform_task >> load_tasks[table_name]
            
        return load_tasks
    
    # Create and connect task groups
    transform_tasks = transform_group()
    load_tasks = load_group(transform_tasks)
    
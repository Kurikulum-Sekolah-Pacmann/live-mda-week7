from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
import pandas as pd
from io import StringIO

class S3:
    @staticmethod        
    def push(aws_conn_id, bucket_name, key, string_data):
        hook = S3Hook(aws_conn_id=aws_conn_id)
        
        # Check if the key already exists
        if hook.check_for_key(key, bucket_name):
            raise AirflowSkipException(f"Key {key} already exists.")
        
        # If the key does not exist, push the data
        hook.load_bytes(string_data, key, bucket_name)
    
    @staticmethod
    def pull(aws_conn_id, bucket_name, key):
        hook = S3Hook(aws_conn_id=aws_conn_id)
        
        data = hook.get_key(
            key=key,
            bucket_name=bucket_name
        )
        
        csv_string = data.get()['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(csv_string))
import json
import time
import logging
import pandas as pd
from io import BytesIO
from datetime import datetime
from airflow.exceptions import AirflowSkipException, AirflowException
from confluent_kafka import Consumer
from helper.s3 import S3

class Extract:
    """
    A class used to extract data from kafka topic and load it into a data lake.
    """

    @staticmethod
    def _kafka(topic: str, **kwargs) -> None:
        """
        Consume messages from a Kafka topic and save them to MinIO

        Args:
            topic: Kafka topic name
        """
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        formatted_date = execution_date.strftime('%Y/%m/%d')

        # Extract from topic name
        # Format: source.schema.table_name
        parts = topic.split('.')
        if len(parts) >= 3:
            schema = parts[1]
            table = parts[2]
            object_key = f"raw/{schema}/{table}/{formatted_date}/{table}_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
        else:
            raise AirflowException(f"Topic name '{topic}' format tidak sesuai (source.schema.table_name)")

        # Initialize Kafka consumer
        consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': f'warehouse_consumer-{topic}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        consumer.subscribe([topic])

        messages = []
        count = 0
        
        logging.info(f"Starting to consume from topic: {topic}")

        try:
            # Poll for messages with a timeout
            poll_start_time = time.time()
            max_poll_time = 60  # Maximum time to poll in seconds
            
            while time.time() - poll_start_time < max_poll_time:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    # No more messages available right now
                    if count > 0:
                        # If we've already received messages, we're done
                        break
                    # Otherwise, continue polling until timeout
                    continue
                
                if msg.error():
                    raise AirflowException(f"Kafka error: {msg.error()}")
                
                # Try to parse message value (assuming JSON format)
                try:
                    message_data = json.loads(msg.value())
                    messages.append(message_data)
                except Exception as e:
                    # If not JSON, use the raw message
                    try:
                        messages.append({'raw_data': msg.value().decode('utf-8', errors='ignore')})
                    except Exception as e:
                        raise AirflowException(f"Kafka error: {msg.error()}")
                
                count += 1

                if count % 1000 == 0:
                    consumer.commit()
                    logging.info(f"Processed {count} messages from {topic}")
                
                # Reset timer when finding messages
                if count % 100 == 0:
                    poll_start_time = time.time()

        except Exception as e:
            logging.error(f"Error when extracting {topic}: {str(e)}")
            raise AirflowException(f"Error when extracting {topic}: {str(e)}")

        finally:
            consumer.commit()
            consumer.close()
            logging.info(f"Finished consuming from {topic}, total messages: {count}")

        # If no messages, skip
        if not messages:
            logging.info(f"No messages found for {schema}.{table}")
            ti.xcom_push(
                key=f"extract_info-{schema}.{table}",
                value={"status": "skipped", "data_date": formatted_date}
            )
            raise AirflowSkipException(f"Table '{schema}.{table}' doesn't have new data. Skipped...")
        else:
            try:
                logging.info(f"Converting {len(messages)} messages to DataFrame for {schema}.{table}")
                df = pd.DataFrame(messages)
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str)
                

                ti.xcom_push(
                    key=f"extract_info-{schema}.{table}",
                    value={"status": "success", "data_date": formatted_date, "record_count": len(messages), "message": df}
                )
                
                return df

            except Exception as e:
                logging.error(f"Error: {str(e)}")
                raise AirflowException(f"Error {str(e)}")
            
    
    @staticmethod
    def _dwh(schema: str, table_name: str, **kwargs) -> None:
        """
        Extract data from a PostgreSQL database

        Parameters:
        schema (str): The schema name.
        table_name (str): The table name.
        kwargs: Additional keyword arguments.
        """
        try:
            # Get execution date and convert to Jakarta timezone
            ti = kwargs['ti']
            execution_date = ti.execution_date
            tz = pytz.timezone('Asia/Jakarta')
            execution_date = execution_date.astimezone(tz)
            data_date = (pd.to_datetime(execution_date) - timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Connect to PostgreSQL database
            pg_hook = PostgresHook(postgres_conn_id='warehouse')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            # Formulate the extract query
            extract_query = f"SELECT * FROM {schema}.{table_name}"

            # Execute the query and fetch results
            cursor.execute(extract_query)
            result = cursor.fetchall()
            column_list = [desc[0] for desc in cursor.description]
            cursor.close()
            connection.commit()
            connection.close()

            
            # Convert results to DataFrame
            df = pd.DataFrame(result, columns=column_list)

            # Check if DataFrame is empty and handle accordingly
            if df.empty:
                ti.xcom_push(
                    key=f"extract_dwh_info-{schema}.{table_name}", 
                    value={"status": "skipped", "data_date": execution_date}
                )
                raise AirflowSkipException(f"Table '{schema}.{table_name}' doesn't have data. Skipped...")
            else:
                ti.xcom_push(
                    key=f"extract_dwh_info-{schema}.{table_name}", 
                    value={"status": "success", "data_date": execution_date}
                )
                
                return df
            
        except AirflowSkipException as e:
            raise e
        
        except AirflowException as e:
            raise AirflowException(f"Error when extracting {schema}.{table_name} : {str(e)}")
        
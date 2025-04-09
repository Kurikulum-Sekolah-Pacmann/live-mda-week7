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
            'group.id': f'datalake_consumer-{topic}',
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

        # If no messages, skip writing to MinIO
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

                # Convert to Parquet
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                # Upload to MinIO
                logging.info(f"Uploading {schema}.{table} data to MinIO: {object_key}")
                S3.push(
                    aws_conn_id='s3-conn',
                    bucket_name='transactions',
                    key=object_key,
                    string_data=parquet_buffer.getvalue()  # Use parquet binary data
                )

                ti.xcom_push(
                    key=f"extract_info-{schema}.{table}",
                    value={"status": "success", "data_date": formatted_date, "record_count": len(messages)}
                )
                logging.info(f"Successfully processed and stored {len(messages)} records for {schema}.{table}")

            except Exception as e:
                logging.error(f"Error when writing {schema}.{table} to MinIO: {str(e)}")
                raise AirflowException(f"Error when writing {schema}.{table} to MinIO: {str(e)}")
from airflow.exceptions import AirflowSkipException, AirflowException
from helper.s3 import S3
from datetime import timedelta
from confluent_kafka import Consumer

import pandas as pd
from io import BytesIO

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

        # Initialize Kafka consumer using confluent_kafka
        consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': f'datalake_consumer-{topic}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        consumer.subscribe([topic])

        messages = []
        count = 0

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    break
                if msg.error():
                    raise AirflowException(f"Kafka error: {msg.error()}")
                messages.append(msg.value())
                count += 1

                if count % 1000 == 0:
                    consumer.commit()

        except Exception as e:
            raise AirflowException(f"Error when extracting {topic}: {str(e)}")

        finally:
            consumer.commit()
            consumer.close()

        # If no messages, skip writing to MinIO
        if not messages:
            ti.xcom_push(
                key=f"extract_info-{schema}.{table}",
                value={"status": "skipped", "data_date": formatted_date}
            )
            raise AirflowSkipException(f"Table '{schema}.{table}' doesn't have new data. Skipped...")
        else:
            try:
                df = pd.DataFrame(messages)

                # Convert to Parquet
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                # Upload to MinIO
                S3.push(
                    aws_conn_id='s3-conn',
                    bucket_name='transactions',
                    key=object_key,
                    string_data=df.to_csv(index=False)
                )

                ti.xcom_push(
                    key=f"extract_info-{schema}.{table}",
                    value={"status": "success", "data_date": formatted_date}
                )

            except Exception as e:
                raise AirflowException(f"Error when writing {schema}.{table} to MinIO: {str(e)}")

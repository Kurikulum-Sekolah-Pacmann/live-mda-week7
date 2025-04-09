from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer
from pipeline_data_lake.task.extract_load import Extract
import logging

# Kafka configuration
BOOTSTRAP_SERVERS = 'kafka:9092'
TOPIC_PREFIX = 'source'
CONSUMER_GROUP = 'minio-consumer'

def get_kafka_topics() -> list:
    """Get all Kafka topics based on prefix using confluent_kafka"""
    try:
        consumer = Consumer({
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        logging.info("[KAFKA] Connected to Kafka, listing topics...")

        all_topics = consumer.list_topics(timeout=10).topics.keys()
        filtered_topics = [topic for topic in all_topics if topic.startswith(f"{TOPIC_PREFIX}.")]
        logging.info(f"[KAFKA] Topics found: {filtered_topics}")
        consumer.close()
        return filtered_topics

    except Exception as e:
        logging.error(f"[KAFKA] Error listing topics: {str(e)}")
        return [f"{TOPIC_PREFIX}.schema.fallback_table"]  # Return fallback topic if error


def create_topic_tasks(**kwargs):
    """Push Kafka topics to XCom"""
    try:
        topics = get_kafka_topics()
        logging.info(f"[DISCOVER] Kafka topics discovered: {topics}")
        kwargs['ti'].xcom_push(key='kafka_topics', value=topics)
        return topics
    except Exception as e:
        logging.error(f"[DISCOVER] Failed to discover Kafka topics: {str(e)}")
        raise


PREDEFINED_TOPICS = get_kafka_topics()


@task_group(group_id="consume_and_store")
def consume_and_store():
    """
    Task group to consume data from Kafka and store it in Data Lake.
    """
    # Task to discover Kafka topics at runtime
    discover_topics_task = PythonOperator(
        task_id='discover_kafka_topics',
        python_callable=create_topic_tasks,
        provide_context=True,
    )

    # Create a consumer task for each Kafka topic
    for topic in PREDEFINED_TOPICS:
        topic_safe_name = topic.replace('.', '_')
        
        # Create a task that will extract data from this topic
        consumer_task = PythonOperator(
            task_id=f'consume_{topic_safe_name}',
            python_callable=Extract._kafka,
            op_kwargs={'topic': topic},
            provide_context=True,
        )
        
        # Set the task dependency
        discover_topics_task >> consumer_task
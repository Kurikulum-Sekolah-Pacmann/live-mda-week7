from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer
from pipeline_data_lake.task.extract_load import Extract

# Kafka configuration
BOOTSTRAP_SERVERS = 'kafka:9092'
TOPIC_PREFIX = 'source'
CONSUMER_GROUP = 'minio-consumer'


def get_kafka_topics() -> list:
    """Get all Kafka topics based on prefix using confluent_kafka"""
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })

    all_topics = consumer.list_topics(timeout=10).topics.keys()
    filtered_topics = [topic for topic in all_topics if topic.startswith(f"{TOPIC_PREFIX}.")]
    consumer.close()
    return filtered_topics


def create_topic_tasks(**kwargs):
    """Push Kafka topics to XCom"""
    topics = get_kafka_topics()
    kwargs['ti'].xcom_push(key='kafka_topics', value=topics)


@task_group(group_id="consume_and_store")
def consume_and_store():
    """
    Task group to consume data from Kafka and store it in Data Lake.
    """

    # Task to discover Kafka topics
    discover_topics_task = PythonOperator(
        task_id='discover_kafka_topics',
        python_callable=create_topic_tasks,
        provide_context=True,
    )

    # Create consumer tasks for each topic
    def create_consumer_tasks(**kwargs):
        """Create consumer tasks for each Kafka topic"""
        ti = kwargs['ti']
        topics = ti.xcom_pull(task_ids='consume_and_store.discover_kafka_topics', key='kafka_topics')

        from airflow.models.dag import DagBag
        dag = kwargs['dag']
        task_list = []

        for topic in topics:
            topic_safe_name = topic.replace('.', '_')

            task = PythonOperator(
                task_id=f'consume_{topic_safe_name}',
                python_callable=Extract._kafka,
                op_kwargs={'topic': topic},
                provide_context=True,
                dag=dag,
            )
            task_list.append(task)
            discover_topics_task >> task

    create_tasks = PythonOperator(
        task_id='create_consumer_tasks',
        python_callable=create_consumer_tasks,
        provide_context=True,
    )

    # Task dependencies
    discover_topics_task >> create_tasks

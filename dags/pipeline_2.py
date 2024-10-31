# imports
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from confluent_kafka import Producer
import requests
import datetime
import json


# define function/stages
def fetch_data():
    KAFKA_SERVERS=['kafka_broker_1:19092', 'kafka_broker_2:19093', 'kafka_broker_3:19094']
    producer = Producer({
        'bootstrap.servers': ','.join(KAFKA_SERVERS),
        'client.id': 'producer_instance'
    }
    )
    print('Sending data to topic')
    i = 0
    offset = 0
    limit = 100
    while True:
        url = f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit={limit}&offset={offset}"
        topic = 'velib_test'
        print('Getting data from api!')
        data = requests.get(url).json()
        if data['results']:
            print(f'Send iteration:{i}')
            producer.produce(
                topic=topic,
                value=json.dumps(data).encode('utf-8')
            )
            producer.flush()
            offset += limit
            i += 1
        else:
            break


# define dag
with DAG(
    'kafka_streaming_pipeline_2',
    description='Kafka streaming pipeline',
    start_date=datetime.datetime(2024, 10, 10),
    schedule_interval="*/15 * * * *",
    catchup=False
) as dag1:

    fetch_data = PythonOperator(
        task_id='fetch_produce_data',
        python_callable=fetch_data,
        dag=dag1,
    )

    fetch_data

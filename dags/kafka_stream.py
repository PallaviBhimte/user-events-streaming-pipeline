from datetime import datetime
import logging
from airflow import DAG
import json
import os
import uuid

import time
from airflow.operators.python import PythonOperator

# default settings for the DAG
default_args = {
    'owner': 'pallavi',
    'start_date': datetime(2023, 8, 3, 10, 00)
}

def get_data():
    import requests

    # call the API
    res = requests.get("https://randomuser.me/api/")
    # read the JSON response
    res = res.json()
    # take the user record out of the results list
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    location = res['location']

    # generate an id for the record
    data['id'] = str(uuid.uuid4())

    # copy the name and gender fields
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']

    # build one address string from the nested location fields
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']

    # copy the contact and profile fields
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    from kafka import KafkaProducer

    # connect to the Kafka broker
    bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    producer = KafkaProducer(bootstrap_servers=[bootstrap], max_block_ms=5000)
    curr_time = time.time()

    # keep sending records for 60 seconds
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            # fetch one user from the API
            res = get_data()
            # flatten the record
            res = format_data(res)
            # send it to the users_created topic as JSON bytes
            producer.send('users_created', json.dumps(res).encode('utf-8'))
            time.sleep(1)
        except Exception as e:
            # log the error and move on to the next record
            logging.error(f'An error occurred: {e}')
            time.sleep(1)
            continue

# define the DAG and schedule it to run once a day
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # single task that runs the producer
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

# run the producer directly, without Airflow
if __name__ == '__main__':
    stream_data()

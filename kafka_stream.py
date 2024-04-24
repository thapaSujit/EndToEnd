from datetime import datetime
#from airflow.models.dag import DAG
#from airflow.operators.python import PythonOperator
import json
import uuid
from kafka import KafkaProducer

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    print(res)
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    #data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    res = get_data()
    res = format_data(res)
    #print(json.dumps(res, indent= 3))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    producer.send("users_created", json.dumps(res).encode("utf-8"))

stream_data()
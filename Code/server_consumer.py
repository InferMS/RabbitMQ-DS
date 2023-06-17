import json
import time
from collections import namedtuple

import grpc
from concurrent import futures

import pika
import meteo_utils
import redis, pickle

r = redis.Redis(host='localhost', port=6379, decode_responses=False)

def processMeteoData(data):

    wellness_bytes = r.get('wellness'.encode())
    wellness_dict = pickle.loads(wellness_bytes)
    processor = meteo_utils.MeteoDataProcessor()
    Object = lambda **kwargs: type("Object", (), kwargs)
    obj = Object(
        temperature = data['temperature'],
        humidity = data['humidity']
    )
    wellness = processor.process_meteo_data(obj)
    wellnessData = {
        "id": data['id'],
        "timer_seconds": data['timestamp'],
        "value": wellness
    }
    if data['id'] not in wellness_dict:
        wellness_dict[data['id']] = []

    wellness_dict[data['id']].append(wellnessData)

    wellness_bytes = pickle.dumps(wellness_dict)
    r.set('wellness', wellness_bytes)


def processPollutionData(data):
    pollution_bytes = r.get('pollution'.encode())
    pollution_dict = pickle.loads(pollution_bytes)
    processor = meteo_utils.MeteoDataProcessor()
    Object = lambda **kwargs: type("Object", (), kwargs)
    obj = Object(
        co2=data['co2'],
    )
    quality = processor.process_pollution_data(obj)
    pollutionData = {
        "id": data['id'],
        "timer_seconds": data['timestamp'],
        "value": quality
    }
    if data['id'] not in pollution_dict:
        pollution_dict[data['id']] = []

    pollution_dict[data['id']].append(pollutionData)

    pollution_bytes = pickle.dumps(pollution_dict)
    r.set('pollution', pollution_bytes)



def callback(ch, method, properties, body):
    data = json.loads(body)

    if "temperature" in data:
        processMeteoData(data)
    else:
        processPollutionData(data)
    ch.basic_ack(delivery_tag=method.delivery_tag)



def start(index):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='server_queue', durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='server_queue', on_message_callback=callback)

    channel.start_consuming()

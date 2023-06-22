import json
import time

import grpc

# import the generated classes
import meteo_utils
import pika


def sendMeteoData(sensorId):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='server_queue', durable=True)
    detector = meteo_utils.MeteoDataDetector()
    try:
        while True:
            air = detector.analyze_air()
            meteoData = {
                "id": sensorId,
                "temperature": air['temperature'],
                "humidity": air["humidity"],
                "timestamp": round(time.time())
            }

            channel.basic_publish(
                exchange='',
                routing_key='server_queue',
                body=json.dumps(meteoData),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ))
            time.sleep(1)

    except KeyboardInterrupt:
        connection.close()


def sendPollutionData(sensorId):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1'))
    channel = connection.channel()
    channel.queue_declare(queue='server_queue', durable=True)
    detector = meteo_utils.MeteoDataDetector()
    try:
        while True:
            air = detector.analyze_pollution()
            pollutionData = {
                "id": sensorId,
                "co2": air['co2'],
                "timestamp": round(time.time())
            }

            channel.basic_publish(
                exchange='',
                routing_key='server_queue',
                body=json.dumps(pollutionData),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ))
            time.sleep(1)
    except KeyboardInterrupt:
        connection.close()

import pickle, redis, pika, time
import terminal_pb2
from google.protobuf.timestamp_pb2 import Timestamp

# Create a connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the exchange
channel.exchange_declare(exchange='proxy_terminals', exchange_type='fanout')

# Create a Redis client
r = redis.Redis(host='localhost', port=6379, decode_responses=False)

p_last = dict()
w_last = dict()
first_2 = False
timestamp = 1
timesleep = 2


def create_timestamp(seconds):
    timestamp = Timestamp()
    timestamp.FromSeconds(seconds)
    return timestamp

def generate_pollution_data():
    pollution_bytes = r.get("pollution".encode())
    pollution_dict = pickle.loads(pollution_bytes)
    return pollution_dict


def generate_wellness_data():
    wellness_bytes = r.get("wellness".encode())
    wellness_dict = pickle.loads(wellness_bytes)
    return wellness_dict

def run_client():
    timestamp = 1

    while True:
        # Generate new data
        pollution_dict = generate_pollution_data()
        wellness_dict = generate_wellness_data()
        p1 = []
        w1 = []
        for x in pollution_dict.keys():
            for y in pollution_dict[x]:
                timer = create_timestamp(timestamp)
                y['timer_seconds'] = timer
                if p_last.get(y['id']) == None:
                    p1.append(terminal_pb2.pollutionData(id=y['id'], timestamp=y['timer_seconds'], coefficient=float(y['value'])))
                else:
                    if y['timer_seconds'].seconds == timestamp and (
                            p_last.get(y['id'])['timer_seconds'].seconds != (y['timer_seconds'].seconds - timesleep) and
                            p_last.get(y['id'])['value'] != y['value']):
                        p1.append(terminal_pb2.pollutionData(id=y['id'], timestamp=y['timer_seconds'], coefficient=float(y['value'])))
                p_last[y['id']] = y

        for x in wellness_dict.keys():
            for y in wellness_dict[x]:
                timer = create_timestamp(timestamp)
                y['timer_seconds'] = timer
                if w_last.get(y['id']) == None:
                    w1.append(terminal_pb2.wellnessData(id=y['id'], timestamp=y['timer_seconds'], coefficient=float(y['value'])))
                else:
                    if y['timer_seconds'].seconds == timestamp and (
                            w_last.get(y['id'])['timer_seconds'].seconds != (y['timer_seconds'].seconds - timesleep) and
                            w_last.get(y['id'])['value'] != y['value']):
                        w1.append(terminal_pb2.wellnessData(id=y['id'], timestamp=y['timer_seconds'],
                                                             coefficient=float(y['value'])))
                w_last[y['id']] = y

        data = terminal_pb2.airData(pollution=p1, wellness=w1)

        # Serialize the data
        serialized_data = data.SerializeToString()

        # Publish the serialized data to the exchange
        channel.basic_publish(exchange='proxy_terminals', routing_key='', body=serialized_data)
        timestamp += 1
        time.sleep(2)  # Ajusta el valor según tus necesidades

if __name__ == '__main__':
    run_client()

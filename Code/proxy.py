import pickle, redis, pika, time, json

# Create a connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the exchange
channel.exchange_declare(exchange='proxy_terminals', exchange_type='fanout')

# Create a Redis client
r = redis.Redis(host='localhost', port=6379, decode_responses=False)

p_last = dict()
w_last = dict()
timestamp = 1
timesleep = 2


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
        data = {
            'pollution': {},
            'wellness': {}
        }

        pollution_dict = generate_pollution_data()
        wellness_dict = generate_wellness_data()

        for id in pollution_dict.keys():
            for pollution_data in pollution_dict[id]:
                pollution_data['timer_seconds'] = timestamp
                if p_last.get(pollution_data['id']) is None:
                    data['pollution'][pollution_data['id']] = {
                        'timestamp': pollution_data['timer_seconds'],
                        'coefficient': float(pollution_data['value'])
                    }
                else:
                    if (
                            pollution_data['timer_seconds'] == timestamp and
                            p_last.get(pollution_data['id'])['timer_seconds'] != (pollution_data['timer_seconds'] - timesleep) and
                            p_last.get(pollution_data['id'])['value'] != pollution_data['value']
                    ):
                        data['pollution'][pollution_data['id']] = {
                            'timestamp': pollution_data['timer_seconds'],
                            'coefficient': float(pollution_data['value'])
                        }
                p_last[pollution_data['id']] = pollution_data

        for id in wellness_dict.keys():
            for wellness_data in wellness_dict[id]:
                wellness_data['timer_seconds'] = timestamp
                if w_last.get(wellness_data['id']) is None:
                    data['wellness'][wellness_data['id']] = {
                        'timestamp': wellness_data['timer_seconds'],
                        'coefficient': float(wellness_data['value'])
                    }
                else:
                    if (
                            wellness_data['timer_seconds'] == timestamp and
                            w_last.get(wellness_data['id'])['timer_seconds'] != (wellness_data['timer_seconds'] - timesleep) and
                            w_last.get(wellness_data['id'])['value'] != wellness_data['value']
                    ):
                        data['wellness'][wellness_data['id']] = {
                            'timestamp': wellness_data['timer_seconds'],
                            'coefficient': float(wellness_data['value'])
                        }
                w_last[wellness_data['id']] = wellness_data

        # Publish the serialized data to the exchange
        channel.basic_publish(exchange='proxy_terminals',
                              routing_key='',
                              body=json.dumps(data))
        timestamp += 1
        time.sleep(2)  # Adjust the value as needed


if __name__ == '__main__':
    run_client()
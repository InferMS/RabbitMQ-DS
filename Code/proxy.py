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

        for x in pollution_dict.keys():
            for y in pollution_dict[x]:
                y['timer_seconds'] = timestamp
                if p_last.get(y['id']) is None:
                    data['pollution'][y['id']] = {
                        'timestamp': y['timer_seconds'],
                        'coefficient': float(y['value'])
                    }
                else:
                    if (
                            y['timer_seconds'] == timestamp and
                            p_last.get(y['id'])['timer_seconds'] != (y['timer_seconds'] - timesleep) and
                            p_last.get(y['id'])['value'] != y['value']
                    ):
                        data['pollution'][y['id']] = {
                            'timestamp': y['timer_seconds'],
                            'coefficient': float(y['value'])
                        }
                p_last[y['id']] = y

        for x in wellness_dict.keys():
            for y in wellness_dict[x]:
                y['timer_seconds'] = timestamp
                if w_last.get(y['id']) is None:
                    data['wellness'][y['id']] = {
                        'timestamp': y['timer_seconds'],
                        'coefficient': float(y['value'])
                    }
                else:
                    if (
                            y['timer_seconds'] == timestamp and
                            w_last.get(y['id'])['timer_seconds'] != (y['timer_seconds'] - timesleep) and
                            w_last.get(y['id'])['value'] != y['value']
                    ):
                        data['wellness'][y['id']] = {
                            'timestamp': y['timer_seconds'],
                            'coefficient': float(y['value'])
                        }
                w_last[y['id']] = y

        # Publish the serialized data to the exchange
        channel.basic_publish(exchange='proxy_terminals',
                              routing_key='',
                              body=json.dumps(data))
        timestamp += 1
        time.sleep(2)  # Adjust the value as needed


if __name__ == '__main__':
    run_client()
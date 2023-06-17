import pika, json
from terminal_service import terminal_service
class send_resultsServicer:

    def init(self, id_terminal):
        self.id_terminal = id_terminal

    def send_results(self, request, context):
        terminal_service.send_results(request['pollution'], request['wellness'], self.id_terminal)

    def callback(self, ch, method, properties, body):
        self.send_results(json.loads(body), None)

    def run_server(self, id_terminal):
        self.id_terminal = id_terminal
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        channel.exchange_declare(exchange='proxy_terminals', exchange_type='fanout')

        result = channel.queue_declare(queue=f'terminal:{id_terminal}', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='proxy_terminals', queue=queue_name)

        print(' [*] Waiting for logs. To exit press CTRL+C')

        channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)

        # Start consuming messages in an infinite loop
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            pass

        connection.close()

        connection.close()
from csv import DictReader
from io import StringIO
from time import sleep
import json
from .workers import Worker

WAIT_TIME_PIKA = 15

class Sender(Worker):
    def __init__(self, rabbit_hostname, dst_queue):
        self.new(rabbit_hostname, dst_routing_key=dst_queue)
        self.channel.queue_declare(queue=dst_queue)
    
    def callback(self, ch, method, properties, body):
        pass

    def publish(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.routing_key, body=message)


class Proxy(Worker):
    def __init__(self, rabbit_hostname, src_queue, dst_exchange, keys_getter = None):
        self.get_keys = keys_getter if keys_getter is not None else lambda x: ""
        self.new(rabbit_hostname, src_queue=src_queue, dst_exchange=dst_exchange)

    def callback(self, ch, method, properties, body: bytes):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        message = body.decode('utf-8')
        csv_stream = StringIO(message)
        reader = DictReader(csv_stream)
        
        for row in reader:
            self.channel.basic_publish(exchange=self.dst_exchange, routing_key=self.get_keys(row), body=json.dumps(row))


def wait_rabbitmq():
    """Pauses execution for few seconds in order start rabbitmq broker."""
    # this needs to be moved to the docker compose as a readiness probe
    # we should first create rabbit, then workers, and finally start sending messages
    sleep(WAIT_TIME_PIKA)

from abc import ABC, abstractmethod
from csv import DictReader
from io import StringIO
from time import sleep
from json import dumps
import pika

WAIT_TIME_PIKA = 15
MAX_KEY_LENGTH = 255

class Worker(ABC):
    def new(self, rabbit_hostname, src_queue, dst_queue=None, exchange=None, exchange_type='direct'):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_hostname))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=src_queue, durable=True)
        self.channel.basic_consume(queue=src_queue, on_message_callback=self.callback)

        if exchange is None:
            self.dst_queue = dst_queue
            self.channel.queue_declare(queue=dst_queue, durable=True)
        else:
            self.dst_queue = exchange
            self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)


    @abstractmethod
    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        pass

    def start(self):
        self.channel.start_consuming()

    def end(self):
        # nothing to clean up - MAY be overriden by subclasses.
        pass


class Filter(Worker):
    def __init__(self, rabbit_hostname, src_queue, dst_queue, filter_condition):
        self.filter_condition = filter_condition
        self.new(rabbit_hostname, src_queue, dst_queue)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        if self.filter_condition(body):
            self.channel.basic_publish(exchange='', routing_key=self.dst_queue, body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)


class Map(Worker):
    def __init__(self, rabbit_hostname, src_queue, dst_queue, map_fn):
        self.map_fn = map_fn
        self.new(rabbit_hostname, src_queue, dst_queue)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        self.channel.basic_publish(exchange='', routing_key=self.dst_queue, body=self.map_fn(body))
        ch.basic_ack(delivery_tag=method.delivery_tag)


class Aggregate(Worker):
    def __init__(self, rabbit_hostname, src_queue, dst_queue, aggregate_fn, result_fn, accumulator):
        self.aggregate_fn = aggregate_fn
        self.result_fn = result_fn
        self.accumulator = accumulator
        self.new(rabbit_hostname, src_queue, dst_queue)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        self.aggregate_fn(body, self.accumulator)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def end(self):
        return self.result_fn(self.accumulator)


class Sender(Worker):
    def __init__(self, rabbit_hostname, dst_queue):
        self.new(rabbit_hostname, '', dst_queue)
    
    def callback(self, ch, method, properties, body):
        pass

    def publish(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.dst_queue, body=message)

class Proxy(Worker):
    def __init__(self, rabbit_hostname, src_queue, exchange, exchange_type='direct', keys_getter = None):
        self.get_keys = keys_getter if keys_getter is not None else lambda x: ""
        self.new(rabbit_hostname, src_queue=src_queue, exchange=exchange, exchange_type=exchange_type)

    def callback(self, ch, method, properties, body: bytes):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        message = body.decode('utf-8').split('\n')
        headers = message.pop(0).split(',')

        for row in message:
            csv_file = StringIO(row)
            reader = DictReader(csv_file, fieldnames=headers)
            for row in reader:
                self.channel.basic_publish(exchange=self.dst_queue, routing_key=self.get_keys(row), body=dumps(row))



def wait_rabbitmq():
    """Pauses execution for few seconds in order start rabbitmq broker."""
    # this needs to be moved to the docker compose as a readiness check
    # we should first create rabbit, then workers, and finally start sending messages
    sleep(WAIT_TIME_PIKA)
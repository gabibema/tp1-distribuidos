from abc import ABC, abstractmethod
from csv import DictReader
from io import StringIO
from time import sleep

from json import dumps
from pika.exchange_type import ExchangeType

import pika

WAIT_TIME_PIKA = 15
MAX_KEY_LENGTH = 255

class Worker(ABC):

    def new(self, rabbit_hostname, src_queue='', src_exchange=None, src_routing_key=None, src_exchange_type=ExchangeType.direct, dst_exchange=None, dst_routing_key=None, dst_exchange_type=ExchangeType.direct, dst_queue=None):
        # init RabbitMQ channel
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_hostname))
        self.channel = connection.channel()
        # init source queue and bind to exchange
        self.channel.queue_declare(queue=src_queue)
        self.channel.basic_consume(queue=src_queue, on_message_callback=self.callback)

        if src_exchange:
            self.channel.exchange_declare(src_exchange, exchange_type=src_exchange_type)
            self.channel.queue_bind(src_queue, src_exchange, routing_key=src_routing_key)
        # init destination exchange
        if dst_exchange:
            self.dst_exchange = dst_exchange
            self.routing_key = dst_routing_key
            self.channel.exchange_declare(exchange=dst_exchange, exchange_type=dst_exchange_type)

        if dst_queue:
            self.dst_queue = dst_queue
            self.channel.queue_declare(queue=dst_queue)

    def connect_to_peers(self):
        # set up control queues between workers that consume from the same queue
        # this will be used for propagating client EOFs
        raise NotImplementedError


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
    def __init__(self, filter_condition, *args, **kwargs):
        self.filter_condition = filter_condition
        self.new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        if self.filter_condition(body):
            ch.basic_publish(exchange=self.dst_exchange, routing_key=self.routing_key, body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)


class Map(Worker):
    def __init__(self, map_fn, *args, **kwargs):
        self.map_fn = map_fn
        self.new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        ch.basic_publish(exchange=self.dst_exchange, routing_key=self.routing_key, body=self.map_fn(body))
        ch.basic_ack(delivery_tag=method.delivery_tag)


class Aggregate(Worker):
    def __init__(self, aggregate_fn, result_fn, accumulator, *args, **kwargs):
        self.aggregate_fn = aggregate_fn
        self.result_fn = result_fn
        self.accumulator = accumulator
        self.new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        self.aggregate_fn(body, self.accumulator)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def end(self):
        for msg in self.result_fn(self.accumulator):
            self.channel.basic_publish(exchange=self.dst_exchange, routing_key=self.routing_key, body=msg)


class Sender(Worker):
    def __init__(self, rabbit_hostname, dst_queue):
        self.new(rabbit_hostname, dst_queue=dst_queue)
    
    def callback(self, ch, method, properties, body):
        pass

    def publish(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.dst_queue, body=message)

class Proxy(Worker):
    def __init__(self, rabbit_hostname, src_queue, dst_exchange, keys_getter = None):
        self.get_keys = keys_getter if keys_getter is not None else lambda x: ""
        self.new(rabbit_hostname, src_queue=src_queue, dst_exchange=dst_exchange)

    def callback(self, ch, method, properties, body: bytes):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        message = body.decode('utf-8').split('\n')
        headers = message.pop(0).split(',')

        for row in message:
            csv_file = StringIO(row)
            reader = DictReader(csv_file, fieldnames=headers)
            for row in reader:
                self.channel.basic_publish(exchange=self.dst_exchange, routing_key=self.get_keys(row), body=dumps(row))



def wait_rabbitmq():
    """Pauses execution for few seconds in order start rabbitmq broker."""
    # this needs to be moved to the docker compose as a readiness probe
    # we should first create rabbit, then workers, and finally start sending messages
    sleep(WAIT_TIME_PIKA)

from abc import ABC, abstractmethod
from time import sleep, time
import json
import logging
from pika.exchange_type import ExchangeType
import pika

WAIT_TIME_PIKA=5

class Worker(ABC):

    def new(self, rabbit_hostname, src_queue='', src_exchange='', src_routing_key=[''], src_exchange_type=ExchangeType.direct, dst_exchange='', dst_routing_key='', dst_exchange_type=ExchangeType.direct):
        wait_rabbitmq()
        # init RabbitMQ channel
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_hostname))
        self.channel = connection.channel()
        self.channel.confirm_delivery()
        # init source queue and bind to exchange
        self.channel.queue_declare(queue=src_queue, durable=True)
        self.channel.basic_consume(queue=src_queue, on_message_callback=self.callback)
        if src_exchange:
            self.channel.exchange_declare(src_exchange, exchange_type=src_exchange_type)
            if type(src_routing_key) == str:
                src_routing_key = [src_routing_key]
            for routing_key in src_routing_key:
                self.channel.queue_bind(src_queue, src_exchange, routing_key=routing_key)
        if src_exchange_type == ExchangeType.topic:
            self.channel.queue_bind(src_queue, src_exchange, routing_key='EOF')
        # init destination exchange
        self.dst_exchange = dst_exchange
        if dst_exchange:
            self.channel.exchange_declare(exchange=dst_exchange, exchange_type=dst_exchange_type)
        self.routing_key = dst_routing_key

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
        if json.loads(body).get('type') == 'EOF':
            logging.warning(json.loads(body))
        if json.loads(body).get('type') == 'EOF' or self.filter_condition(body):
            ch.basic_publish(exchange=self.dst_exchange, routing_key=self.routing_key, body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)


class Map(Worker):
    def __init__(self, map_fn, *args, **kwargs):
        self.map_fn = map_fn
        self.new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        if json.loads(body).get('type') == 'EOF':
            logging.warning(json.loads(body))
            ch.basic_publish(exchange=self.dst_exchange, routing_key=self.routing_key, body=body)
        else:
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
        msg = json.loads(body)
        if msg.get('type') == 'EOF':
            logging.warning(json.loads(body))
            self.end(msg)
        else:
            self.aggregate_fn(msg, self.accumulator)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def end(self, message):
        msg = self.result_fn(message, self.accumulator)
        self.channel.basic_publish(exchange=self.dst_exchange, routing_key=self.routing_key, body=msg)


class Router(Worker):
    def __init__(self, routing_fn, *args, **kwargs):
        self.routing_fn = routing_fn
        self.new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        if json.loads(body).get('type') == 'EOF':
            logging.warning(json.loads(body))
        routing_keys = self.routing_fn(body)
        for routing_key in routing_keys:
            ch.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)


def wait_rabbitmq(host='rabbitmq', timeout=120, interval=10):
    """
    Waits for RabbitMQ to be available.
    """
    start_time = time()
    
    while time() - start_time < timeout:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            connection.close()
            return
        except pika.exceptions.AMQPConnectionError:
            sleep(interval)

    raise TimeoutError("RabbitMQ did not become available within the timeout period.")



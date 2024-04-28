from abc import ABC, abstractmethod
from pika.exchange_type import ExchangeType
import pika

class Worker(ABC):

    def new(self, rabbit_hostname, src_queue='', src_exchange=None, src_routing_key=None, src_exchange_type=ExchangeType.direct, dst_exchange=None, dst_routing_key=None, dst_exchange_type=ExchangeType.direct):
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
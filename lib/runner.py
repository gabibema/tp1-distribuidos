from abc import ABC, abstractmethod
import sys, os
import pika


class Runner(ABC):
    def new(self, rabbit_hostname, src_queue, dst_queue):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_hostname))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=src_queue, durable=True)
        self.channel.basic_consume(queue=src_queue, on_message_callback=self.callback, auto_ack=True)
        self.dst_queue = dst_queue
        self.channel.queue_declare(queue=dst_queue, durable=True)

    @abstractmethod
    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        pass

    def start(self):
        self.channel.start_consuming()

    def end(self):
        # nothing to clean up - MAY be overriden by subclasses.
        pass


class Filter(Runner):
    def __init__(self, rabbit_hostname, src_queue, dst_queue, filter_condition):
        self.filter_condition = filter_condition
        self.new(rabbit_hostname, src_queue, dst_queue)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        if self.filter_condition(body):
            self.channel.basic_publish(exchange='', routing_key=self.dst_queue, body=message)
        ch.basic_ack(delivery_tag=method.delivery_tag)


class Map(Runner):
    def __init__(self, rabbit_hostname, src_queue, dst_queue, map_fn):
        self.map_fn = map_fn
        self.new(rabbit_hostname, src_queue, dst_queue)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        self.channel.basic_publish(exchange='', routing_key=self.dst_queue, body=map_fn(message))
        ch.basic_ack(delivery_tag=method.delivery_tag)


class Aggregate(Runner):
    def __init__(self, rabbit_hostname, src_queue, dst_queue, aggregate_fn, result_fn, accumulator):
        self.aggregate_fn = aggregate_fn
        self.result_fn = result_fn
        self.accumulator = accumulator
        self.new(rabbit_hostname, src_queue, dst_queue)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        self.aggregate_fn(message, self.accumulator)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def end():
        return self.result_fn(self.accumulator)

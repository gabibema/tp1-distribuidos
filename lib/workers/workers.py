from abc import ABC, abstractmethod
from time import sleep, time
import json
import logging
from pika.exchange_type import ExchangeType
import pika
from uuid import uuid4

WAIT_TIME_PIKA=5

class Worker(ABC):
    def new(self, connection, control_queue_prefix, src_queue='', src_exchange='', src_routing_key=[''], src_exchange_type=ExchangeType.direct, dst_exchange='', dst_routing_key='', dst_exchange_type=ExchangeType.direct):
        self.id = str(uuid4())
        self.connection = connection
        self.connection.create_control_queue(control_queue_prefix, self.control_callback)
        self.connection.channel.basic_qos(prefetch_count=50, global_qos=True)

        # init source queue and bind to exchange
        self.connection.create_queue(src_queue, persistent=True)
        self.connection.set_consumer(src_queue, self.callback)

        if src_exchange:
            self.connection.create_router(src_exchange, src_exchange_type)
            if isinstance(src_routing_key, str):
                src_routing_key = [src_routing_key]
            for routing_key in src_routing_key:
                self.connection.link_queue(src_queue, src_exchange, routing_key)

        if src_exchange_type == ExchangeType.topic:
            self.connection.link_queue(src_queue, src_exchange, routing_key='EOF')

        # init destination exchange
        self.dst_exchange = dst_exchange
        if dst_exchange:
            self.connection.create_router(dst_exchange, dst_exchange_type)
        self.routing_key = dst_routing_key

    @abstractmethod
    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        pass

    def control_callback(self, ch, method, properties, body):
        'Callback given to a control queue to invoke for each message in the queue'
        message = json.loads(body)
        if message.get('type') == 'NEW_PEER':
            # Insert peer in linked list, between self and the next peer.
            old_next_peer = self.next_peer
            self.next_peer = message['peer_name']
            # Tell the new peer who their next peer is.
            message = {'type': 'SET_NEXT_PEER', 'peer_name': old_next_peer}
            self.connection.send_message('', self.next_peer, json.dumps(message))
        elif message.get('type') == 'SET_NEXT_PEER':
            self.next_peer = message['peer_name']
            self.start()
        elif message.get('type') == 'EOF':
            if message['intended_recipient'] == self.id:
                self.end(message)
            else:
                self.connection.send_message('', self.next_peer, body)
        self.connection.acknowledge_message(method.delivery_tag)

    def start(self):
        self.connection.begin_consuming()

    def end(self, eof_message):
        # nothing to clean up - MAY be overriden by subclasses.
        pass


class Filter(Worker):
    def __init__(self, filter_condition, connection, *args, **kwargs):
        self.filter_condition = filter_condition
        super().new(connection=connection, *args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        if json.loads(body).get('type') == 'EOF':
            logging.warning(json.loads(body))
        if json.loads(body).get('type') == 'EOF' or self.filter_condition(body):
            self.connection.send_message(self.dst_exchange, self.routing_key, body)
        self.connection.acknowledge_message(method.delivery_tag)


class Map(Worker):
    def __init__(self, map_fn, connection, *args, **kwargs):
        self.map_fn = map_fn
        super().new(connection=connection, *args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        if json.loads(body).get('type') == 'EOF':
            logging.warning(json.loads(body))
            self.connection.send_message(self.dst_exchange, self.routing_key, body)
        else:
            self.connection.send_message(self.dst_exchange, self.routing_key, self.map_fn(body))
        self.connection.acknowledge_message(method.delivery_tag)


class Aggregate(Worker):
    def __init__(self, aggregate_fn, result_fn, accumulator, connection, *args, **kwargs):
        self.aggregate_fn = aggregate_fn
        self.result_fn = result_fn
        self.accumulator = accumulator
        super().new(connection=connection, *args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        messages = json.loads(body)
        if type(messages) != list:
            messages = [messages]
        for msg in messages:
            if msg.get('type') == 'EOF':
                logging.warning(json.loads(body))
                self.end(msg)
            else:
                self.aggregate_fn(msg, self.accumulator)
        self.connection.acknowledge_message(method.delivery_tag)

    def end(self, eof_message):
        logging.warning(f'{self.dst_exchange=}, {self.routing_key=}')
        msg = self.result_fn(eof_message, self.accumulator)
        self.connection.send_message(self.dst_exchange, self.routing_key, msg)
        self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(eof_message))


class Router(Worker):
    def __init__(self, routing_fn, connection, *args, **kwargs):
        self.routing_fn = routing_fn
        super().new(connection=connection, *args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        if json.loads(body).get('type') == 'EOF':
            logging.warning(json.loads(body))
        routing_keys = self.routing_fn(body)
        for routing_key in routing_keys:
            self.connection.send_message(self.dst_exchange, routing_key, body)
        self.connection.acknowledge_message(method.delivery_tag)


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



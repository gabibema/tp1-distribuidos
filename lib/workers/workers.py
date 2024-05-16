from abc import ABC, abstractmethod
from time import sleep, time
import json
import logging
from pika.exchange_type import ExchangeType
import pika
from uuid import uuid4

WAIT_TIME_PIKA=5

class Worker(ABC):
    def new(self, connection, src_queue='', src_exchange='', src_routing_key=[''], src_exchange_type=ExchangeType.direct, dst_exchange='', dst_routing_key='', dst_exchange_type=ExchangeType.direct):
        self.connection = connection
        self.connection.channel.basic_qos(prefetch_count=50, global_qos=True)

        # init source queue and bind to exchange
        self.connection.create_queue(src_queue, persistent=True)
        self.src_queue = src_queue

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

    def start(self):
        self.connection.set_consumer(self.src_queue, self.callback)
        self.connection.begin_consuming()

    def end(self, ch, method, properties, body):
        # nothing to do - MAY be overriden by subclasses.
        pass


class ParallelWorker(Worker):
    def new(self, control_queue_prefix, *args, **kwargs):
        super().new(*args, **kwargs)
        self.id = str(uuid4())
        self.connection.create_control_queue(control_queue_prefix, self.control_callback)

    def control_callback(self, ch, method, properties, body):
        'Callback given to a control queue to invoke for each message in the queue'
        message = json.loads(body)
        if message.get('type') == 'NEW_PEER':
            # Insert peer in linked list, between self and the next peer.
            old_next_peer = self.connection.next_peer
            self.connection.next_peer = message['peer_name']
            # Tell the new peer who their next peer is.
            message = {'type': 'SET_NEXT_PEER', 'peer_name': old_next_peer}
            self.connection.send_message('', self.connection.next_peer, json.dumps(message))
        elif message.get('type') == 'SET_NEXT_PEER':
            self.connection.next_peer = message['peer_name']
            # Worker is ready to process incoming messages, subscribe it to work queue.
            self.connection.set_consumer(self.src_queue, self.callback)
        elif message.get('type') == 'EOF':
            if message['intended_recipient'] == self.id:
                self.end(ch, method, properties, body)
            else:
                self.connection.send_message('', self.connection.next_peer, body)
        self.connection.acknowledge_message(method.delivery_tag)

    def end(self, ch, method, properties, body):
        'Send EOF to next layer'
        eof_message = json.loads(body)
        del eof_message['intended_recipient']
        self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(eof_message))


class Filter(ParallelWorker):
    def __init__(self, filter_condition, *args, **kwargs):
        self.filter_condition = filter_condition
        super().new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        message = json.loads(body)
        if message.get('type') == 'EOF':
            logging.warning(message)
            message['intended_recipient'] = self.id
            self.connection.send_message('', self.connection.next_peer, json.dumps(message))
        elif self.filter_condition(body):
            self.connection.send_message(self.dst_exchange, self.routing_key, body)
        self.connection.acknowledge_message(method.delivery_tag)


class Map(ParallelWorker):
    def __init__(self, map_fn, *args, **kwargs):
        self.map_fn = map_fn
        super().new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        message = json.loads(body)
        if message.get('type') == 'EOF':
            logging.warning(message)
            message['intended_recipient'] = self.id
            self.connection.send_message('', self.connection.next_peer, json.dumps(message))
        else:
            self.connection.send_message(self.dst_exchange, self.routing_key, self.map_fn(body))
        self.connection.acknowledge_message(method.delivery_tag)


class Aggregate(Worker):
    def __init__(self, aggregate_fn, result_fn, accumulator, *args, **kwargs):
        self.aggregate_fn = aggregate_fn
        self.result_fn = result_fn
        self.accumulator = accumulator
        super().new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        messages = json.loads(body)
        if type(messages) != list:
            messages = [messages]
        for msg in messages:
            if msg.get('type') == 'EOF':
                logging.warning(json.loads(body))
                self.end(ch, method, properties, json.dumps(msg))
            else:
                self.aggregate_fn(msg, self.accumulator)
        self.connection.acknowledge_message(method.delivery_tag)

    def end(self, ch, method, properties, body):
        eof_message = json.loads(body)
        logging.warning(f'{self.dst_exchange=}, {self.routing_key=}')
        msg = self.result_fn(eof_message, self.accumulator)
        self.connection.send_message(self.dst_exchange, self.routing_key, msg)
        self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(eof_message))


class Router(Worker):
    def __init__(self, routing_fn, *args, **kwargs):
        self.routing_fn = routing_fn
        super().new(*args, **kwargs)

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



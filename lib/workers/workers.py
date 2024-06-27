from abc import ABC, abstractmethod
from time import sleep, time
from uuid import uuid4
import json
import logging
from multiprocessing import Process
from pika.exchange_type import ExchangeType
from lib.healthcheck import Healthcheck, HEALTH
import pika
from pika.exchange_type import ExchangeType
from lib.fault_tolerance import save_state, load_state, is_duplicate

WAIT_TIME_PIKA=5

class Worker(ABC):
    def new(self, connection, src_queue='', src_exchange='', src_routing_key=[''], src_exchange_type=ExchangeType.direct, dst_exchange='', dst_routing_key='', dst_exchange_type=ExchangeType.direct):
        self.connection = connection
        self.connection.channel.basic_qos(prefetch_count=1, global_qos=True)

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
        self.healthcheck = Process(target=Healthcheck().listen_healthchecks)
        self.health = HEALTH

    @abstractmethod
    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        pass

    def start(self):
        self.healthcheck.start()
        self.connection.set_consumer(self.src_queue, self.callback)
        self.connection.begin_consuming()

    def end(self, ch, method, properties, body):
        # nothing to do - MAY be overriden by subclasses.
        pass


class ParallelWorker(Worker):
    def new(self, control_queue_prefix, *args, **kwargs):
        super().new(*args, **kwargs)
        state = load_state()
        self.id = state.get('id', str(uuid4()))
        self.peers = state.get('peers', [self.id])
        self.finished_peers = state.get('finished_peers', {})
        self.peer_agora = control_queue_prefix
        self.connection.create_control_queue(control_queue_prefix, self.control_callback, self.src_queue, self.callback, self.id)

    def control_callback(self, ch, method, properties, body):
        'Callback given to a control queue to invoke for each message in the queue'
        message = json.loads(body)
        if message.get('type') == 'NEW_PEER' and message.get('sender_id') != self.id:
            known_peers = self.peers
            if message['sender_id'] not in self.peers:
                self.peers.append(message['sender_id'])
            message = {'type': 'ADD_PEERS', 'peer_ids': known_peers, 'sender_id': self.id}
            self.connection.send_message(self.peer_agora, self.peer_agora, json.dumps(message))
        elif message.get('type') == 'ADD_PEERS' and message.get('sender_id') != self.id:
            self.peers += [peer for peer in message['peer_ids'] if peer not in self.peers]
        elif message.get('type') == 'EOF':
            if message['intended_recipient'] == 'BROADCAST':
                # EOF was received by another worker and is expecting an answer.
                message['intended_recipient'] = message['sender_id']
                message['sender_id'] = self.id
                self.connection.send_message(self.peer_agora, self.peer_agora, json.dumps(message))
            elif message['intended_recipient'] == self.id and self.finished_peers:
                # Other workers are responding to this worker's EOF.
                if message['sender_id'] not in self.finished_peers[message['request_id']]:
                    self.finished_peers[message['request_id']].append(message['sender_id'])
                if self.finished_peers[message['request_id']] == self.peers:
                    del self.finished_peers[message['request_id']]
                    self.end(ch, method, properties, body)
        save_state(id=self.id, peers=self.peers, finished_peers=self.finished_peers)
        self.connection.acknowledge_message(method.delivery_tag)

    def end(self, ch, method, properties, body):
        'Send EOF to next layer'
        eof_message = json.loads(body)
        del eof_message['intended_recipient']
        del eof_message['sender_id']
        self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(eof_message))


class Filter(ParallelWorker):
    def __init__(self, filter_condition, *args, **kwargs):
        self.filter_condition = filter_condition
        super().new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        batch = json.loads(body)
        if batch['items'] and batch['items'][0].get('type') == 'EOF':
            logging.warning(batch)
            message = {'request_id': batch['request_id'], 'message_id': batch['message_id'], 'type': 'EOF', 'items': batch['items'], 'sender_id': self.id, 'intended_recipient': 'BROADCAST'}
            self.connection.send_message(self.peer_agora, self.peer_agora, json.dumps(message))
            self.finished_peers[message['request_id']] = [self.id]
            save_state(id=self.id, peers=self.peers, finished_peers=self.finished_peers)
        else:
            batch['items'] = [msg for msg in batch['items'] if self.filter_condition(json.dumps(msg))]
            self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(batch))
        self.connection.acknowledge_message(method.delivery_tag)


class Map(ParallelWorker):
    def __init__(self, map_fn, *args, **kwargs):
        self.map_fn = map_fn
        super().new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        batch = json.loads(body)
        if batch['items'] and batch['items'][0].get('type') == 'EOF':
            logging.warning(batch)
            message = {'request_id': batch['request_id'], 'message_id': batch['message_id'], 'type': 'EOF', 'items': batch['items'], 'sender_id': self.id, 'intended_recipient': 'BROADCAST'}
            self.connection.send_message(self.peer_agora, self.peer_agora, json.dumps(message))
            self.finished_peers[message['request_id']] = [self.id]
            save_state(id=self.id, peers=self.peers, finished_peers=self.finished_peers)
        else:
            mapped_messages = [self.map_fn(item) for item in batch['items']]
            message = {'request_id': batch['request_id'], 'message_id': batch['message_id'], 'items': mapped_messages}
            self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(message))
        self.connection.acknowledge_message(method.delivery_tag)


class Router(ParallelWorker):
    def __init__(self, routing_fn, *args, **kwargs):
        self.routing_fn = routing_fn
        super().new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        batch = json.loads(body)
        if batch['items'] and batch['items'][0].get('type') == 'EOF':
            logging.warning(batch)
            message = {'request_id': batch['request_id'], 'message_id': batch['message_id'], 'type': 'EOF', 'items': batch['items'], 'sender_id': self.id, 'intended_recipient': 'BROADCAST'}
            self.connection.send_message(self.peer_agora, self.peer_agora, json.dumps(message))
            self.finished_peers[message['request_id']] = [self.id]
            save_state(id=self.id, peers=self.peers, finished_peers=self.finished_peers)
        else:
            messages_per_target = {}
            for msg in batch['items']:
                for routing_key in self.routing_fn(json.dumps(msg)):
                    messages_per_target[routing_key] = messages_per_target.get(routing_key, [])
                    messages_per_target[routing_key].append(msg)
            for routing_key, messages in messages_per_target.items():
                batch['items'] = messages
                self.connection.send_message(self.dst_exchange, routing_key, json.dumps(batch))
        self.connection.acknowledge_message(method.delivery_tag)

    def end(self, ch, method, properties, body):
        'Send EOF to next layer'
        eof_message = json.loads(body)
        del eof_message['intended_recipient']
        routing_keys = self.routing_fn(body)
        for routing_key in routing_keys:
            self.connection.send_message(self.dst_exchange, routing_key, json.dumps(eof_message))


class Aggregate(Worker):
    def __init__(self, aggregate_fn, result_fn, accumulator, *args, **kwargs):
        self.aggregate_fn = aggregate_fn
        self.result_fn = result_fn
        state = load_state()
        self.accumulator = state.get("accumulator", accumulator)
        self.duplicate_filter = state.get("duplicate_filter", {})
        super().new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a queue to invoke for each message in the queue'
        batch = json.loads(body)
        if is_duplicate(batch['request_id'], batch['message_id'], self.duplicate_filter):
            # There's no need to update state for duplicate messages.
            self.connection.acknowledge_message(method.delivery_tag)
            return
        for item in batch['items']:
            message = {'request_id': batch['request_id'], 'message_id': batch['message_id']}
            message.update(item)
            if message.get('type') == 'EOF':
                logging.warning(message)
                self.end(ch, method, properties, body)
            else:
                self.aggregate_fn(message, self.accumulator)
        save_state(accumulator=self.accumulator, duplicate_filter=self.duplicate_filter)
        self.connection.acknowledge_message(method.delivery_tag)

    def end(self, ch, method, properties, body):
        eof_message = json.loads(body)
        message_id = 1
        result = self.result_fn(eof_message, self.accumulator)
        messages = json.loads(result)
        if type(messages) != list:
            logging.error('Result is not a list')
            messages = [messages]
        for message in messages:
            message['message_id'] = message_id
            message_id += 1
            self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(message))
        eof_message['message_id'] = message_id
        self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(eof_message))


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



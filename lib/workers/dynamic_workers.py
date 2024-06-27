import json
import logging
from uuid import uuid4
from abc import abstractmethod
from .workers import Worker, ParallelWorker
from lib.fault_tolerance import save_state, load_state, is_repeated

class DynamicWorker(Worker):
    """
    Wrapper around class Worker.
    Keeps a state with the temp queues that have been created, and dynamically creates new ones when there's a new request_id.
    Prevents subclasses from having to manage temporary queues.
    """

    def new(self, tmp_queues, connection, *args, **kwargs):
        self.tmp_queues = tmp_queues
        self.ongoing_requests = set()
        super().new(connection=connection, *args, **kwargs)

    @abstractmethod
    def inner_callback(self, ch, method, properties, msg):
        pass

    def callback(self, ch, method, properties, body):
        """
        Wrapper to create tmp_queues before invoking the actual callback.
        This is to ensure that the callback fn will always publish to an existing queue.
        """
        message = json.loads(body)
        if message['request_id'] not in self.ongoing_requests:
            self.create_queues(message['request_id'])
        self.inner_callback(ch, method, properties, message)
        if message['items'] and message['items'][0].get('type') == 'EOF':
            logging.warning(json.loads(body))
            self.ongoing_requests.discard(message['request_id'])
            # DON'T delete queues yet! messages need to be consumed.
            # queues should be deleted by consumer after reading the EOF.

    def create_queues(self, request_id):
        for queue_prefix, routing_key in self.tmp_queues:
            new_dst_queue = f"{queue_prefix}_{request_id}_queue"
            self.connection.create_queue(new_dst_queue, persistent=True)
            if routing_key:
                routing_key = f"{routing_key}_{request_id}"
                self.connection.link_queue(new_dst_queue, self.dst_exchange, routing_key=routing_key)
        self.ongoing_requests.add(request_id)


class DynamicRouter(DynamicWorker):
    control_callback = ParallelWorker.control_callback

    def __init__(self, routing_fn, control_queue_prefix, *args, **kwargs):
        self.routing_fn = routing_fn
        super().new(*args, **kwargs)
        state = load_state()
        self.id = state.get('id', str(uuid4()))
        self.peers = state.get('peers', [self.id])
        self.finished_peers = state.get('finished_peers', {})
        self.peer_agora = control_queue_prefix
        self.connection.create_control_queue(control_queue_prefix, self.control_callback, self.src_queue, self.callback, self.id)

    def callback(self, ch, method, properties, body):
        """
        Wrapper to create tmp_queues before invoking the actual callback.
        This is to ensure that the callback fn will always publish to an existing queue.
        """
        message = json.loads(body)
        if message['request_id'] not in self.ongoing_requests:
            self.create_queues(message['request_id'])
        if message['items'] and message['items'][0].get('type') == 'EOF':
            message = {'request_id': message['request_id'], 'message_id': message['message_id'], 'items': message['items'], 'type': 'EOF', 'sender_id': self.id, 'intended_recipient': 'BROADCAST'}
            self.connection.send_message(self.peer_agora, self.peer_agora, json.dumps(message))
            self.finished_peers[message['request_id']] = [self.id]
            save_state(id=self.id, peers=self.peers, finished_peers=self.finished_peers)
        else:
            self.inner_callback(ch, method, properties, message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def inner_callback(self, ch, method, properties, batch):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        messages_per_target = {}
        for msg in batch['items']:
            message = {'request_id': batch['request_id'], 'message_id': batch['message_id']}
            message.update(msg)
            for routing_key in self.routing_fn(message):
                messages_per_target[routing_key] = messages_per_target.get(routing_key, [])
                messages_per_target[routing_key].append(msg)
        for routing_key, messages in messages_per_target.items():
            batch['items'] = messages
            ch.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=json.dumps(batch))

    def end(self, ch, method, properties, body):
        'Send EOF to next layer'
        eof_message = json.loads(body)
        del eof_message['intended_recipient']
        del eof_message['sender_id']
        for routing_key in self.routing_fn(eof_message):
            ch.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=json.dumps(eof_message))
        self.ongoing_requests.discard(eof_message['request_id'])
        # DON'T delete queues yet! messages need to be consumed.
        # queues should be deleted by consumer after reading the EOF.


class DynamicAggregate(DynamicWorker):
    def __init__(self, aggregate_fn, result_fn, accumulator, *args, **kwargs):
        self.aggregate_fn = aggregate_fn
        self.result_fn = result_fn
        state = load_state()
        self.accumulator = state.get("accumulator", accumulator)
        super().new(*args, **kwargs)

    def inner_callback(self, ch, method, properties, msg):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        if msg['items'] and msg['items'][0].get('type') == 'EOF':
            msg['type'] = msg['items'][0]['type']
            self.end(msg)
        else:
            self.aggregate_fn(msg, self.accumulator)
        save_state(accumulator=self.accumulator)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def end(self, eof_message):
        message_id = 1
        messages = self.result_fn(eof_message, self.accumulator)
        for msg in messages:
            msg['message_id'] = message_id
            message_id += 1
            routing_key = f"{self.routing_key}_{msg['request_id']}"
            self.connection.send_message(self.dst_exchange, routing_key, json.dumps(msg))
        eof_message['message_id'] = message_id
        routing_key = f"{self.routing_key}_{eof_message['request_id']}"
        self.connection.send_message(self.dst_exchange, routing_key, json.dumps(eof_message))


class DynamicFilter(Worker):
    """
    Wrapper around class Worker.
    Subscribes to a queue where different filter conditions are announced for each request,
    and subscribes to the request specific queues when that condition arrives.
    """
    def __init__(self, update_state, filter_condition, tmp_queues_prefix, connection, *args, **kwargs):
        self.update_state = update_state
        self.filter_condition = filter_condition
        self.tmp_queues_prefix = tmp_queues_prefix
        self.connection = connection
        self.recover_from_state(load_state())
        super().new(connection, *args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback used to update the internal state, to change how future messages are filtered'
        msg = json.loads(body)
        if is_repeated(msg['request_id'], msg['message_id'], self.duplicates_state):
            # There's no need to update state for duplicate messages.
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        # Ignore EOFs through this queue. Each client will send their EOF through the tmp queue.
        if msg.get('type') != 'EOF':
            self.filter_state = self.update_state(self.filter_state, msg)
            # If there is a new client, subscribe to the new queue
            new_tmp_queue = f"{self.tmp_queues_prefix}_{msg['request_id']}_queue"
            self.connection.create_queue(new_tmp_queue, persistent=True)
            self.connection.set_consumer(new_tmp_queue, self.filter_callback)
            save_state(filter_state=self.filter_state, duplicates_state=self.duplicates_state)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def client_EOF(self, body):
        msg = json.loads(body)
        msg['type'] = 'EOF'
        self.filter_state = self.update_state(self.filter_state, msg)
        tmp_queue = f"{self.tmp_queues_prefix}_{msg['request_id']}_queue"
        self.connection.channel.queue_delete(queue=tmp_queue)
        save_state(filter_state=self.filter_state, duplicates_state=self.duplicates_state)

    def filter_callback(self, ch, method, properties, body):
        'Callback used to filter messages in a queue'
        batch = json.loads(body)
        if batch['items'] and batch['items'][0].get('type') == 'EOF':
            self.client_EOF(body)
            self.connection.send_message(self.dst_exchange, self.routing_key, body)
        else:
            filtered_messages = []
            for item in batch['items']:
                message = {'request_id': batch['request_id'], 'message_id': batch['message_id']}
                message.update(item)
                if self.filter_condition(self.filter_state, message):
                    filtered_messages.append(item)
            batch['items'] = filtered_messages
            self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(batch))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def recover_from_state(self, state):
        self.duplicates_state = state.get("duplicates_state", {})
        self.filter_state = state.get("filter_state", {})
        for request_id in self.filter_state:
            new_tmp_queue = f"{self.tmp_queues_prefix}_{msg['request_id']}_queue"
            self.connection.create_queue(new_tmp_queue, persistent=True)
            self.connection.set_consumer(new_tmp_queue, self.filter_callback)

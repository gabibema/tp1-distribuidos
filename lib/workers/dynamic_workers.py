import json
import logging
from abc import abstractmethod
from .workers import Worker, ParallelWorker

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
        msg = message[-1] if isinstance(message, list) else message
        if msg['request_id'] not in self.ongoing_requests:
            self.create_queues(msg['request_id'])
        self.inner_callback(ch, method, properties, message)
        if msg.get('type') == 'EOF':
            logging.warning(json.loads(body))
            self.ongoing_requests.discard(msg['request_id'])
            # DON'T delete queues yet! messages need to be consumed.
            # queues should be deleted by consumer after reading the EOF.

    def create_queues(self, request_id):
        for queue_prefix, routing_key in self.tmp_queues:
            new_dst_queue = f"{queue_prefix}_{request_id}_queue"
            self.connection.create_queue(new_dst_queue, persistent=True)
            if routing_key:
                routing_key = f"{routing_key}_{request_id}"
                self.connection.link_queue(new_dst_queue, self.dst_exchange, routing_key=routing_key)


class DynamicRouter(DynamicWorker):
    control_callback = ParallelWorker.control_callback

    def __init__(self, routing_fn, control_queue_prefix, *args, **kwargs):
        self.routing_fn = routing_fn
        super().new(*args, **kwargs)
        self.connection.create_control_queue(control_queue_prefix, self.control_callback)

    def callback(self, ch, method, properties, body):
        """
        Wrapper to create tmp_queues before invoking the actual callback.
        This is to ensure that the callback fn will always publish to an existing queue.
        """
        message = json.loads(body)
        msg = message[-1] if isinstance(message, list) else message
        if msg['request_id'] not in self.ongoing_requests:
            self.create_queues(msg['request_id'])
        if msg.get('type') == 'EOF':
            msg['intended_recipient'] = self.connection.id
            self.connection.send_message('', self.connection.next_peer, json.dumps(msg))
        else:
            self.inner_callback(ch, method, properties, message)

    def inner_callback(self, ch, method, properties, messages):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        for msg in messages:
            for routing_key in self.routing_fn(msg):
                ch.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=json.dumps(msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def end(self, ch, method, properties, body):
        'Send EOF to next layer'
        eof_message = json.loads(body)
        del eof_message['intended_recipient']
        for routing_key in self.routing_fn(eof_message):
            ch.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=json.dumps(eof_message))
        self.ongoing_requests.discard(eof_message['request_id'])
        # DON'T delete queues yet! messages need to be consumed.
        # queues should be deleted by consumer after reading the EOF.


class DynamicAggregate(DynamicWorker):
    def __init__(self, aggregate_fn, result_fn, accumulator, *args, **kwargs):
        self.aggregate_fn = aggregate_fn
        self.result_fn = result_fn
        self.accumulator = accumulator
        super().new(*args, **kwargs)

    def inner_callback(self, ch, method, properties, msg):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        if msg.get('type') == 'EOF':
            self.end(msg)
        else:
            self.aggregate_fn(msg, self.accumulator)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def end(self, eof_message):
        messages = self.result_fn(eof_message, self.accumulator)
        for msg in messages:
            routing_key = f"{self.routing_key}_{msg['request_id']}"
            self.connection.send_message(self.dst_exchange, routing_key, json.dumps(msg))
        routing_key = f"{self.routing_key}_{eof_message['request_id']}"
        self.connection.send_message(self.dst_exchange, routing_key, json.dumps(eof_message))


class DynamicFilter(Worker):
    """
    Wrapper around class Worker.
    Subscribes to a queue where different filter conditions are announced for each request,
    and subscribes to the request specific queues when that condition arrives.
    """
    def __init__(self, update_state, filter_condition, tmp_queues_prefix, *args, **kwargs):
        self.update_state = update_state
        self.filter_condition = filter_condition
        self.tmp_queues_prefix = tmp_queues_prefix
        self.state = {}
        super().new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback used to update the internal state, to change how future messages are filtered'
        msg = json.loads(body)
        # Ignore EOFs through this queue. Each client will send their EOF through the tmp queue.
        if msg.get('type') != 'EOF':
            self.state = self.update_state(self.state, msg)
            # If there is a new client, subscribe to the new queue
            new_tmp_queue = f"{self.tmp_queues_prefix}_{msg['request_id']}_queue"
            self.connection.create_queue(new_tmp_queue, persistent=True)
            self.connection.set_consumer(new_tmp_queue, self.filter_callback)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def client_EOF(self, body):
        msg = json.loads(body)
        self.state = self.update_state(self.state, msg)
        tmp_queue = f"{self.tmp_queues_prefix}_{msg['request_id']}_queue"
        self.connection.channel.queue_delete(queue=tmp_queue)

    def filter_callback(self, ch, method, properties, body):
        'Callback used to filter messages in a queue'
        msg = json.loads(body)
        if msg.get('type') == 'EOF':
            self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(msg))
            self.client_EOF(body)
        elif self.filter_condition(self.state, msg):
            self.connection.send_message(self.dst_exchange, self.routing_key, json.dumps(msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)

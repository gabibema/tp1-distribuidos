import json
from abc import abstractmethod
from .workers import Worker

class DynamicWorker(Worker):
    """
    Wrapper around class Worker.
    Keeps a state with the temp queues that have been created, and dynamically creates new ones when there's a new request_id.
    Prevents subclasses from having to manage temporary queues.
    """

    def new(self, tmp_queues, *args, **kwargs):
        self.tmp_queues = tmp_queues
        self.ongoing_requests = set()
        super().new(*args, **kwargs)

    @abstractmethod
    def inner_callback(self, ch, method, properties, body):
        pass

    def callback(self, ch, method, properties, body):
        """
        Wrapper to create tmp_queues before invoking the actual callback.
        This is to ensure that the callback fn will always publish to an existing queue.
        """
        # Pending: if msg is an EOF, remove request from self.ongoing_requests
        # if there's no queues for this request_id, create them.
        if body['request_id'] not in self.ongoing_requests:
            # create queues and subscribe them to dst_exchange
            for queue_prefix, routing_key in self.tmp_queues:
                # 2 queues can suscribe to the same messages using the same routing_key, delegate that option to the user.
                new_dst_queue = f"{queue_prefix}_{body['request_id']}_queue"
                self.channel.queue_declare(queue=new_dst_queue, durable=True)
                if routing_key != '':
                    routing_key = f"{routing_key}_{body['request_id']}"
                    self.channel.queue_bind(new_dst_queue, self.dst_exchange, routing_key=routing_key)
        self.inner_callback(ch, method, properties, body)


class DynamicRouter(DynamicWorker):
    def __init__(self, routing_fn, *args, **kwargs):
        self.routing_fn = routing_fn
        self.new(*args, **kwargs)

    def inner_callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        routing_keys = self.routing_fn(body)
        for routing_key in routing_keys:
            ch.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)


class DynamicAggregate(DynamicWorker):
    def __init__(self, aggregate_fn, result_fn, accumulator, *args, **kwargs):
        self.aggregate_fn = aggregate_fn
        self.result_fn = result_fn
        self.accumulator = accumulator
        self.new(*args, **kwargs)

    def inner_callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        self.aggregate_fn(body, self.accumulator)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def end(self):
        for msg in self.result_fn(self.accumulator):
            routing_key = f"{self.routing_key}_{msg['request_id']}"
            self.channel.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=msg)


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
        self.new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback used to update the internal state, to change how future messages are filtered'
        msg = json.loads(body)
        self.state = self.update_state(self.state, msg)
        # If there is a new client, subscribe to the new queue
        new_tmp_queue = f"{self.tmp_queues_prefix}_{msg['request_id']}_queue"
        ch.queue_declare(queue=new_tmp_queue, durable=True)
        ch.basic_consume(queue=new_tmp_queue, on_message_callback=self.filter_callback)

    def client_eof(self, msg):
        # Pending: If an EOF arrives, delete client queue
        self.state = self.update_state(self.state, msg)

    def filter_callback(self, ch, method, properties, body):
        'Callback used to filter messages in a queue'
        # Pending: If message is an EOF, delegate to self.client_eof
        if self.filter_condition(self.state, body):
            ch.basic_publish(exchange=self.dst_exchange, routing_key=self.routing_key, body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

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
        if msg['request_id'] not in self.ongoing_requests:
            # create queues and subscribe them to dst_exchange
            for queue_prefix, routing_key in self.tmp_queues:
                # 2 queues can suscribe to the same messages using the same routing_key, delegate that option to the user.
                new_dst_queue = f"{queue_prefix}_{msg['request_id']}_queue"
                self.channel.queue_declare(queue=new_dst_queue)
                if routing_key is not '':
                    routing_key = f"{routing_key}_{msg['request_id']}_queue"
                    self.channel.queue_bind(new_dst_queue, self.dst_exchange, routing_key=routing_key)
        self.inner_callback(ch, method, properties, body)


class DynamicRouter(DynamicWorker):
    def __init__(self, routing_fn, *args, **kwargs):
        self.routing_fn = routing_fn
        self.new(*args, **kwargs)

    def inner_callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        routing_key = self.routing_fn(body)
        ch.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

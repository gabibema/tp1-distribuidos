import json
from .workers import Worker

class StatefulFilter(Worker):
    def __init__(self, update_state, filter_condition, tmp_queues_prefix, *args, **kwargs):
        self.update_state = update_state
        self.filter_condition = filter_condition
        self.tmp_queues_prefix = tmp_queues_prefix
        self.new(*args, src_routing_key=src_routing_key, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback used to update the internal state, to change how future messages are filtered'
        msg = json.loads(body)
        self.state = self.update_state(self.state, msg)
        # If there is a new client, subscribe to the new queue
        new_tmp_queue = f"{self.tmp_queues_prefix}_{msg['request_id']}_queue"
        ch.queue_declare(queue=new_tmp_queue)
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

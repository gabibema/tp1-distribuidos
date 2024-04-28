import json
from .workers import Worker

class Sharder(Worker):
    def __init__(self, hash_fn, *args, **kwargs):
        # Pending: read amount of shards from env
        self.shards_count = 1
        self.hash_fn
        self.new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        msg = json.loads(body)
        shard_number = self.hash_fn(msg) % self.shards_count
        routing_key = f"{self.routing_key}_shard{shard_number}_queue"
        ch.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)


class StatefulSharder(Worker):
    def __init__(self, hash_fn, tmp_queue_prefixes=[], *args, **kwargs):
        # Pending: read amount of shards from env
        self.shards_count = 1
        self.hash_fn
        self.tmp_queue_prefixes = tmp_queue_prefixes
        self.ongoing_requests = set()
        self.new(*args, **kwargs)

    def callback(self, ch, method, properties, body):
        'Callback given to a RabbitMQ queue to invoke for each message in the queue'
        msg = json.loads(body)
        # Pending: if msg is an EOF, remove request from self.ongoing_requests
        # if there's no queues for this request_id, create them.
        if msg['request_id'] not in self.ongoing_requests:
            # create queues and subscribe them to dst_exchange
            for queue_prefix in tmp_queue_prefixes:
                for shard_id in range(self.shards_count):
                    new_dst_queue = f"{queue_prefix}_shard{shard_id}_{msg['request_id']}_queue"
                    self.channel.queue_declare(queue=new_dst_queue)
                    # use same routing_key for queues from different groups of shards
                    routing_key = f"{self.self.routing_key}_shard{shard_id}_{msg['request_id']}_queue"
                    self.channel.queue_bind(new_dst_queue, self.dst_exchange, routing_key=routing_key)
        shard_id = self.hash_fn(msg) % self.shards_count
        routing_key = f"{self.routing_key}_shard{shard_id}_{msg['request_id']}_queue"
        ch.basic_publish(exchange=self.dst_exchange, routing_key=routing_key, body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

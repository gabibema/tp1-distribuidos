import json
from pika.exchange_type import ExchangeType
from lib.workers import Router
SHARD_COUNT = 1

def routing_fn(body):
    "Shard by title and route to request specific tmp queues"
    msg = json.loads(body)
    shard_ids = set(hash(author) % SHARD_COUNT for author in msg['authors']) 
    routing_keys = [f"authors_shard{shard_id}" for shard_id in shard_ids]
    return routing_keys

def main():
    # Pending: move variables to env.
    # Pending: update SHARD_COUNT variable to match the env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'book_queue'
    src_exchange = 'books_exchange'
    src_routing_key = '#'
    dst_exchange = 'authors_sharded_exchange'
    dst_routing_key = 'authors'
    worker = Router(routing_fn, rabbit_hostname, src_queue, src_exchange, src_routing_key, ExchangeType.topic, dst_exchange, dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()
import json
from pika.exchange_type import ExchangeType
from lib.workers import Router
SHARD_COUNT = 1

def routing_fn(body):
    "Shard by title and route to request specific tmp queues"
    msg = json.loads(body)
    shard_id = hash(msg['Title']) % SHARD_COUNT
    routing_key = f"90s_books_shard{shard_id}"
    return [routing_key]

def main():
    # Pending: move variables to env.
    # Pending: update SHARD_COUNT variable to match the env.
    SHARD_COUNT = 1
    rabbit_hostname = 'rabbitmq'
    src_queue = '90s_queue'
    src_exchange = 'books_exchange'
    src_routing_key = '#.1.9.9.*'
    dst_exchange = '90s_books_sharded_exchange'
    dst_routing_key = '90s_books'
    worker = Router(routing_fn, rabbit_hostname, src_queue, src_exchange, src_routing_key, ExchangeType.topic, dst_exchange, dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

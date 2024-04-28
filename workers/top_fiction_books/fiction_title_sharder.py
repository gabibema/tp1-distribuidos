import json
from pika.exchange_type import ExchangeType
from lib.workers import Router
SHARD_COUNT = 1

def routing_fn(body):
    "Shard by title and route to request specific tmp queues"
    msg = json.loads(body)
    shard_id = hash(msg['Title']) % SHARD_COUNT
    routing_key = f"{self.routing_key}_shard{shard_id}"
    return routing_key

def main():
    # Pending: move variables to env.
    # Pending: update SHARD_COUNT variable to match the env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'fiction_queue'
    src_exchange = 'books_exchange'
    src_routing_key = '#.fiction.#'
    dst_exchange = 'fiction_books_sharded_exchange'
    dst_routing_key = 'fiction_books'
    worker = Router(routing_fn, rabbit_hostname, src_queue, src_exchange, src_routing_key, ExchangeType.topic, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

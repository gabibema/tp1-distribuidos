import json
import logging
from pika.exchange_type import ExchangeType
from lib.broker import MessageBroker
from lib.workers import Router
SHARD_COUNT = 1

def routing_fn(body):
    "Shard by title and route to request specific tmp queues"
    msg = json.loads(body)
    if msg.get('type') == 'EOF':
        return [f"authors_shard{shard_id}" for shard_id in range(SHARD_COUNT)]
    else:
        shard_ids = set(hash(author) % SHARD_COUNT for author in msg['authors']) 
        return [f"authors_shard{shard_id}" for shard_id in shard_ids]

def main():
    # Pending: move variables to env.
    # Pending: update SHARD_COUNT variable to match the env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'authors_book_queue'
    src_exchange = 'books_exchange'
    dst_exchange = 'authors_sharded_exchange'
    dst_routing_key = 'authors'
    control_queue_prefix = 'ctrl_author_sharder'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = MessageBroker(rabbit_hostname)
    worker = Router(routing_fn, control_queue_prefix, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_exchange_type=ExchangeType.fanout, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

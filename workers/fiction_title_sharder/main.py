import json
import logging
from lib.broker import MessageBroker
from lib.workers import Router
SHARD_COUNT = 1

def routing_fn(body):
    "Shard by title and route to request specific tmp queues"
    msg = json.loads(body)
    if msg.get('type') == 'EOF':
        return [f"fiction_books_shard{shard_id}" for shard_id in range(SHARD_COUNT)]
    else:
        shard_id = hash(msg['Title']) % SHARD_COUNT
        return [f"fiction_books_shard{shard_id}"]

def main():
    # Pending: move variables to env.
    # Pending: update SHARD_COUNT variable to match the env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'fiction_filtered_queue'
    src_exchange = 'fiction_filtered_exchange'
    src_routing_key = 'fiction_filtered_queue'
    dst_exchange = 'fiction_books_sharded_exchange'
    dst_routing_key = 'fiction_books'
    control_queue_prefix = 'ctrl_fiction_title_sharder'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = MessageBroker(rabbit_hostname)
    worker = Router(routing_fn, control_queue_prefix, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_routing_key=src_routing_key, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

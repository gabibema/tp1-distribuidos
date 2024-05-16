import json
from pika.exchange_type import ExchangeType
from lib.broker import RabbitMQConnection
from lib.workers import Router
SHARD_COUNT = 1

def routing_fn(body):
    "Shard by title and route to request specific tmp queues"
    msg = json.loads(body)
    if msg.get('type') == 'EOF':
        return [f"90s_books_shard{shard_id}" for shard_id in range(SHARD_COUNT)]
    else:
        shard_id = hash(msg['Title']) % SHARD_COUNT
        return [f"90s_books_shard{shard_id}"]

def main():
    # Pending: move variables to env.
    # Pending: update SHARD_COUNT variable to match the env.
    SHARD_COUNT = 1
    rabbit_hostname = 'rabbitmq'
    src_queue = '90s_queue'
    src_exchange = 'books_exchange'
    src_routing_key = '1990'
    dst_exchange = '90s_books_sharded_exchange'
    dst_routing_key = '90s_books'
    connection = RabbitMQConnection(rabbit_hostname)
    control_queue_prefix = 'ctrl_90s_title_sharder'
    worker = Router(routing_fn, control_queue_prefix, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_routing_key=src_routing_key, src_exchange_type=ExchangeType.topic, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

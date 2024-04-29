import json
from lib.workers import DynamicRouter
SHARD_COUNT = 1

def routing_fn(body):
    "Shard by title and route to request specific tmp queues"
    msg = json.loads(body)
    shard_id = hash(msg['Title']) % SHARD_COUNT
    routing_key = f"reviews_shard{shard_id}_{msg['request_id']}"
    return [routing_key]

def main():
    # Pending: move variables to env.
    # Pending: update SHARD_COUNT variable to match the env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'reviews_queue'
    dst_exchange = 'reviews_sharded_exchange'
    tmp_queues = [('fiction_reviews', 'reviews'),('90s_reviews','reviews')]
    worker = DynamicRouter(routing_fn, tmp_queues, rabbit_hostname, src_queue, dst_exchange=dst_exchange)
    worker.start()

if __name__ == '__main__':
    main()

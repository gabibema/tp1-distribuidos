from lib.broker import WorkerBroker
from lib.workers import DynamicRouter
SHARD_COUNT = 1

def routing_fn(msg):
    "Shard by title and route to request specific tmp queues"
    if msg.get('type') == 'EOF':
        return [f"reviews_shard{shard_id}_{msg['request_id']}" for shard_id in range(SHARD_COUNT)]
    else:
        shard_id = hash(msg['Title']) % SHARD_COUNT
        return [f"reviews_shard{shard_id}_{msg['request_id']}"]

def main():
    # Pending: move variables to env.
    # Pending: update SHARD_COUNT variable to match the env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'reviews_queue'
    dst_exchange = 'reviews_sharded_exchange'
    fiction_tmp_queues = [(f'fiction_reviews_shard{shard_id}', f'reviews_shard{shard_id}') for shard_id in range(SHARD_COUNT)]
    nineties_tmp_queues = [(f'90s_reviews_shard{shard_id}', f'reviews_shard{shard_id}') for shard_id in range(SHARD_COUNT)]
    tmp_queues = fiction_tmp_queues + nineties_tmp_queues
    connection = WorkerBroker(rabbit_hostname)
    control_queue_prefix = 'ctrl_title_sharder'
    worker = DynamicRouter(routing_fn, control_queue_prefix, tmp_queues=tmp_queues, connection=connection, src_queue=src_queue, dst_exchange=dst_exchange)
    worker.start()

if __name__ == '__main__':
    main()

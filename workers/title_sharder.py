import json
from lib.workers import StatefulSharder

def hash_fn(msg):
    # shard by title
    return hash(msg['Title'])

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_exchange = 'reviews_exchange'
    dst_exchange = 'reviews_sharded_exchange'
    tmp_queue_prefixes = ['fiction_reviews','90s_reviews']
    worker = StatefulSharder(hash_fn, tmp_queue_prefixes, rabbit_hostname, src_exchange=src_exchange, dst_exchange=dst_exchange, dst_routing_key='fiction_top_90_percentile')
    worker.start()

if __name__ == '__main__':
    main()

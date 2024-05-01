import json
from lib.workers import Aggregate

def aggregate(msg, accumulator):
    accumulator[msg['request_id']] = accumulator.get(msg['request_id'], [])
    accumulator[msg['request_id']].append(msg['Title'])

def result(msg, accumulator):
    titles = accumulator.get(msg['request_id'], [])
    del accumulator[msg['request_id']]
    return [json.dumps({'request_id': msg['request_id'], 'titles': titles})]

def main():
    """
    Receives the books in the category fiction for this specific shard and waits until the last one arrives, acting as a barrier.
    Once all fiction books for a request_id have arrived, sends them all in a single message to the next exchange.
    """
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_routing_key = f'fiction_books_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = 'fiction_books_sharded_exchange'
    dst_exchange = 'fiction_titles_barrier_exchange'
    dst_routing_key = f'fiction_titles_shard{shard_id}'
    accumulator = []
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, src_routing_key, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

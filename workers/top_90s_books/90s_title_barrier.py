import json
from lib.runner import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
    accumulator[msg['request_id']] = accumulator.get(msg['request_id'], [])
    accumulator[msg['request_id']].append(msg['Title'])

def result(accumulator):
    # Pending: When adding concurrent requests this should only return the result for the corresponding request_id
    return [v for k,v in accumulator.items()][0]

def main():
    """
    Receives the books published in the 90s for this specific shard and waits until the last one arrives, acting as a barrier.
    Once all 90s books for a request_id have arrived, sends them all in a single message to the next exchange.
    """
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_queue = ''
    src_exchange = 'books_exchange'
    src_routing_key = '#.1.9.9.*'
    dst_exchange = '90s_titles_barrier_exchange'
    dst_routing_key = f'90s_titles_shard{shard_id}'
    accumulator = []
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, src_routing_key, ExchangeType.topic, dst_exchange, dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

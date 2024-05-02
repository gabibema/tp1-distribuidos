import json
from lib.workers import Aggregate

def aggregate(msg, accumulator):
    accumulator[msg['request_id']] = accumulator.get(msg['request_id'], {})
    accumulator[msg['request_id']][msg['Title']] = accumulator[msg['request_id']].get(msg['Title'], 0) + 1

def result(msg, accumulator):
    acc = accumulator.pop(msg['request_id'], {})
    popular_books = [{'Title': title, 'count': values.count} for title, count in acc.items() if count >= 500]
    return json.dumps({'request_id': msg['request_id'], 'popular_books': popular_books })

def main():
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_queue = f'90s_rev_shard{shard_id}_queue'
    dst_exchange = 'popular_90s_exchange'
    dst_routing_key = 'popular_90s_queue'
    accumulator = {}
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

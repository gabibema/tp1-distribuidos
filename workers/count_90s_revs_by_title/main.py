import json
from lib.workers import Aggregate

def aggregate(msg, accumulator):
    accumulator[msg['request_id']] = accumulator.get(msg['request_id'], {})
    accumulator[msg['request_id']][msg['Title']] = accumulator[msg['request_id']].get(msg['Title'], 0) + 1

def result(msg, accumulator):
    acc = accumulator.pop(msg['request_id'], {})
    return [json.dumps({'request_id': msg['request_id'], 'Title': title, 'count': values.count}) for title, count in acc.items() if count >= 500]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = '90s_rev_queue'
    src_exchange = '90s_rev_exchange'
    dst_exchange = 'popular_90s_exchange'
    dst_routing_key = 'popular_90s_queue'
    accumulator = {}
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

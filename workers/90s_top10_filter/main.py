import json
from heapq import nlargest
from lib.workers import Aggregate

def aggregate(msg, accumulator):
    accumulator[msg['request_id']] = accumulator.get(msg['request_id'], [])
    accumulator[msg['request_id']].append((msg['ratingsCount'],msg))

def result(msg, accumulator):
    request_titles = accumulator.get(msg['request_id'], [])
    del accumulator[msg['request_id']]
    return [json.dumps({'request_id': msg['request_id'], 'top10': [msg[1] for msg in nlargest(10, request_titles)]})]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'top_90s_queue'
    dst_routing_key = 'top_90s_books'
    accumulator = {}
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

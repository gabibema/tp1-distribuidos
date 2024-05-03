import json
from heapq import nlargest
from lib.workers import Aggregate

def aggregate(msg, accumulator):
    if type(msg) != list:
        msg = [msg]
    accumulator[msg[0]['request_id']] = accumulator.get(msg[0]['request_id'], [])
    accumulator[msg[0]['request_id']].extend(msg)

def result(msg, accumulator):
    request_titles = accumulator.pop(msg['request_id'], [])
    return json.dumps({'request_id': msg['request_id'], 'top10': [book for book in nlargest(10, request_titles, key=lambda title: title['count'])]})

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'popular_90s_queue'
    src_routing_key = 'popular_90s_queue'
    src_exchange='popular_90s_exchange'
    dst_routing_key = 'top_90s_books'
    accumulator = {}
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, src_routing_key, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

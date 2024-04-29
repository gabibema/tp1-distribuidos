import json
from heapq import nlargest
from lib.runner import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
    accumulator.append((msg['ratingsCount'],msg))

def result(accumulator):
    return [msg[1] for msg in nlargest(10, accumulator)]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'top_90s_queue'
    src_exchange = '90s_rev_exchange'
    dst_exchange = 'output_exchange'
    dst_routing_key = 'top_90s_books'
    accumulator = []
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

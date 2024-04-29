import json
from heapq import nlargest
from lib.workers import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
    accumulator.append((msg['ratingsCount'],msg))

def result(accumulator):
    return [msg[1] for msg in nlargest(10, accumulator)]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'top_90s_queue'
    dst_routing_key = 'top_90s_books'
    accumulator = []
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

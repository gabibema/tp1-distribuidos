import json
from lib.workers import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
    accumulator[msg['Title']] = accumulator.get(msg['Title'], 0) + 1

def result(accumulator):
    return [json.dumps({'Title': title, 'count': values.count}) for title, count in accumulator.items() if count >= 500]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = '90s_rev_queue'
    src_exchange = '90s_rev_exchange'
    dst_routing_key = 'top_90s_queue'
    accumulator = {}
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

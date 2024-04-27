import json
from lib.workers import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
    accumulator[msg['Title']] = accumulator.get(msg['Title'], 0) + 1

def result(accumulator):
    return [json.dumps({'Title': title, 'count': values.count}) for title, count in accumulator.items() if count >= 500]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'localhost'
    src_queue = '90s_rev_q'
    src_exchange = '90s_rev_exch'
    dst_exchange = 'top_90s_exch'
    accumulator = {}
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, dst_exchange=dst_exchange)
    worker.start()

if __name__ == '__main__':
    main()

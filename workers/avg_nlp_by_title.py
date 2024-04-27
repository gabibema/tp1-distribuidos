import json
from pika.exchange_type import ExchangeType
from lib.workers import Aggregate

AvgAccumulator = namedtuple('AvgAccumulator', ['sum', 'count'])

def aggregate(message, accumulator):
    msg = json.loads(message)
    old_values = accumulator.get(msg['Title'], AvgAccumulator(sum=0, count=0))
    new_values = AvgAccumulator(sum=old_values.sum + msg['sentiment'], count=old_values.count + 1)
    accumulator[msg['Title']] = new_values

def result(accumulator):
    return [json.dumps({'Title': title, 'average': values.sum/values.count}) for title, values in accumulator.items()]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'localhost'
    src_queue = 'nlp_revs_q'
    src_exchange = 'nlp_revs_exch'
    dst_exchange = 'avg_nlp_exch'
    accumulator = {}
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, dst_exchange=dst_exchange, dst_exchange_type=ExchangeType.fanout)
    worker.start()

if __name__ == '__main__':
    main()

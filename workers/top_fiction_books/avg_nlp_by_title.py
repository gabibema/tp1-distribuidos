import json
from pika.exchange_type import ExchangeType
from lib.workers import DynamicAggregate

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
    rabbit_hostname = 'rabbitmq'
    src_routing_key = f'nlp_revs_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = 'nlp_revs_exchange'
    dst_exchange = 'avg_nlp_exchange'
    accumulator = {}
    worker = DynamicAggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, dst_exchange=dst_exchange, dst_routing_key='avg_nlp', dst_exchange_type=ExchangeType.topic)
    worker.start()

if __name__ == '__main__':
    main()

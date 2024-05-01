import json
from collections import namedtuple
from pika.exchange_type import ExchangeType
from lib.workers import DynamicAggregate

AvgAccumulator = namedtuple('AvgAccumulator', ['sum', 'count'])

def aggregate(msg, accumulator):
    accumulator[msg['request_id']] = accumulator.get(msg['request_id'], {})
    old_values = accumulator[msg['request_id']].get(msg['Title'], AvgAccumulator(sum=0, count=0))
    new_values = AvgAccumulator(sum=old_values.sum + msg['sentiment'], count=old_values.count + 1)
    accumulator[msg['request_id']][msg['Title']] = new_values

def result(msg, accumulator):
    acc = accumulator.get(msg['request_id'], {})
    del accumulator[msg['request_id']]
    return [json.dumps({'request_id': msg['request_id'], 'Title': title, 'average': values.sum/values.count}) for title, values in acc.items()]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    shard_id = 0
    src_routing_key = f'nlp_revs_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = 'nlp_revs_exchange'
    dst_exchange = 'avg_nlp_exchange'
    tmp_queues = [('avg_nlp','')]
    accumulator = {}
    worker = DynamicAggregate(aggregate, result, accumulator, tmp_queues, rabbit_hostname, src_queue, src_exchange, dst_exchange=dst_exchange, dst_routing_key='avg_nlp', dst_exchange_type=ExchangeType.topic)
    worker.start()

if __name__ == '__main__':
    main()

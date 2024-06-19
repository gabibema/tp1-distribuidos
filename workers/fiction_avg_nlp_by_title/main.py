import logging
from collections import namedtuple
from pika.exchange_type import ExchangeType
from lib.broker import MessageBroker
from lib.workers import DynamicAggregate

AvgAccumulator = namedtuple('AvgAccumulator', ['sum', 'count'])
BATCH_SIZE = 100

def aggregate(message, accumulator):
    accumulator[message['request_id']] = accumulator.get(message['request_id'], {})
    for item in message['items']:
        old_values = accumulator[message['request_id']].get(item['Title'], AvgAccumulator(sum=0, count=0))
        new_values = AvgAccumulator(sum=old_values.sum + item['score'], count=old_values.count + 1)
        accumulator[message['request_id']][item['Title']] = new_values

def result(msg, accumulator):
    acc = accumulator.pop(msg['request_id'], {})
    items = [{'Title': title, 'average': values.sum/values.count} for title, values in acc.items()]
    return [{'request_id': msg['request_id'], 'items': items[i:i+BATCH_SIZE]} for i in range(0, len(items), BATCH_SIZE)]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    shard_id = 0
    src_routing_key = f'nlp_revs_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = 'nlp_revs_exchange'
    dst_exchange = 'avg_nlp_exchange'
    tmp_queues = [('avg_nlp','avg_nlp')]
    accumulator = {}
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = MessageBroker(rabbit_hostname)
    worker = DynamicAggregate(aggregate, result, accumulator, tmp_queues=tmp_queues, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_routing_key=src_routing_key, dst_exchange=dst_exchange, dst_routing_key='avg_nlp', dst_exchange_type=ExchangeType.topic)
    worker.start()

if __name__ == '__main__':
    main()

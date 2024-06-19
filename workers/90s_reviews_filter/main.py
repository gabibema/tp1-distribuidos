import logging
from pika.exchange_type import ExchangeType
from lib.broker import MessageBroker
from lib.workers import DynamicFilter

def update_state(old_state, message):
    if message.get('type') == 'EOF':
        # delete info that was required to process the request, which has been fulfilled
        old_state.pop(message['request_id'], None)
    else:
        logging.warning(message['titles'][:10])
        old_state[message['request_id']] = set(message['titles'])
    return old_state

def filter_condition(state, msg):
    # if Book's average review NLP is greater than the 10th percentile from state
    return msg['Title'] in state[msg['request_id']]

def main():
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_routing_key = f'90s_titles_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = '90s_titles_barrier_exchange'
    dst_routing_key = f'90s_rev_shard{shard_id}_queue'
    tmp_queues_prefix = f'90s_reviews_shard{shard_id}'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = MessageBroker(rabbit_hostname)
    worker = DynamicFilter(update_state, filter_condition, tmp_queues_prefix, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_routing_key=src_routing_key, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

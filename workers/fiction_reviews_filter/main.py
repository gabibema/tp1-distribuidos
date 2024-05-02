import logging
from pika.exchange_type import ExchangeType
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
    # if review is in the list of fiction titles
    return msg['Title'] in state[msg['request_id']]

def main():
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_routing_key = f'fiction_titles_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = 'fiction_titles_barrier_exchange'
    dst_routing_key = f'fiction_rev_shard{shard_id}_queue'
    tmp_queues_prefix = f'fiction_reviews_shard{shard_id}'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    worker = DynamicFilter(update_state, filter_condition, tmp_queues_prefix, rabbit_hostname, src_queue, src_exchange, src_routing_key, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

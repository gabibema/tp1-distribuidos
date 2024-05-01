import json
from pika.exchange_type import ExchangeType
from lib.workers import DynamicFilter

def update_state(old_state, message):
    if message.get('type') == 'EOF':
        # delete info that was required to process the request, which has been fulfilled
        old_state.pop(message['request_id'], None)
    else:
        old_state[message['request_id']] = set(message['titles'])
    return old_state

def filter_condition(state, body):
    msg = json.loads(body)
    # if Book's average review NLP is greater than the 10th percentile from state
    return msg['Title'] in state[msg['request_id']]

def main():
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_routing_key = f'90s_titles_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = '90s_titles_barrier_exchange'
    dst_exchange = '90s_rev_exchange'
    tmp_queues_prefix = f'90s_reviews_shard{shard_id}'
    worker = DynamicFilter(update_state, filter_condition, tmp_queues_prefix, rabbit_hostname, src_queue, src_exchange, src_routing_key, ExchangeType.direct, dst_exchange, '90s_rev_queue')
    worker.start()

if __name__ == '__main__':
    main()

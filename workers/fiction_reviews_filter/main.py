import json
from lib.workers import StatefulFilter

def update_state(old_state, message):
    if message['type'] == 'new_request':
        old_state[message['request_id']] = set(message['titles'])
    elif message['type'] == 'EOF':
        # delete info that was required to process the request, which has been fulfilled
        del old_state[message['request_id']]
    return old_state

def filter_condition(state, body):
    msg = json.loads(body)
    # if Book's average review NLP is greater than the 10th percentile from state
    return msg['Title'] in state[msg['request_id']]

def main():
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_routing_key = f'fiction_titles_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = 'fiction_titles_barrier_exchange'
    dst_exchange = 'fiction_rev_exchange'
    dst_routing_key = f'fiction_rev_shard{shard_id}'
    tmp_queues_prefix = f'fiction_reviews_shard{shard_id}'
    worker = StatefulFilter(update_state, filter_condition, tmp_queues_prefix, rabbit_hostname, src_queue, src_exchange, src_routing_key, ExchangeType.direct, dst_exchange, dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

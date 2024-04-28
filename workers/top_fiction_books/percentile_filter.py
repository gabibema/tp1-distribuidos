import json
import random
from pika.exchange_type import ExchangeType
from lib.workers import StatefulFilter

def update_state(old_state, message):
    if message['type'] == 'new_request':
        old_state[message['request_id']] = message['percentile']
    elif message['type'] == 'EOF':
        # delete info that was required to process the request, which has been fulfilled
        del old_state[message['request_id']]
    return old_state

def filter_condition(state, body):
    msg = json.loads(body)
    # if Book's average review NLP is greater than the 10th percentile from state
    return msg['average'] >= state[msg['request_id']]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_exchange = 'nlp_percentile_exchange'
    dst_exchange = 'output_exchange'
    tmp_queues_prefix = 'avg_nlp'
    worker = StatefulFilter(update_state, filter_condition, tmp_queues_prefix, rabbit_hostname, src_exchange=src_exchange, dst_exchange=dst_exchange, dst_routing_key='top_fiction_books')
    worker.start()

if __name__ == '__main__':
    main()

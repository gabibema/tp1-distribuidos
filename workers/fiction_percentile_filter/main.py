import logging
from pika.exchange_type import ExchangeType
from lib.broker import MessageBroker
from lib.workers import DynamicFilter

def update_state(old_state, message):
    if message.get('type') == 'EOF':
        # delete info that was required to process the request, which has been fulfilled
        old_state.pop(message['request_id'], None)
    else:
        old_state[message['request_id']] = message['percentile']
    return old_state

def filter_condition(state, msg):
    # if Book's average review NLP is greater than the 10th percentile from state
    return msg['average'] >= state[msg['request_id']]

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_exchange = 'nlp_percentile_exchange'
    src_queue = 'nlp_percentile'
    src_routing_key = src_queue
    dst_routing_key = 'top_fiction_books'
    tmp_queues_prefix = 'avg_nlp'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = MessageBroker(rabbit_hostname)
    worker = DynamicFilter(update_state, filter_condition, tmp_queues_prefix, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_exchange_type=ExchangeType.fanout, src_routing_key=src_routing_key, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

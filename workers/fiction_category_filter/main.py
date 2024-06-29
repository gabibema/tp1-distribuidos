import json
import logging
from pika.exchange_type import ExchangeType
from lib.broker import MessageBroker
from lib.workers import Filter

def category_filter(body):
    msg = json.loads(body)
    for word in msg['categories'].split(' '):
        if 'fiction' == word.strip('\'"[],'):
            return True
    else:
        return False

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'fiction_unfiltered_queue'
    src_exchange = 'books_exchange'
    dst_exchange = 'fiction_filtered_exchange'
    dst_routing_key = 'fiction_filtered_queue'
    control_queue_prefix = 'ctrl_fiction_category_filter'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = MessageBroker(rabbit_hostname)
    worker = Filter(category_filter, control_queue_prefix, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_exchange_type=ExchangeType.fanout, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

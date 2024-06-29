import json
import logging
from pika.exchange_type import ExchangeType
from lib.broker import MessageBroker
from lib.workers import Filter

def title_filter(body):
    msg = json.loads(body)
    date = msg['publishedDate']
    if not date:
        return False
    try:
        year = int(date.split('-', maxsplit=1)[0])
        return 2000 <= year <= 2023 and 'Computers' in msg['categories'] and 'distributed' in msg['Title']
    except:
        return False

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'computers_queue'
    src_exchange = 'books_exchange'
    dst_routing_key = 'computer_books'
    control_queue_prefix = 'ctrl_computer_books_filter'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = MessageBroker(rabbit_hostname)
    worker = Filter(title_filter, control_queue_prefix, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_exchange_type=ExchangeType.fanout, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

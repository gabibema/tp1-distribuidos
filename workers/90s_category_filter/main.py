import json
from pika.exchange_type import ExchangeType
from lib.broker import MessageBroker
from lib.workers import Filter

def category_filter(body):
    date_str = json.loads(body)['publishedDate']
    try:
        year = int(date_str.split('-', maxsplit=1)[0])
        decade = year - year % 10
        return decade == 1990
    except ValueError:
        return False

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = '90s_unfiltered_queue'
    src_exchange = 'books_exchange'
    dst_exchange = '90s_filtered_exchange'
    dst_routing_key = '90s_filtered_queue'
    connection = MessageBroker(rabbit_hostname)
    control_queue_prefix = 'ctrl_90s_category_filter'
    worker = Filter(category_filter, control_queue_prefix, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_exchange_type=ExchangeType.fanout, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

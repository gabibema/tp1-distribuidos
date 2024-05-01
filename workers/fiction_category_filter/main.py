import json
from pika.exchange_type import ExchangeType
from lib.workers import Filter

def category_filter(body):
    msg = json.loads(body)
    for category in msg['categories']:
        if 'fiction' in category.split(' '):
            return True
    else:
        return False

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'fiction_unfiltered_queue'
    src_exchange = 'books_exchange'
    src_routing_key = '#'
    dst_exchange = 'fiction_filtered_exchange'
    dst_routing_key = 'fiction_filtered_queue'
    worker = Filter(category_filter, rabbit_hostname, src_queue, src_exchange, src_routing_key, ExchangeType.topic, dst_exchange, dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

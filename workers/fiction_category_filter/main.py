import json
from pika.exchange_type import ExchangeType
from lib.broker import WorkerBroker
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
    src_routing_key = '#'
    dst_exchange = 'fiction_filtered_exchange'
    dst_routing_key = 'fiction_filtered_queue'
    connection = WorkerBroker(rabbit_hostname)
    control_queue_prefix = 'ctrl_fiction_category_filter'
    worker = Filter(category_filter, control_queue_prefix, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_routing_key=src_routing_key, src_exchange_type=ExchangeType.topic, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

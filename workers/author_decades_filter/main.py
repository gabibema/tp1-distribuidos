import json
import logging
from lib.broker import RabbitMQConnection
from lib.workers import Aggregate

def aggregate(msg, accumulator):
    date = msg['publishedDate']
    if not date:
        # skip books with invalid date
        return
    try:
        year = int(date.split('-', maxsplit=1)[0])
        decade = year - year % 10
    except ValueError:
        logging.info(f'Skipping book with unsupported date format: "{date}"')
        return
    # ignore brackets
    authors = msg['authors'][1:-1]
    for author in authors.split(','):
        accumulator[msg['request_id']] = accumulator.get(msg['request_id'], {})
        accumulator[msg['request_id']][author] = accumulator[msg['request_id']].get(author, set())
        accumulator[msg['request_id']][author].add(decade)

def result(msg, accumulator):
    authors = [author for author, decades in accumulator.pop(msg['request_id'], {}).items() if len(decades) >= 10]
    return json.dumps({'request_id': msg['request_id'], 'authors': authors})

def main():
    # Pending: move variables to env.
    shard_id = 0
    accumulator = {}
    rabbit_hostname = 'rabbitmq'
    src_routing_key = f'authors_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = 'authors_sharded_exchange'
    dst_routing_key = 'author_decades'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = RabbitMQConnection(rabbit_hostname)
    worker = Aggregate(aggregate, result, accumulator, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_routing_key=src_routing_key, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

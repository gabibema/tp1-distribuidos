import json
import logging
from lib.workers import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
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
        accumulator[author] = accumulator.get(author, set())
        accumulator[author].add(decade)

def result(accumulator):
    return [author for author, decades in accumulator.items() if len(decades) >= 10]

def main():
    # Pending: move variables to env.
    shard_id = 0
    accumulator = {}
    rabbit_hostname = 'rabbitmq'
    src_routing_key = f'authors_shard{shard_id}'
    src_queue = src_routing_key + '_queue'
    src_exchange = 'authors_sharded_exchange'
    dst_exchange = 'output_exchange'
    dst_routing_key = 'author_decades'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, src_routing_key, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

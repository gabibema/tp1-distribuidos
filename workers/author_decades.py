import json
from pika.exchange_type import ExchangeType
from lib.workers import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
    date = msg['publishedDate']
    if not date:
        # skip books with invalid date
        return
    year = int(date.split('-', maxsplit=1)[0])
    decade = year - year % 10
    # ignore brackets
    authors = msg['authors'][1:-1]
    for author in authors.split(','):
        accumulator[author] = accumulator.get(author, set())
        accumulator[author].add(decade)

def result(accumulator):
    return [author for author, decades in accumulator.items() if len(decades) >= 10]

def main():
    # Pending: move variables to env.
    accumulator = []
    rabbit_hostname = 'rabbitmq'
    src_queue = 'book_queue'
    src_exchange = 'books_exchange'
    src_routing_key = '#'
    dst_exchange = 'output_exchange'
    dst_routing_key = 'author_decades'
    worker = Aggregate(aggregate, result, accumulator, rabbit_hostname, src_queue, src_exchange, src_routing_key, ExchangeType.topic, dst_exchange, dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

import json
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
    rabbit_hostname = 'localhost'
    src_queue = 'nlp_title_q'
    dst_queue = 'avg_nlp_q'
    accumulator = []
    worker = Aggregate(rabbit_hostname, src_queue, dst_queue, aggregate, result, accumulator)
    worker.start()

if __name__ == '__main__':
    main()

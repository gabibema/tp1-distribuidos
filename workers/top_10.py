import json
from heapq import nlargest
from lib.runner import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
    accumulator.append((msg['ratingsCount'],msg))

def result(accumulator):
    return [msg[1] for msg in nlargest(10, accumulator)]

def main():
    rabbit_hostname = 'localhost'
    src_queue = 'nlp_title_q'
    dst_queue = 'avg_nlp_q'
    accumulator = []
    runner = Aggregate(rabbit_hostname, src_queue, dst_queue, aggregate, result, accumulator)
    runner.start()

if __name__ == '__main__':
    main()

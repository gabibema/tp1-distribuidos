import json
from lib.runner import Aggregate

AvgAccumulator = namedtuple('AvgAccumulator', ['sum', 'count'])

def aggregate(message, accumulator):
    msg = json.loads(message)
    old_values = accumulator.get(msg['Title'], AvgAccumulator(sum=0, count=0))
    new_values = AvgAccumulator(sum=old_values.sum + msg['sentiment'], count=old_values.count + 1)
    accumulator[msg['Title']] = new_values

def result(accumulator):
    return [json.dumps({'Title': title, 'average': values.sum/values.count}) for title, values in accumulator.items()]

def main():
    rabbit_hostname = 'localhost'
    src_queue = 'nlp_title_q'
    dst_queue = 'avg_nlp_q'
    accumulator = {}
    runner = Aggregate(rabbit_hostname, src_queue, dst_queue, aggregate, result, accumulator)
    runner.start()

if __name__ == '__main__':
    main()

import json
from lib.runner import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
    accumulator[msg['Title']] = accumulator.get(msg['Title'], 0) + 1

def result(accumulator):
    return [json.dumps({'Title': title, 'count': values.sum/values.count}) for title, values in accumulator.items()]

def main():
    rabbit_hostname = 'localhost'
    src_queue = '90s_rev_q'
    dst_queue = 'top_90s_q'
    accumulator = {}
    runner = Aggregate(rabbit_hostname, src_queue, dst_queue, aggregate, result, accumulator)
    runner.start()

if __name__ == '__main__':
    main()

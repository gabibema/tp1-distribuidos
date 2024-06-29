import json
import random
import logging
from pika.exchange_type import ExchangeType
from lib.broker import MessageBroker
from lib.workers import Aggregate

def aggregate(msg, accumulator):
    accumulator[msg['request_id']] = accumulator.get(msg['request_id'], [])
    accumulator[msg['request_id']].append(msg['average'])

def result(msg, accumulator):
    acc = accumulator.pop(msg['request_id'], [])
    percentile_10_idx = len(acc) // 10
    if len(acc) <= 0:
        logging.warning(f"Received EOF for {msg['request_id']} without any books.")
        return json.dumps([{'request_id': msg['request_id'], 'percentile': float('inf')}])
    if percentile_10_idx == 0:
        logging.warning(f"Received EOF for {msg['request_id']} with less than 10 books.")
        return json.dumps([{'request_id': msg['request_id'], 'percentile': float('-inf')}])
    percentile = kth_smallest(percentile_10_idx, acc, 0, len(acc) - 1)
    logging.warning(f'The 10th percentile is {percentile}.')
    return json.dumps([{'request_id': msg['request_id'], 'percentile': percentile}])

def kth_smallest(k, buffer, start, end):
    "Find the Kth smallest value in O(n) time"
    i, j = start, end
    logging.info(f'k: {k}, start: {start}, end: {end}.')
    pivot = choose_pivot(buffer, i, j)
    pivot_duplicates = 0
    # Create (in-place) 2 sub buffers. One with elements lesser than the pivot and another one with elements greater than the pivot.
    while i + pivot_duplicates <= j:
        v = buffer[i + pivot_duplicates]
        if v < pivot:
            buffer[i] = v
            i += 1
        elif v > pivot:
            swap(buffer, i + pivot_duplicates, j)
            j -= 1
        else:
            pivot_duplicates += 1
    # Determine which sub buffer contains the Kth element and run recursively
    lesser_buffer_size = i - start
    greater_buffer_size = end - j
    logging.debug(f'i: {i}, j: {j}, pivot_duplicates: {pivot_duplicates}.')
    if k <= lesser_buffer_size:
        return kth_smallest(k, buffer, start, i - 1)
    len_buffer = end - start + 1
    if len_buffer - k < greater_buffer_size:
        return kth_smallest(k - (j + 1 - start), buffer, j + 1, end)
    return pivot

def choose_pivot(buffer, i, j):
    "Find 3 random posible pivots, and choose the one with the intermediate value"
    pivot_1 = buffer[random.randint(i,j)]
    pivot_2 = buffer[random.randint(i,j)]
    pivot_3 = buffer[random.randint(i,j)]
    if pivot_1 <= pivot_2 <= pivot_3:
        return pivot_2
    if pivot_1 <= pivot_3 <= pivot_2:
        return pivot_3
    return pivot_1

def swap(buffer, i, j):
    "Take a buffer and 2 indices and swap the values stored in those indices"
    tmp = buffer[i]
    buffer[i] = buffer[j]
    buffer[j] = tmp

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'avg_nlp_queue'
    src_exchange = 'avg_nlp_exchange'
    src_routing_key = '#'
    dst_exchange = 'nlp_percentile_exchange'
    dst_routing_key = 'nlp_percentile_queue'
    accumulator = {}
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = MessageBroker(rabbit_hostname)
    worker = Aggregate(aggregate, result, accumulator, connection=connection, src_queue=src_queue, src_exchange=src_exchange, src_routing_key=src_routing_key, src_exchange_type=ExchangeType.topic, dst_exchange=dst_exchange, dst_exchange_type=ExchangeType.fanout, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

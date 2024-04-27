import json
import random
from lib.runner import Aggregate

def aggregate(message, accumulator):
    msg = json.loads(message)
    accumulator.append(msg['average'])

def result(accumulator):
    percentile_10_idx = len(accumulator) / 10
    return kth_smallest(percentile_10_idx, accumulator, 0, len(accumulator) - 1)

def kth_smallest(k, buffer, start, end):
    "Find the Kth smallest value in O(n) time"
    i, j = start, end
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
    if k <= lesser_buffer_size:
        return kth_smallest(k, buffer, start, i - 1)
    if len(buffer) - k < greater_buffer_size:
        return kth_smallest(k, buffer, j + 1, end)
    return pivot

def choose_pivot(buffer, i, j):
    "Find 3 random posible pivots, and choose the one with the intermediate value"
    pivot_1 = buffer[random.randrange(0,j+1)]
    pivot_2 = buffer[random.randrange(0,j+1)]
    pivot_3 = buffer[random.randrange(0,j+1)]
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
    rabbit_hostname = 'localhost'
    src_queue = 'nlp_title_q'
    dst_queue = 'avg_nlp_q'
    accumulator = []
    runner = Aggregate(rabbit_hostname, src_queue, dst_queue, aggregate, result, accumulator)
    runner.start()

if __name__ == '__main__':
    main()

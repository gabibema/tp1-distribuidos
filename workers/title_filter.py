from lib.workers import Filter

def title_filter(message):
    msg = json.loads(message)
    date = msg['publishedDate']
    if not date:
        return False
    year = int(date.split('-', maxsplit=1)[0])
    return 2000 <= year <= 2023 and 'distributed' in msg['Title']

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'computers_q'
    src_exchange = 'books_exchange'
    src_routing_key = '#.computers.#'
    dst_exchange = 'output_exch'
    dst_routing_key = 'computer_books'
    worker = Filter(title_filter, rabbit_hostname, src_queue, src_exchange, src_routing_key, ExchangeType.topic, dst_exchange, dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

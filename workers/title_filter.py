from lib.workers import Filter

def title_filter(message):
    msg = json.loads(message)
    date = msg['publishedDate']
    if not date:
        return False
    year = int(date.split('-', maxsplit=1)[0])
    return 2000 <= year <= 2023 and 'distributed' in msg['Title']

def main():
    rabbit_hostname = 'localhost'
    src_queue = 'fiction_rev_q'
    dst_queue = 'nlp_title_q'
    worker = Filter(rabbit_hostname, src_queue, dst_queue, title_filter)
    worker.start()

if __name__ == '__main__':
    main()

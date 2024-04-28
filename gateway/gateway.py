
from threading import Thread
from lib.workers import MAX_KEY_LENGTH, Proxy, wait_rabbitmq

class Gateway:
    def __init__(self, config):
        self.books_queue = config['books_queue']
        self.books_exchange = config['books_exchange']

        self.reviews_queue = config['reviews_queue']
        self.reviews_exchange = config['reviews_exchange']

        self.books_gateway = Thread(target=self.__gateway_books)
        self.reviews_gateway = Thread(target=self.__gateway_reviews)

    def start(self):
        wait_rabbitmq()
        self.books_gateway.start()
        self.reviews_gateway.start()
    
    def __gateway_books(self):
        gateway = Proxy('rabbitmq', self.books_queue, self.books_exchange, keys_getter=get_books_keys)
        gateway.start()
    
        
    def __gateway_reviews(self):
        gateway = Proxy('rabbitmq', self.reviews_queue, self.reviews_exchange)
        gateway.start()

def get_books_keys(row):
    date_str = row['publishedDate']
    categories_str = row['categories']

    year = date_str.split('-', maxsplit=1)[0]
    year_dots = '.'.join(year)

    categories = categories_str.strip("[]")
    categories = categories.replace("'", "").replace(" & ", ".")
    categories_list = categories.split(", ")
    categories_dots = '.'.join(categories_list)
    if len(categories_dots) + len(year_dots) > MAX_KEY_LENGTH:
        categories_dots = categories_dots[:MAX_KEY_LENGTH - len(year_dots) - 1]

    return f"{categories_dots}.{year_dots}"
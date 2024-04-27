
from threading import Thread
from lib.workers import MAX_KEY_LENGTH, Proxy, wait_rabbitmq

class Gateway:
    def __init__(self, config):
        self.books_queue = config['books_queue']
        self.books_exchange = config['books_exchange']

        self.ratings_queue = config['ratings_queue']
        self.ratings_exchange = config['ratings_exchange']

        self.books_gateway = Thread(target=self.__gateway_books)
        self.ratings_gateway = Thread(target=self.__gateway_ratings)

    def start(self):
        wait_rabbitmq()
        self.books_gateway.start()
        self.ratings_gateway.start()
    
    def __gateway_books(self):
        gateway = Proxy('rabbitmq', self.books_queue, self.books_exchange, keys_getter=get_books_keys)
        gateway.start()
    
        
    def __gateway_ratings(self):
        gateway = Proxy('rabbitmq', self.ratings_queue, self.ratings_exchange)
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

    print(f"{categories_dots}.{year_dots}")
    return f"{categories_dots}.{year_dots}"
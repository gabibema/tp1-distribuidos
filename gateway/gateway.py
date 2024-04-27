
from threading import Thread
from lib.workers import Proxy, wait_rabbitmq


class Gateway:
    def __init__(self, config):
        self.books_queue = config['books_queue']
        self.books_exchange = config['books_exchange']
        self.books_keys = config['books_keys']

        self.ratings_queue = config['ratings_queue']
        self.ratings_exchange = config['ratings_exchange']
        self.ratings_keys = config['ratings_keys']

        self.books_gateway = Thread(target=self.__gateway_books)
        self.ratings_gateway = Thread(target=self.__gateway_ratings)

    def start(self):
        wait_rabbitmq()
        self.books_gateway.start()
        self.ratings_gateway.start()
    
    def __gateway_books(self):
        gateway = Proxy('rabbitmq', self.books_queue, self.books_exchange, keys=self.books_keys)
        gateway.start()
        
    def __gateway_ratings(self):
        gateway = Proxy('rabbitmq', self.ratings_queue, self.ratings_exchange, keys=self.ratings_keys)
        gateway.start()
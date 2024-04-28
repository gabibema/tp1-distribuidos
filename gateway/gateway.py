
from queue import Queue
from threading import Thread
from lib.workers.workers import Proxy
from lib.workers import wait_rabbitmq, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol
from socket import SOCK_STREAM, socket, AF_INET


class Gateway:
    def __init__(self, config):
        self.port = config['port']
        self.conn = None

        self.books_queue = Queue()
        self.reviews_queue = Queue()

        #self.gateway_books = Thread(target=self.__gateway_file(config['books_exchange'], get_books_keys, MESSAGE_FLAG['BOOK']))
        #self.gateway_reviews = Thread(target=self.__gateway_file(config['ratings_exchange'], None, MESSAGE_FLAG['RATING']))


    def start(self):
        self.__start_socket()
        #self.gateway_books.start()
        #self.gateway_reviews.start()


    def __start_socket(self):
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.conn.bind(('', self.port))

        while True:
            self.conn.listen(5)
            print("Waiting for connection...")
            client, addr = self.conn.accept()
            Thread(target=self.__handle_client, args=(client,)).start()


    def __handle_client(self, client):
        protocol = TransferProtocol(client)
        while True:
            message, flag = protocol.receive_message()
            print(f"Received message: {message}")
            #if flag == MESSAGE_FLAG['ERROR']:
            #    client.close()
            #elif flag == MESSAGE_FLAG['BOOK']:
            #    self.books_queue.put(message)
            #elif flag == MESSAGE_FLAG['RATING']:
            #    self.reviews_queue.put(message, flag)


    def __gateway_file(self, exchange, keys_getter=None, flag=None):
        wait_rabbitmq()
        proxy = Proxy('rabbitmq',exchange, keys_getter)
        proxy.start()

        if flag == MESSAGE_FLAG['BOOK']:
            queue = self.books_queue
        elif flag == MESSAGE_FLAG['RATING']:
            queue = self.reviews_queue

        while True:
            message = queue.get()
            proxy.publish(message)


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
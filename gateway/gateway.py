
from queue import Queue
from threading import Thread
from lib.workers.workers import Proxy
from lib.workers import wait_rabbitmq, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol
from socket import SOCK_STREAM, socket


class Gateway:
    def __init__(self, config):
        self.port = config['port']
        self.conn = None

        self.books_queue = config['books_queue']
        self.books_exchange = config['books_exchange']

        self.reviews_queue = config['reviews_queue']
        self.reviews_exchange = config['reviews_exchange']

        self.message_queue = Queue()
        self.gateway_files = Thread(target=self.__gateway_files)

    def start(self):
        self.__start_socket()
        self.gateway_files.start()


    def __start_socket(self):
        self.conn = socket(SOCK_STREAM)
        self.conn.bind(('', self.port))

        while True:
            client, addr = self.conn.accept()
            Thread(target=self.__handle_client, args=(client,)).start()


    def __handle_client(self, client):
        protocol = TransferProtocol(client)
        while True:
            message, flag = protocol.receive_message()
            if flag == MESSAGE_FLAG['ERROR']:
                client.close()
                break
            self.message_queue.put((message, flag))


    def __gateway_files(self):
        wait_rabbitmq()
        gateway_books = Proxy('rabbitmq', self.books_queue, self.books_exchange, keys_getter=get_books_keys)
        gateway_books.start()
        gateway_reviews = Proxy('rabbitmq', self.reviews_queue, self.reviews_exchange)
        gateway_reviews.start()

        eof = False
        while not eof:
            message, flag = self.message_queue.get()        
            if flag == MESSAGE_FLAG['EOF']:
                eof = True
            elif flag == MESSAGE_FLAG['BOOK']:
                gateway_books.publish(message)
            elif flag == MESSAGE_FLAG['REVIEW']:
                gateway_reviews.publish(message)


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
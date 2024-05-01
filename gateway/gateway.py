from threading import Thread
from socket import SOCK_STREAM, socket, AF_INET
from pika.exchange_type import ExchangeType
from lib.workers import Proxy, wait_rabbitmq, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol

CLIENTS_BACKLOG = 5


class Gateway:
    def __init__(self, config):
        self.port = config['port']
        self.conn = None

        self.books_exchange = config['books_exchange']
        self.ratings_exchange = config['ratings_exchange']

    def start(self):
        self.__start_socket()


    def __start_socket(self):
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.conn.bind(('', self.port))

        while True:
            self.conn.listen(CLIENTS_BACKLOG)
            client, addr = self.conn.accept()
            Thread(target=self.__handle_client, args=(client,)).start()


    def __handle_client(self, client):
        wait_rabbitmq()
        protocol = TransferProtocol(client)
        exchanges = {
            'books_exchange': get_books_keys,
            'reviews_exchange': None
        }
        proxy = Proxy('rabbitmq', exchanges)
        # pending: declare resources once when Gateway is created
        proxy.channel.exchange_declare('books_exchange', exchange_type=ExchangeType.topic)
        proxy.channel.exchange_declare('reviews_exchange', exchange_type=ExchangeType.direct)
        proxy.channel.exchange_declare('popular_90s_exchange', exchange_type=ExchangeType.direct)
        proxy.channel.queue_declare(queue='author_decades', durable=True)
        proxy.channel.queue_declare(queue='computer_books', durable=True)
        proxy.channel.queue_declare(queue='top_fiction_books', durable=True)
        proxy.channel.queue_declare(queue='top_90s_books', durable=True)
        proxy.channel.queue_declare(queue='popular_90s_books', durable=True)
        proxy.channel.queue_bind('popular_90s_books', 'popular_90s_exchange', routing_key='popular_90s_queue')

        while True:
            message, flag = protocol.receive_message()
            if flag == MESSAGE_FLAG['BOOK']:
                proxy.publish(message, 'books_exchange')
            elif flag == MESSAGE_FLAG['RATING']:
                proxy.publish(message, 'reviews_exchange')


def get_books_keys(row):
    date_str = row['publishedDate']
    categories_str = row['categories']

    year = date_str.split('-', maxsplit=1)[0]
    year_dots = '.'.join(year)

    categories = categories_str.strip("[]")
    categories = categories.replace("'", "").replace(" & ", ".")
    categories_list = categories.split(", ")
    categories_dots = '.'.join(categories_list)
    if len(categories_dots) + len(year_dots) + 2 > MAX_KEY_LENGTH:
        categories_dots = categories_dots[:MAX_KEY_LENGTH - len(year_dots) - 2]
    return f".{categories_dots}.{year_dots}"

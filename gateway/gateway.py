from threading import Thread
from socket import SOCK_STREAM, socket, AF_INET
from pika.exchange_type import ExchangeType
from lib.gateway import BookPublisher, ReviewPublisher, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol

CLIENTS_BACKLOG = 5

class Gateway:
    def __init__(self, config):
        self.port = config['port']
        self.conn = None

    def start(self):
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.conn.bind(('', self.port))

        while True:
            self.conn.listen(CLIENTS_BACKLOG)
            client, addr = self.conn.accept()
            Thread(target=self.__handle_client, args=(client,)).start()


    def __handle_client(self, client):
        protocol = TransferProtocol(client)
        book_publisher = BookPublisher('rabbitmq', 'books_exchange', ExchangeType.topic)
        review_publisher = ReviewPublisher('rabbitmq')
        # Pending: declare resources once when Gateway is created.
        # declare queues for reading query results.
        book_publisher.channel.queue_declare(queue='author_decades', durable=True)
        book_publisher.channel.queue_declare(queue='computer_books', durable=True)
        book_publisher.channel.queue_declare(queue='top_fiction_books', durable=True)
        book_publisher.channel.queue_declare(queue='top_90s_books', durable=True)
        book_publisher.channel.queue_declare(queue='popular_90s_books', durable=True)
        book_publisher.channel.exchange_declare('popular_90s_exchange', exchange_type=ExchangeType.direct)
        book_publisher.channel.queue_bind('popular_90s_books', 'popular_90s_exchange', routing_key='popular_90s_queue')

        while True:
            message, flag = protocol.receive_message()
            if flag == MESSAGE_FLAG['BOOK']:
                book_publisher.publish(message, get_books_keys)
            elif flag == MESSAGE_FLAG['REVIEW']:
                review_publisher.publish(message, 'reviews_queue')


def get_books_keys(row):
    date_str = row['publishedDate']
    try:
        year = int(date_str.split('-', maxsplit=1)[0])
        decade = year - year % 10
        return str(decade)
    except ValueError:
        # invalid date format
        return ''

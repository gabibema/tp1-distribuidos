import json
import logging
from threading import Thread
from socket import SOCK_STREAM, socket, AF_INET
from pika.exchange_type import ExchangeType
from lib.gateway import BookPublisher, ResultReceiver, ReviewPublisher, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol

CLIENTS_BACKLOG = 5

class Gateway:
    def __init__(self, config):
        self.result_queues = config['result_queues']
        self.port = config['port']
        self.conn = None

    def start(self):
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.conn.bind(('', self.port))
        #self.__wait_workers()


        self.book_publisher = BookPublisher('rabbitmq', 'books_exchange', ExchangeType.topic)
        self.review_publisher = ReviewPublisher('rabbitmq')
        #self.result_receiver = ResultReceiver('rabbitmq', )

        while True:
            self.conn.listen(CLIENTS_BACKLOG)
            client, addr = self.conn.accept()
            Thread(target=self.__handle_client, args=(client,)).start()


    def __handle_client(self, client):
        protocol = TransferProtocol(client)
        # Pending: declare resources once when Gateway is created.
        # declare queues for reading query results.

        while True:
            message, flag = protocol.receive_message()
            if flag == MESSAGE_FLAG['BOOK']:
                self.book_publisher.publish(message, get_books_keys)
            elif flag == MESSAGE_FLAG['REVIEW']:
                self.review_publisher.publish(message, 'reviews_queue')

    def __wait_workers(self):
        book_publisher = BookPublisher('rabbitmq', 'books_exchange', ExchangeType.topic)
        review_publisher = ReviewPublisher('rabbitmq')
        book_publisher.publish('EOF', get_books_keys)
        review_publisher.publish('EOF', 'reviews_queue')
        book_publisher.close()
        review_publisher.close()

        result_receiver = ResultReceiver('rabbitmq', self.result_queues, callback_result, self.result_queues.copy())
        result_receiver.start()

        result_receiver.close()


def callback_result(ch, method, properties, body, queue_name, callback_arg):
    body = json.loads(body)
    logging.warning(f'Received message of length {len(body)} from {queue_name}: {body}')
    
    if body['type'] == 'EOF':
        callback_arg.remove(queue_name)
    
    if not callback_arg:
        ch.stop_consuming()


def get_books_keys(row):
    date_str = row['publishedDate']
    try:
        year = int(date_str.split('-', maxsplit=1)[0])
        decade = year - year % 10
        return str(decade)
    except ValueError:
        # invalid date format
        return ''

import json
import logging
from threading import Thread
from socket import SOCK_STREAM, socket, AF_INET
from pika.exchange_type import ExchangeType
from lib.gateway import BookPublisher, ResultReceiver, ReviewPublisher, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol
from lib.workers.workers import wait_rabbitmq

CLIENTS_BACKLOG = 5

class Gateway:
    def __init__(self, config):
        self.result_queues = config['result_queues']
        self.port = config['port']
        self.conn = None

    def start(self):
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.conn.bind(('', self.port))
        wait_rabbitmq()
        self.__wait_workers()


        self.book_publisher = BookPublisher('rabbitmq', 'books_exchange', ExchangeType.topic)
        self.review_publisher = ReviewPublisher('rabbitmq')

        while True:
            self.conn.listen(CLIENTS_BACKLOG)
            client, addr = self.conn.accept()
            Thread(target=self.__handle_client, args=(client,)).start()


    def __handle_client(self, client):
        protocol = TransferProtocol(client)
        result_receiver = ResultReceiver('rabbitmq', self.result_queues, callback_result_client, protocol)

        eof_count = 0
        while True:
            message, flag = protocol.receive_message()
                
            if flag == MESSAGE_FLAG['BOOK']:
                eof_count += self.book_publisher.publish(message, get_books_keys)
            elif flag == MESSAGE_FLAG['REVIEW']:
                eof_count += self.review_publisher.publish(message, 'reviews_queue')
            
            if eof_count == 2:
                break

        logging.warning('EOF received from both files')
        result_receiver.start()


    def __wait_workers(self):
        book_publisher = BookPublisher('rabbitmq', 'books_exchange', ExchangeType.topic)
        review_publisher = ReviewPublisher('rabbitmq')
        result_receiver = ResultReceiver('rabbitmq', self.result_queues, callback_result, self.result_queues.copy())

        book_publisher.publish('EOF', get_books_keys)
        review_publisher.publish('EOF', 'reviews_queue')
        book_publisher.close()
        review_publisher.close()
        
        result_receiver.start()
        result_receiver.close()


def callback_result_client(ch, method, properties, body, queue_name, callback_arg: TransferProtocol):
    body = json.loads(body)

    if 'type' in body:
        callback_arg.send_message(json.dumps({'file': queue_name}), MESSAGE_FLAG['EOF'])
    else:
        callback_arg.send_message(json.dumps({'file':queue_name, 'body':body}), MESSAGE_FLAG['RESULT'])
    

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

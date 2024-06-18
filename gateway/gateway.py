import json
import logging
from threading import Thread
from socket import SOCK_STREAM, socket, AF_INET
from pika.exchange_type import ExchangeType
from lib.broker import MessageBroker
from lib.gateway import BookPublisher, ResultReceiver, ReviewPublisher, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, MessageTransferProtocol
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
        #self.__wait_workers()

        while True:
            self.conn.listen(CLIENTS_BACKLOG)
            client, addr = self.conn.accept()
            Thread(target=self.__handle_client, args=(client,)).start()


    def __handle_client(self, client):
        logging.warning(f'New client connection: {client}')
        protocol = MessageTransferProtocol(client)
        connection = MessageBroker("rabbitmq")
        result_receiver = ResultReceiver(connection, self.result_queues, callback_result_client, protocol)
        book_publisher = BookPublisher(connection, 'books_exchange', ExchangeType.topic)
        review_publisher = ReviewPublisher(connection)

        eof_count = 0
        while True:
            flag, client_id, message_id, message_csv = protocol.receive_message()
            if flag == MESSAGE_FLAG['BOOK']:
                eof_count += book_publisher.publish(client_id, message_id, message_csv, get_books_keys)
            elif flag == MESSAGE_FLAG['REVIEW']:
                eof_count += review_publisher.publish(client_id, message_id, message_csv, 'reviews_queue')
            else:
                logging.error(f'Unsupported message flag {repr(flag)}')
            if eof_count == 2:
                break

        logging.warning('EOF received from both files')
        result_receiver.start()


    def __wait_workers(self):
        connection = MessageBroker("rabbitmq")
        book_publisher = BookPublisher(connection, 'books_exchange', ExchangeType.topic)
        review_publisher = ReviewPublisher(connection)
        result_receiver = ResultReceiver(connection, self.result_queues, callback_result, self.result_queues.copy())

        client_id = 'READINESS_PROBE'
        message_id = 0
        book_publisher.publish(client_id, message_id, '', get_books_keys)
        review_publisher.publish(client_id, message_id, '', 'reviews_queue')
        book_publisher.close()
        review_publisher.close()
        
        result_receiver.start()
        result_receiver.close()


def callback_result_client(self, ch, method, properties, body, queue_name, callback_arg: MessageTransferProtocol):
    message = json.loads(body)
    # PENDING: propagate message_id all they way back to the client.
    if message.get('type') == 'EOF':
        callback_arg.send_message(MESSAGE_FLAG['EOF'], self.id, message['message_id'], json.dumps({'file': queue_name}))
    else:
        callback_arg.send_message(MESSAGE_FLAG['RESULT'], self.id, message['message_id'], json.dumps({'file':queue_name, 'body':message}))
    
    self.connection.acknowledge_message(method.delivery_tag)
    

def callback_result(ch, method, properties, body, queue_name, callback_arg):
    message = json.loads(body)
    logging.warning(f'Received message of length {len(body)} from {queue_name}: {message}')

    if message.get('type') == 'EOF':
        callback_arg.remove(queue_name)

    if not callback_arg:
        ch.stop_consuming()


def get_books_keys(row):
    if 'type' in row and row['type'] == 'EOF':
        return 'EOF'

    date_str = row['publishedDate']
    try:
        year = int(date_str.split('-', maxsplit=1)[0])
        decade = year - year % 10
        return str(decade)
    except ValueError:
        # invalid date format
        return ''

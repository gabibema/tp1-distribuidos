import json
import logging
from multiprocessing import Process
from socket import SOCK_STREAM, socket, AF_INET
from typing import Tuple
from uuid import UUID
from pika.exchange_type import ExchangeType
from data_storage import DataSaver
from lib.broker import MessageBroker
from lib.gateway import BookPublisher, ResultReceiver, ReviewPublisher, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, MessageTransferProtocol, RouterProtocol
from lib.workers.workers import wait_rabbitmq

CLIENTS_BACKLOG = 5

class Gateway:
    def __init__(self, config):
        self.result_queues = config['result_queues']
        self.port = config['port']
        self.router = RouterProtocol()
        self.data_saver = DataSaver(config['records_path'])
        self.data_saver_results = DataSaver(config['results_path'])
        logging.warning(f'Gateway initialized with save path {config["records_path"]}')
        self.conn = None

    def start(self):
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.conn.bind(('', self.port))
        wait_rabbitmq()
        #self.__wait_workers()

        while True:
            self.conn.listen(CLIENTS_BACKLOG)
            client, addr = self.conn.accept()
            Process(target=self.__handle_client, args=(client, self.router)).start()


    def __handle_client(self, client, router: RouterProtocol):
        logging.warning(f'New client connection: {client}')
        protocol = MessageTransferProtocol(client)
        connection = MessageBroker("rabbitmq")
        result_receiver = ResultReceiver(connection, self.result_queues, callback_result_client, protocol, self.data_saver_results)
        book_publisher = BookPublisher(connection, 'books_exchange', ExchangeType.topic, self.data_saver)
        review_publisher = ReviewPublisher(connection, self.data_saver)
        self.__main_loop_client(protocol, router, book_publisher, review_publisher)
        normal_dict = {k: dict(v) for k, v in self.data_saver.shared_last_rows.items()}
        logging.warning(f'Final dict is: {normal_dict}')

        result_receiver.start()

    def __get_checkpoint(self, client_id) -> Tuple[int, dict]:
        client_checkpoint = self.data_saver.shared_last_rows.get(str(client_id), {})
        eof_count = 0
        if MESSAGE_FLAG['BOOK'] in client_checkpoint and client_checkpoint[MESSAGE_FLAG['BOOK']].get('eof'):
            eof_count += 1
        if MESSAGE_FLAG['REVIEW'] in client_checkpoint and client_checkpoint[MESSAGE_FLAG['REVIEW']].get('eof'):
            eof_count += 1
            
        return eof_count, dict(client_checkpoint)

    def __main_loop_client(self, protocol, router, book_publisher, review_publisher):
        flag, client_id, message_id, message = protocol.receive_message()
        eof_count, client_routed = self.__get_checkpoint(client_id)
        
        while True:
            client_routed = self.__ensure_client_routed(router, protocol, client_id, client_routed)
            eof_count += self.__handle_message(flag, client_id, message_id, message, book_publisher, review_publisher, protocol)
            if eof_count == 2:
                break
            flag, client_id, message_id, message = protocol.receive_message()

    def __ensure_client_routed(self, router, protocol, client_id, client_routed):
        if not client_routed:
            logging.warning(f'Adding connection for client {client_id}')
            router.add_connection(protocol, client_id)
            client_routed = True
        return client_routed

    def __handle_message(self, flag, client_id, message_id, message, book_publisher, review_publisher, protocol):
        if flag == MESSAGE_FLAG['BOOK']:
            return book_publisher.publish(client_id, message_id, message, get_books_keys)
        elif flag == MESSAGE_FLAG['REVIEW']:
            return review_publisher.publish(client_id, message_id, message, 'reviews_queue')
        elif flag == MESSAGE_FLAG['CHECKPOINT']:
            _, client_checkpoint = self.__get_checkpoint(client_id)
            protocol.send_message(MESSAGE_FLAG['CHECKPOINT'], client_id, message_id, json.dumps(client_checkpoint))
            return 0
        else:
            logging.error(f'Unsupported message flag {repr(flag)}')
            return 0
            
    def __wait_workers(self):
        connection = MessageBroker("rabbitmq")
        book_publisher = BookPublisher(connection, 'books_exchange', ExchangeType.topic)
        review_publisher = ReviewPublisher(connection)
        result_receiver = ResultReceiver(connection, self.result_queues, callback_result, self.result_queues.copy())

        book_publisher.publish('', get_books_keys)
        review_publisher.publish('', 'reviews_queue')
        book_publisher.close()
        review_publisher.close()
        
        result_receiver.start()
        result_receiver.close()


def callback_result_client(self, ch, method, properties, body, queue_name, callback_arg1: RouterProtocol, callback_arg2: DataSaver):
    body = json.loads(body)
    # PENDING: propagate message_id all they way back to the client.
    message_id = body.get('message_id', 1)
    request_id = UUID(get_uid(body))
    logging.warning(f'Received message of length {len(body)} from {queue_name}:\n {body}')
    saved_body = {'request_id': str(request_id), 'message_id': message_id, 'source': queue_name, 'body': body}
    callback_arg2.save_message_to_json(saved_body)

    if isinstance(body, dict) and body.get('type') == 'EOF':
        callback_arg1.send_message(MESSAGE_FLAG['EOF'], request_id, message_id, json.dumps({'file': queue_name}))
    else:
        callback_arg1.send_message(MESSAGE_FLAG['RESULT'],request_id,message_id, json.dumps({'file':queue_name, 'body':body}))
    
    self.connection.acknowledge_message(method.delivery_tag)




"""
Para books:
2024-06-13 19:15:43 gateway-1                        |  {'request_id': 'f412e42c-8f80-4d1c-882f-fac6a88d8465', 'authors': ["'John Ruskin'", "'Oscar Wilde'", "'Bertrand Russell'", "'Charles Kingsley'", "'Nathaniel Hawthorne'", "'Edgar Allan Poe'", "'Charles Dickens'", "'Edgar Rice Burroughs'", "'Erle Stanley Gardner'", "'Thomas Hardy'", "'William Shakespeare'", "'Franklin W. Dixon'", "'Arthur Conan Doyle'", "'Graham Greene'", "'Sigmund Freud'", "'Henry James'", "'John Bunyan'", "'Zane Grey'", "'Dante Alighieri'", "'Henry David Thoreau'", "'Henry Adams'", "'Joseph Conrad'", "'Agatha Christie'", "'Thomas Carlyle'", "'Henry Wadsworth Longfellow'", "'Voltaire'", "'Francis Parkman'", "'Rudyard Kipling'", "'Jack London'", "'Daniel Defoe'", "'Bernard Shaw'", "'Walter Scott'", "'William Dean Howells'", "'Robert Louis Stevenson'", "'Washington Irving'", "'James Fenimore Cooper'", "'Jules Verne'", "'Anthony Trollope'", "'Ellery Queen'", "'Jane Austen'", "'Alexandre Dumas'", "'Ernest Hemingway'", "'Mark Twain'", "'William Makepeace Thackeray'", "'Charles Darwin'", "'Charles Haddon Spurgeon'", "'Louisa May Alcott'", "'Ralph Waldo Emerson'", "'Henrik Ibsen'", "'Herbert George Wells'", "'Andrew Murray'", "'Miguel de Cervantes Saavedra'", "'Thomas Jefferson'", "'Lewis Carroll'", "'Gilbert Keith Chesterton'", "'Mary Roberts Rinehart'", "'Plato'", "'Lafcadio Hearn'", "'Karl Marx'", "'Emanuel Swedenborg'", "'Herman Melville'", "'John Milton'", "'Niccolò Machiavelli'", "'Edward Gibbon'"]}
2024-06-13 19:15:43 gateway-1                        |  {'request_id': 'f412e42c-8f80-4d1c-882f-fac6a88d8465', 'message_id': 55, 'type': 'EOF'}

"""


def callback_result(ch, method, properties, body, queue_name, callback_arg):
    body = json.loads(body)
    logging.warning(f'Received message of length {len(body)} from {queue_name}: {body}')

    if body.get('type') == 'EOF':
        callback_arg.remove(queue_name)

    if not callback_arg:
        ch.stop_consuming()

def get_uid_raw(message):
    parts = message.split('\n', 1)
    if len(parts) > 0:
        return parts[0]
    return None
    

def get_uid(body):
    if isinstance(body, dict) and 'request_id' in body:
        return body['request_id']
    elif isinstance(body, list) and body and 'request_id' in body[0]:
        return body[0]['request_id']


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

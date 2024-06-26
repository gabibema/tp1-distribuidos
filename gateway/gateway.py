import json
import logging
from multiprocessing import Process
from socket import SOCK_STREAM, socket, AF_INET
from typing import Tuple
from uuid import UUID
from pika.exchange_type import ExchangeType
from data_storage import ALL_ROWS, DataSaver, write_csv_to_string
from lib.broker import MessageBroker
from lib.gateway import BookPublisher, ResultReceiver, ReviewPublisher, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, MessageTransferProtocol, RouterProtocol
from lib.workers.workers import wait_rabbitmq
from lib.healthcheck import Healthcheck, HEALTH
from os.path import exists

CLIENTS_BACKLOG = 5
RESULTS_BACKLOG = 5
source_mapping = {
    "author_decades": {"headers": ["author"], "key": "authors"},
    "popular_90s_books": {"headers": ["Title", "count"], "key": "items"},
    "top_90s_books": {"headers": ["Title", "count"], "key": "top10"},
    "top_fiction_books": {"headers": ["Title", "average"], "key": "items"},
    "computer_books": {"headers": [], "key": "items"}
}


class Gateway:
    def __init__(self, config):
        self.result_queues = config['result_queues']
        self.port = config['port']
        self.router = RouterProtocol()
        self.data_saver = DataSaver(config['records_path'])
        self.data_saver_results = DataSaver(config['results_path'], mode=ALL_ROWS)
        self.healthcheck = Process(target=Healthcheck().listen_healthchecks)
        self.conn = None

    def start(self):
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.conn.bind(('', self.port))
        self.healthcheck.start()
        wait_rabbitmq()

        while True:
            self.conn.listen(CLIENTS_BACKLOG)
            client, addr = self.conn.accept()
            Process(target=self.__handle_client, args=(client, self.router)).start()


    def __handle_client(self, client:socket, router: RouterProtocol):
        protocol = MessageTransferProtocol(client)
        connection = MessageBroker("rabbitmq")
        result_receiver = ResultReceiver(connection, self.result_queues, callback_result_client, protocol, self.data_saver_results)
        book_publisher = BookPublisher(connection, 'books_exchange', ExchangeType.topic, self.data_saver)
        review_publisher = ReviewPublisher(connection, self.data_saver)
        client_id = self.__main_loop_client(protocol, router, book_publisher, review_publisher)
        
        if self.data_saver_results.get_eof_count(client_id) != RESULTS_BACKLOG:
            result_receiver.start()

        self.__send_results(protocol, client_id)
        logging.warning(f'Client connection closed: {client_id} all eof received.')
        connection.close_connection()

    def __send_results(self, protocol, client_id):
        results = self.data_saver_results.get(client_id)
        result_data = {source: "" for source in source_mapping}
        eof_data = {}
        client_id = UUID(client_id)
        
        for result in results:
            source = result['source']
            body = result['body']
            
            headers = source_mapping[source]["headers"]
            key = source_mapping[source]["key"]

            if body.get("type") == "EOF":
                eof_data[source] = write_csv_to_string(headers, [])
                continue

            if source == "author_decades":
                rows = [[author.strip("'")] for author in body.get("authors", [])]
            else:
                rows = [[item[header] for header in headers] for item in body.get(key, [])]
            
            if not result_data[source]:
                result_data[source] = write_csv_to_string(headers, rows)
            else:
                result_data[source] += write_csv_to_string([], rows)  

        for source, csv_string in result_data.items():
            if not csv_string:
                continue
            print(f"Source: {source}")
            protocol.send_message(MESSAGE_FLAG['RESULT'], client_id, 1, 
                                  json.dumps({'file': source, 'body': csv_string}))
        
        # Send EOF messages
        for source, csv_string in eof_data.items():
            protocol.send_message(MESSAGE_FLAG['EOF'], client_id, 2, 
                                  json.dumps({'file': source, 'body': csv_string}))
    
    def __get_checkpoint(self, client_id) -> Tuple[int, dict]:
        client_checkpoint = self.data_saver.get(client_id)
        eof_count = 0
        if MESSAGE_FLAG['BOOK'] in client_checkpoint and client_checkpoint[MESSAGE_FLAG['BOOK']].get('eof'):
            eof_count += 1
        if MESSAGE_FLAG['REVIEW'] in client_checkpoint and client_checkpoint[MESSAGE_FLAG['REVIEW']].get('eof'):
            eof_count += 1
            
        return eof_count, dict(client_checkpoint)

    def __main_loop_client(self, protocol: MessageTransferProtocol, router, book_publisher, review_publisher):
        flag, client_id, message_id, message = protocol.receive_message()
        eof_count, _ = self.__get_checkpoint(client_id)
        self.router.add_connection(protocol, client_id)
        eof_count += self.__handle_message(flag, client_id, message_id, message, book_publisher, review_publisher, protocol)

        while eof_count < 2:
            flag, client_id, message_id, message = protocol.receive_message()
            eof_count += self.__handle_message(flag, client_id, message_id, message, book_publisher, review_publisher, protocol)
        
        return str(client_id)


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
        result_receiver = ResultReceiver(connection, self.result_queues, callback_result, self.result_queues.copy(), 0)

        client_id = 'READINESS_PROBE'
        message_id = 0
        book_publisher.publish(client_id, message_id, '', get_books_keys)
        review_publisher.publish(client_id, message_id, '', 'reviews_queue')
        book_publisher.close()
        review_publisher.close()
        
        result_receiver.start()
        result_receiver.close()


def callback_result_client(self, ch, method, properties, body, queue_name, callback_arg1: RouterProtocol, callback_arg2: DataSaver):
    body = json.loads(body)
    # PENDING: propagate message_id all they way back to the client.
    message_id = body.get('message_id', 1) if isinstance(body, dict) else 1
    request_id = UUID(get_uid(body))
    #logging.warning(f'Received message of length {len(body)} from {queue_name}:\n {body}')
    is_eof = body.get('type') == 'EOF'
    saved_body = {'request_id': request_id, 'message_id': message_id, 'source': queue_name, 'body': body, 'eof': is_eof}
    callback_arg2.save_message_to_json(saved_body)
    self.connection.acknowledge_message(method.delivery_tag)
    logging.warning(f'EOF count for {request_id}: {callback_arg2.get_eof_count(request_id)}')
    if callback_arg2.get_eof_count(request_id) == RESULTS_BACKLOG:
        ch.stop_consuming()
        
"""
Para books:
2024-06-13 19:15:43 gateway-1                        |  {'request_id': 'f412e42c-8f80-4d1c-882f-fac6a88d8465', 'authors': ["'John Ruskin'", "'Oscar Wilde'", "'Bertrand Russell'", "'Charles Kingsley'", "'Nathaniel Hawthorne'", "'Edgar Allan Poe'", "'Charles Dickens'", "'Edgar Rice Burroughs'", "'Erle Stanley Gardner'", "'Thomas Hardy'", "'William Shakespeare'", "'Franklin W. Dixon'", "'Arthur Conan Doyle'", "'Graham Greene'", "'Sigmund Freud'", "'Henry James'", "'John Bunyan'", "'Zane Grey'", "'Dante Alighieri'", "'Henry David Thoreau'", "'Henry Adams'", "'Joseph Conrad'", "'Agatha Christie'", "'Thomas Carlyle'", "'Henry Wadsworth Longfellow'", "'Voltaire'", "'Francis Parkman'", "'Rudyard Kipling'", "'Jack London'", "'Daniel Defoe'", "'Bernard Shaw'", "'Walter Scott'", "'William Dean Howells'", "'Robert Louis Stevenson'", "'Washington Irving'", "'James Fenimore Cooper'", "'Jules Verne'", "'Anthony Trollope'", "'Ellery Queen'", "'Jane Austen'", "'Alexandre Dumas'", "'Ernest Hemingway'", "'Mark Twain'", "'William Makepeace Thackeray'", "'Charles Darwin'", "'Charles Haddon Spurgeon'", "'Louisa May Alcott'", "'Ralph Waldo Emerson'", "'Henrik Ibsen'", "'Herbert George Wells'", "'Andrew Murray'", "'Miguel de Cervantes Saavedra'", "'Thomas Jefferson'", "'Lewis Carroll'", "'Gilbert Keith Chesterton'", "'Mary Roberts Rinehart'", "'Plato'", "'Lafcadio Hearn'", "'Karl Marx'", "'Emanuel Swedenborg'", "'Herman Melville'", "'John Milton'", "'Niccolò Machiavelli'", "'Edward Gibbon'"]}
2024-06-13 19:15:43 gateway-1                        |  {'request_id': 'f412e42c-8f80-4d1c-882f-fac6a88d8465', 'message_id': 55, 'type': 'EOF'}
2024-06-17 19:21:25 gateway-1                        |  {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'top10': [{'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': 'Pride and Prejudice', 'count': 20371}, {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': "The Hitchhiker's Guide to the Galaxy", 'count': 4042}, {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': 'A Tree Grows in Brooklyn', 'count': 3904}, {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': 'Harry Potter and the Chamber of Secrets', 'count': 3137}, {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': 'Slaughterhouse-Five', 'count': 2975}, {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': 'Rich Dad, Poor Dad', 'count': 2734}, {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': 'Memoirs of a Geisha (Signed)', 'count': 2693}, {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': 'The Lord of the Rings (3 Volume Set)', 'count': 2478}, {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': 'Wuthering Heights (Signet classics)', 'count': 2160}, {'request_id': '551d0f23-17c9-48ef-b32a-3b1d129c8709', 'Title': 'Catch 22', 'count': 2084}]}
"""

def callback_result(ch, method, properties, body, queue_name, callback_arg):
    message = json.loads(body)
    logging.warning(f'Received message of length {len(body)} from {queue_name}: {message}')

    if message.get('type') == 'EOF':
        callback_arg.remove(queue_name)

    if not callback_arg:
        ch.stop_consuming()
    

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

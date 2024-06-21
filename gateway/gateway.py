import json
import logging
from multiprocessing import Process
from socket import SOCK_STREAM, socket, AF_INET
from typing import Tuple
from uuid import UUID
from pika.exchange_type import ExchangeType
from data_storage import ALL_ROWS, DataSaver
from lib.broker import MessageBroker
from lib.gateway import BookPublisher, ResultReceiver, ReviewPublisher, MAX_KEY_LENGTH
from lib.transfer.transfer_protocol import MESSAGE_FLAG, MessageTransferProtocol, RouterProtocol
from lib.workers.workers import wait_rabbitmq

CLIENTS_BACKLOG = 5
RESULTS_BACKLOG = 5

class Gateway:
    def __init__(self, config):
        self.result_queues = config['result_queues']
        self.port = config['port']
        self.router = RouterProtocol()
        self.data_saver = DataSaver(config['records_path'])
        self.data_saver_results = DataSaver(config['results_path'], mode=ALL_ROWS)
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
        book_publisher = BookPublisher(connection, 'books_exchange', ExchangeType.fanout, self.data_saver)
        review_publisher = ReviewPublisher(connection, self.data_saver)
        self.__main_loop_client(protocol, router, book_publisher, review_publisher)

        #normal_dict = {k: dict(v) for k, v in self.data_saver.shared_rows.items()}
        #logging.warning(f'Final dict is: {normal_dict}')

        eof_count = self.__fetch_results_checkpoint(protocol)
        result_receiver.start(eof_count)

    def __fetch_results_checkpoint(self, protocol):
        flag, client_id, message_id, message = protocol.receive_message()
        eof_count = 0
        if flag != MESSAGE_FLAG['CHECKPOINT']:
            logging.error(f'Expected checkpoint message, but received {flag}')        
            return eof_count

        results = self.data_saver_results.get(client_id)
        for result in results:
            logging.warning(f'Processing result: {result}')
            message = result['body']
            if isinstance(message, dict) and message.get('type') == 'EOF':
                protocol.send_message(MESSAGE_FLAG['EOF'], client_id, result['message_id'], 
                                        json.dumps({'file': result['source']}))
                eof_count += 1
                logging.warning(f'EOF received from {result["source"]}. Total EOFs: {eof_count}')
            else:
                protocol.send_message(MESSAGE_FLAG['RESULT'],client_id,result['message_id'], 
                                      json.dumps({'file':result['source'], 'body':message}))

        protocol.send_message(MESSAGE_FLAG['END_CHECKPOINT'], client_id, 1, '')
        return eof_count

    def __get_checkpoint(self, client_id) -> Tuple[int, dict]:
        client_checkpoint = self.data_saver.get(client_id)
        eof_count = 0
        if MESSAGE_FLAG['BOOK'] in client_checkpoint and client_checkpoint[MESSAGE_FLAG['BOOK']].get('eof'):
            eof_count += 1
        if MESSAGE_FLAG['REVIEW'] in client_checkpoint and client_checkpoint[MESSAGE_FLAG['REVIEW']].get('eof'):
            eof_count += 1

        return eof_count, dict(client_checkpoint)

    def __main_loop_client(self, protocol, router, book_publisher, review_publisher):
        flag, client_id, message_id, message = protocol.receive_message()
        eof_count, _ = self.__get_checkpoint(client_id)
        self.router.add_connection(protocol, client_id)
        eof_count += self.__handle_message(flag, client_id, message_id, message, book_publisher, review_publisher, protocol)

        while eof_count < 2:
            flag, client_id, message_id, message = protocol.receive_message()
            eof_count += self.__handle_message(flag, client_id, message_id, message, book_publisher, review_publisher, protocol)


    def __handle_message(self, flag, client_id, message_id, message, book_publisher, review_publisher, protocol):
        if flag == MESSAGE_FLAG['BOOK']:
            return book_publisher.publish(client_id, message_id, message, '')
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
        book_publisher = BookPublisher(connection, 'books_exchange', ExchangeType.fanout)
        review_publisher = ReviewPublisher(connection)
        result_receiver = ResultReceiver(connection, self.result_queues, callback_result, self.result_queues.copy(), 0)

        client_id = 'READINESS_PROBE'
        message_id = 0
        book_publisher.publish(client_id, message_id, '', '')
        review_publisher.publish(client_id, message_id, '', 'reviews_queue')
        book_publisher.close()
        review_publisher.close()

        result_receiver.start()
        result_receiver.close()


def callback_result_client(self, ch, method, properties, body, queue_name, callback_arg1: RouterProtocol, callback_arg2: DataSaver, eof_count: int):
    body = json.loads(body)
    # PENDING: propagate message_id all they way back to the client.
    message_id = body.get('message_id', 1) if isinstance(body, dict) else 1
    request_id = UUID(get_uid(body))
    logging.warning(f'Received message of length {len(body)} from {queue_name}:\n {body}')
    saved_body = {'request_id': str(request_id), 'message_id': message_id, 'source': queue_name, 'body': body}
    callback_arg2.save_message_to_json(saved_body)

    try: 
        if isinstance(body, dict) and body.get('type') == 'EOF':
            callback_arg1.send_message(MESSAGE_FLAG['EOF'], request_id, message_id, json.dumps({'file': queue_name}))
            self.eof_count += 1
            logging.warning(f'EOF received from {queue_name}. Total EOFs: {self.eof_count}')
        else:
            callback_arg1.send_message(MESSAGE_FLAG['RESULT'],request_id,message_id, json.dumps({'file':queue_name, 'body':body}))
    except Exception as _:
        pass

    self.connection.acknowledge_message(method.delivery_tag)
    if eof_count == RESULTS_BACKLOG:
        ch.stop_consuming()


def callback_result(ch, method, properties, body, queue_name, callback_arg):
    message = json.loads(body)
    logging.warning(f'Received message of length {len(body)} from {queue_name}: {message}')

    if message.get('type') == 'EOF':
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

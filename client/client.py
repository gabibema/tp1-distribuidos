import os
from multiprocessing import Process, Queue, Value
from socket import SOCK_STREAM, socket, AF_INET, create_connection
from time import time
from uuid import UUID
from uuid import uuid4
from csv import DictWriter
from json import dumps, loads
import logging

from lib.transfer.transfer_protocol import MESSAGE_FLAG, MessageTransferProtocol

READ_MODE = 'r'
RESULT_FILES_AMOUNT = 5

class Client:
    def __init__(self, config):
        self.port = config['port']
        self.batch_amount = config['batch_amount']
        self.checkpoint = None
        
        self.books_path = config['books_path']
        self.reviews_path = config['reviews_path']

        self.output_dir = config['output_dir']
        self.uuid = UUID(config['client_id'])

        self.books_queue = Queue()
        self.reviews_queue = Queue()
        self.books_sender = Process(target=self.__enqueue_file, args=(self.books_path, self.books_queue, MESSAGE_FLAG['BOOK']))
        self.reviews_sender = Process(target=self.__enqueue_file, args=(self.reviews_path, self.reviews_queue, MESSAGE_FLAG['REVIEW']))


    def start(self):
        self.conn = self.__try_connect('gateway', self.port)
        self.checkpoint = self.__request_checkpoint()
        logging.warning(f'Checkpoint received: {self.checkpoint}')
        
        self.books_sender.start()
        self.reviews_sender.start()
        self.__send_from_queue(self.books_queue, self.reviews_queue)
        self.books_sender.join()
        self.reviews_sender.join()
        self.__request_results()


    def __try_connect(self, host, port, timeout=15):
        actual_time = time()
        while time() - actual_time < timeout:
            try:
                conn = create_connection((host, port))
                return conn
            except:
                pass

        raise SystemError('Could not connect to the server')    # not handled at the moment

    def __request_checkpoint(self):
        protocol = MessageTransferProtocol(self.conn)
        protocol.send_message(MESSAGE_FLAG['CHECKPOINT'], self.uuid, 1, '')
        flag, _gateway_id, message_id, message = protocol.receive_message()

        if flag == MESSAGE_FLAG['CHECKPOINT']:
            return loads(message)
        else:
            raise SystemError('Invalid response from the server')    # not handled at the moment

    def __enqueue_file(self, path, queue, source):
        # Batch message format:
        # field1,field2,...
        # value1,value2,...
        # value1,value2,...
        # ...
        source = str(source)
        start_id = 1
        eof = None
        
        if source in self.checkpoint:
            start_id = self.checkpoint[source]["message_id"]
            eof = self.checkpoint[source]["eof"]
        if eof:
            logging.warning(f'Skipping file {path} due to EOF')
            return

        with open(path, READ_MODE) as file:
            headers = file.readline()
            self.__queue_file_rows(file, queue, source, start_id, headers)
            

    def __queue_file_rows(self, file, queue, source, start_id, headers):
        message_id = 1
        batch = [headers]
        for line in file:
            batch.append(line)
            if len(batch) - 1 >= self.batch_amount:
                message_id += 1
                if message_id < start_id:
                    logging.warning(f'Skipping message {message_id}')
                    batch = [headers]
                    continue
                batch[-1] = batch[-1].rstrip()
                queue.put((message_id, ''.join(batch)))
                batch = [headers]

        if len(batch) > 1:
            batch[-1] = batch[-1].rstrip()
            queue.put((message_id, ''.join(batch)))

        batch = ['type', 'EOF']
        queue.put((message_id + 1, '\n'.join(batch)))
    
    def __sending_completed(self, books_queue: Queue, reviews_queue: Queue):
        return books_queue.empty() and reviews_queue.empty() and not self.books_sender.is_alive() and not self.reviews_sender.is_alive()

    def __send_from_queue(self, books_queue: Queue, reviews_queue: Queue):
        protocol = MessageTransferProtocol(self.conn)

        while True:
            if not books_queue.empty():
                message_id, message = books_queue.get()
                protocol.send_message(MESSAGE_FLAG['BOOK'], self.uuid, message_id, message)
            if not reviews_queue.empty():
                message_id, message = reviews_queue.get()
                protocol.send_message(MESSAGE_FLAG['REVIEW'], self.uuid, message_id, message)

            if self.__sending_completed(books_queue, reviews_queue):
                break

    def __request_results(self):
        protocol = MessageTransferProtocol(self.conn)
        eof_count = 0
        while eof_count < RESULT_FILES_AMOUNT:
            flag = self.__handle_result(protocol)
            if flag == MESSAGE_FLAG['EOF']:
                eof_count += 1

    def __handle_result(self, protocol: MessageTransferProtocol):
        flag, _gateway_id, message_id, message = protocol.receive_message()
        if flag == MESSAGE_FLAG['RESULT']:
            body = loads(message)
            #logging.warning(f"Received message with ID '{message_id}' from Gateway'")
            self.__save_in_file(body['file'], body['body'])
        
        return flag

    def __save_in_file(self, filename, body: dict):
        filepath = os.path.join(self.output_dir, f'{filename}.csv')
        file_exists = os.path.isfile(filepath)
        if body:
            with open(filepath, 'a+', newline='') as file:
                file.write(body)
import os
from multiprocessing import Process, Queue, Value
from socket import SOCK_STREAM, socket, AF_INET, create_connection
from time import time
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
        
        self.books_path = config['books_path']
        self.reviews_path = config['reviews_path']

        self.output_dir = config['output_dir']
        self.uuid = uuid4()

        self.books_queue = Queue()
        self.reviews_queue = Queue()
        self.books_sender = Process(target=self.__enqueue_file, args=(self.books_path, self.books_queue))
        self.reviews_sender = Process(target=self.__enqueue_file, args=(self.reviews_path, self.reviews_queue))


    def start(self):
        self.books_sender.start()
        self.reviews_sender.start()
        
        self.conn = self.__try_connect('gateway', self.port)
        self.__send_from_queue(self.books_queue, self.reviews_queue)
        self.books_sender.join()
        self.reviews_sender.join()
        self.__save_results()


    def __try_connect(self, host, port, timeout=15):
        actual_time = time()
        while time() - actual_time < timeout:
            try:
                conn = create_connection((host, port))
                return conn
            except:
                pass

        raise SystemError('Could not connect to the server')    # not handled at the moment

    def __enqueue_file(self, path, queue):
        # Batch message format:
        # field1,field2,...
        # value1,value2,...
        # value1,value2,...
        # ...
        message_id = 1
        with open(path, READ_MODE) as file:
            headers = file.readline()
            batch = [headers]

            for line in file:
                batch.append(line)
                if len(batch) - 1 >= self.batch_amount:
                    batch[-1] = batch[-1].rstrip()
                    queue.put((message_id, ''.join(batch)))
                    message_id += 1
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

    def __save_results(self):
        protocol = MessageTransferProtocol(self.conn)
        eof_count = 0
        while True:
            flag, _gateway_id, message_id, message = protocol.receive_message()
            if flag == MESSAGE_FLAG['EOF']:
                logging.warning(f'EOF received')
                eof_count += 1
            elif flag == MESSAGE_FLAG['RESULT']:
                body = loads(message)
                logging.warning(f"Received message with ID '{message_id}' from Gateway'")
                self.__save_in_file(body['file'], body['body'])

            if eof_count == RESULT_FILES_AMOUNT:
                break


    def __save_in_file(self, filename, body: dict):
        filepath = os.path.join(self.output_dir, f'{filename}.csv')
        file_exists = os.path.isfile(filepath)

        if 'authors' in body:
            body_list = [{'author': author} for author in body['authors']]
            body = body_list
        elif 'top10' in body:
            body = body['top10']

        if isinstance(body, dict):
            body = body['items'] if 'items' in body else [body]
        for row in body:
            row.pop('request_id', None)
            row.pop('message_id', None)

        if body:
            with open(filepath, 'a+', newline='') as file:
                writer = DictWriter(file, fieldnames=body[0].keys())
                if not file_exists or file.tell() == 0:
                    writer.writeheader()

                writer.writerows(body)

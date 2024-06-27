import os
from multiprocessing import Process, Queue, Value
from socket import SOCK_STREAM, socket, AF_INET
from uuid import UUID
from json import loads
import logging
from signal import SIGTERM, signal, SIGALRM, alarm
from lib.transfer.transfer_protocol import MESSAGE_FLAG, MessageTransferProtocol

READ_MODE = 'r'
RESULT_FILES_AMOUNT = 5
SERVER_TIMEOUT = 250
RESULT_TIMEOUT = 500

def signal_handler(sig, frame):
    raise TimeoutError("System timeout.")

def signal_handler_exit(sig, frame):
    raise SystemExit("System shutdown.")

class Client:
    def __init__(self, config):
        self.port = config['port']
        self.batch_amount = config['batch_amount']
        self.checkpoint = None

        self.books_path = config['books_path']
        self.reviews_path = config['reviews_path']

        self.output_dir = config['output_dir']
        self.uuid = UUID(config['client_id'])

        books_queue = Queue()
        reviews_queue = Queue()
        self.finished = Value('i', 0)
        self.senders = []
        self.senders.append(Process(target=self.__enqueue_file, args=(self.books_path, books_queue, MESSAGE_FLAG['BOOK'])))
        self.senders.append(Process(target=self.__enqueue_file, args=(self.reviews_path, reviews_queue, MESSAGE_FLAG['REVIEW'])))
        self.senders.append(Process(target=self.__send_from_queue, args=(books_queue, reviews_queue, self.finished)))
        self.results_receiver = Process(target=self.__request_results)
        signal(SIGALRM, signal_handler)
        signal(SIGTERM, signal_handler_exit)
        #logging.warning(f'Client initialized with UUID: {self.uuid}')


    def start(self):
        try:
            cliend_error = False
            self.conn = self.__try_connect('gateway', self.port)
            self.checkpoint = self.__request_checkpoint()
            #logging.warning(f'Checkpoint received: {self.checkpoint}')
            self.__try_run_processes(self.senders)
            self.__try_run_processes([self.results_receiver], RESULT_TIMEOUT)
        except TimeoutError:
            cliend_error = True
        except Exception as e:
            pass
        finally:
            self.conn.close()
            for sender in self.senders:
                if sender.is_alive():
                    sender.terminate()
            if self.results_receiver.is_alive():
                self.results_receiver.terminate()
            return cliend_error

    def __try_run_processes(self, processes, timeout=SERVER_TIMEOUT):
            alarm(timeout)
            for process in processes:
                process.start()

            for process in processes:
                process.join(timeout)
                self.finished.value += 1
            alarm(0)

    def __try_connect(self, host, port):
        while True:
            try:
                conn = socket(AF_INET, SOCK_STREAM)
                conn.connect((host, port))
                return conn
            except:
                pass

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
        message_id = 0
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
            message_id += 1
            queue.put((message_id, ''.join(batch)))

        batch = ['type', 'EOF']
        queue.put((message_id + 1, '\n'.join(batch)))
    
    def __sending_completed(self, books_queue: Queue, reviews_queue: Queue, senders_finished):
        if senders_finished.value == 2 and books_queue.empty() and reviews_queue.empty():
            #logging.warning('All senders finished')
            return True

    def __send_from_queue(self, books_queue: Queue, reviews_queue: Queue, senders_finished):
        protocol = MessageTransferProtocol(self.conn)

        while True:
            if not books_queue.empty():
                message_id, message = books_queue.get()
                protocol.send_message(MESSAGE_FLAG['BOOK'], self.uuid, message_id, message)
            if not reviews_queue.empty():
                message_id, message = reviews_queue.get()
                protocol.send_message(MESSAGE_FLAG['REVIEW'], self.uuid, message_id, message)

            if self.__sending_completed(books_queue, reviews_queue, senders_finished):
                break

    def __request_results(self):
        eof_count = 0
        while eof_count < RESULT_FILES_AMOUNT:
            #logging.warning(f"Requesting result: {eof_count + 1}")
            flag = self.__handle_result()
            if flag == MESSAGE_FLAG['EOF']:
                eof_count += 1
        return True

    def __handle_result(self):
        protocol = MessageTransferProtocol(self.conn)
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
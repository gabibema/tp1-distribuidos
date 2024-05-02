from socket import SOCK_STREAM, socket, AF_INET, create_connection
from multiprocessing import Process, Queue
from time import time
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol
from uuid import uuid4

READ_MODE = 'r'

class Client:
    def __init__(self, config):
        self.port = config['port']
        self.batch_amount = config['batch_amount']
        
        self.books_path = config['books_path']
        self.reviews_path = config['reviews_path']

        self.uid = str(uuid4())

        self.books_queue = Queue()
        self.reviews_queue = Queue()
        self.books_sender = Process(target=self.__enqueue_file, args=(MESSAGE_FLAG['BOOK'], self.books_path, self.books_queue))
        self.reviews_sender = Process(target=self.__enqueue_file, args=(MESSAGE_FLAG['REVIEW'], self.reviews_path, self.reviews_queue))
        self.sender_process = Process(target=self.__send_from_queue, args=(self.books_queue, self.reviews_queue))


    def start(self):
        self.books_sender.start()
        self.reviews_sender.start()
        self.sender_process.start()
        
        self.books_sender.join()
        self.reviews_sender.join()
        self.sender_process.join()
    
    def __try_connect(self, host, port, timeout=15):
        actual_time = time()
        while time() - actual_time < timeout:
            try:
                conn = create_connection((host, port))
                return conn
            except:
                pass
        
        raise SystemError('Could not connect to the server')    # not handled at the moment
        
    def __enqueue_file(self, flag, path, queue):
        with open(path, READ_MODE) as file:
            headers = file.readline().strip()
            batch = [self.uid, headers]

            for line in file:
                batch.append(line.strip())
                if len(batch) >= self.batch_amount:
                    queue.put((flag, '\n'.join(batch)))
                    batch = [self.uid, headers]

            if batch:
                queue.put((flag, '\n'.join(batch)))

            # Signal EOF
            queue.put((flag, self.uid))

    def __sending_completed(self, books_queue: Queue, reviews_queue: Queue):
        return books_queue.empty() and reviews_queue.empty() and not self.books_sender.is_alive() and not self.reviews_sender.is_alive()

    def __send_from_queue(self, books_queue: Queue, reviews_queue: Queue):
        conn = self.__try_connect('gateway', self.port)
        protocol = TransferProtocol(conn)
        
        while True:
            if not books_queue.empty():
                flag, message = books_queue.get()
                protocol.send_message(message, flag)
            if not reviews_queue.empty():
                flag, message = reviews_queue.get()
                protocol.send_message(message, flag)
            
            if self.__sending_completed(books_queue, reviews_queue):
                break

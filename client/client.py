from queue import Queue
from socket import SOCK_STREAM, socket, AF_INET, create_connection
from threading import Thread
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol
from uuid import uuid4

BATCH_AMOUNT = 200

class Client:
    def __init__(self, config):
        self.port = config['port']
        self.conn = None
        
        self.books_path = config['books_path']
        self.ratings_path = config['ratings_path']

        self.uid = str(uuid4())
        self.sender_queue = Queue()
        self.file_sender = Thread(target=self.__send_files)
        self.books_reader = Thread(target=self.__read_file(MESSAGE_FLAG['BOOK']))
        self.ratings_reader = Thread(target=self.__read_file(MESSAGE_FLAG['RATING']))

    def start(self):
        self.__start_socket()
        self.file_sender.start()
        self.books_reader.start()
        self.ratings_reader.start()

    def __start_socket(self):
        self.conn = create_connection(('gateway', self.port))

    def __read_file(self, flag):
        path = self.ratings_path
        if flag == MESSAGE_FLAG['BOOK']:
            path = self.books_path
        

        with open(path, 'r') as file:
            headers = file.readline().strip()
            batch = [self.uid,headers]

            for line in file:
               batch.append(line.strip())
               if len(batch) >= BATCH_AMOUNT:
                    self.sender_queue.put((batch, flag))
                    batch = [self.uid,headers]
                   
            if batch:            
               self.sender_queue.put((batch, flag))

            self.sender_queue.put(([self.uid], flag))

        
    def __send_files(self):
        protocol = TransferProtocol(self.conn)
        while True:
            batch, flag = self.sender_queue.get()
            protocol.send_message('\n'.join(batch), flag)

            
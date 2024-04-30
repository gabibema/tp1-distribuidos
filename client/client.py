from queue import Queue
from socket import SOCK_STREAM, socket, AF_INET, create_connection
from multiprocessing import Process
from time import time
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol
from uuid import uuid4

READ_MODE = 'r'
BATCH_AMOUNT = 200

class Client:
    def __init__(self, config):
        self.port = config['port']
        
        self.books_path = config['books_path']
        self.ratings_path = config['ratings_path']

        self.uid = str(uuid4())
        self.books_sender = Process(target=self.__send_file(MESSAGE_FLAG['BOOK'], self.books_path))
        self.ratings_sender = Process(target=self.__send_file(MESSAGE_FLAG['RATING'], self.ratings_path))


    def start(self):
        self.books_sender.start()
        self.ratings_sender.start()
        self.books_sender.join()
        self.ratings_sender.join()
    
    def __try_connect(self, host, port, timeout=15):
        actual_time = time()
        while time() - actual_time < timeout:
            try:
                conn = create_connection((host, port))
                return conn
            except:
                pass
        
        raise SystemError('Could not connect to the server')    # not handled at the moment
        
    def __send_file(self, flag, path):
        conn = self.__try_connect('gateway', self.port)
        protocol = TransferProtocol(conn)

        with open(path, READ_MODE) as file:
            headers = file.readline().strip()
            batch = [self.uid,headers]

            for line in file:
               batch.append(line.strip())
               if len(batch) >= BATCH_AMOUNT:
                    protocol.send_message('\n'.join(batch), flag)
                    batch = [self.uid,headers]
                   
            if batch:            
               protocol.send_message('\n'.join(batch), flag)

            protocol.send_message(self.uid, flag) # empty message to signal EOF

            
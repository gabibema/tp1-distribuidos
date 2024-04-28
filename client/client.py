from socket import SOCK_STREAM, socket
from threading import Thread
from lib.transfer.transfer_protocol import MESSAGE_FLAG, TransferProtocol

BATCH_AMOUNT = 200

class Client:
    def __init__(self, config):
        self.books_path = config['books_path']
        self.ratings_path = config['ratings_path']
        
        self.port = config['port']
        self.conn = None

        self.books_sender = Thread(target=self.__send_books)
        self.ratings_sender = Thread(target=self.__send_ratings)

    def start(self):
        self.__start_socket()
        self.books_sender.start()
        self.ratings_sender.start()
        
    def __start_socket(self):
        self.conn = socket(SOCK_STREAM)
        self.conn.connect(('', self.port))

        
    def __send_books(self):
        protocol = TransferProtocol(self.conn)

        with open(self.books_path, 'r') as file:
            headers = file.readline().strip()
            batch = [headers]

            for line in file:
                batch.append(line.strip())
                if len(batch) >= BATCH_AMOUNT:
                    protocol.send_message('\n'.join(batch), MESSAGE_FLAG['BOOK'])
                    batch = [headers]
            if batch:            
                protocol.publish('\n'.join(batch), MESSAGE_FLAG['BOOK']) 


    def __send_ratings(self):

        protocol = TransferProtocol(self.conn)
        with open(self.ratings_path, 'r') as file:
            headers = file.readline().strip()
            batch = [headers]

            for line in file:
                batch.append(line.strip())
                if len(batch) >= BATCH_AMOUNT:
                    protocol.send_message('\n'.join(batch), MESSAGE_FLAG['RATING'])
                    batch = [headers]
            if batch:            
                protocol.publish('\n'.join(batch), MESSAGE_FLAG['RATING'])


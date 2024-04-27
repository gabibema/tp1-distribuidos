from threading import Thread
from lib.workers import wait_rabbitmq, Sender

BATCH_AMOUNT = 200

class Client:
    def __init__(self, config):
        self.books_path = config['books_path']
        self.ratings_path = config['ratings_path']
        
        self.books_queue = config['books_queue']
        self.ratings_queue = config['ratings_queue']

        self.books_sender = Thread(target=self.__send_books)
        self.ratings_sender = Thread(target=self.__send_ratings)

    def start(self):
        wait_rabbitmq()
        self.books_sender.start()
        self.ratings_sender.start()
        
    def __send_books(self):

        publisher = Sender('rabbitmq', self.books_queue)

        with open(self.books_path, 'r') as file:
            headers = file.readline().strip()
            batch = [headers]

            for line in file:
                batch.append(line.strip())
                if len(batch) >= BATCH_AMOUNT:
                    publisher.publish('\n'.join(batch))
                    batch = [headers]
            if batch:            
                publisher.publish('\n'.join(batch)) 



    def __send_ratings(self):

        rabbit = Sender('rabbitmq', self.ratings_queue)
        with open(self.ratings_path, 'r') as file:
            headers = file.readline().strip()
            batch = [headers]

            for line in file:
                batch.append(line.strip())
                if len(batch) >= BATCH_AMOUNT:
                    rabbit.publish('\n'.join(batch))
                    batch = [headers]
            if batch:            
                rabbit.publish('\n'.join(batch))


        rabbit.close()


from threading import Thread
from lib.utils import wait_rabbitmq, RabbitMQ, BOOKS_EXCHANGE, RATINGS_EXCHANGE
from pika.exceptions import AMQPError

BATCH_AMOUNT = 200

class Client:
    def __init__(self, config):
        self.books_path = config['books_path']
        self.ratings_path = config['ratings_path']
        
        self.books_queues = config['books_queues']
        self.ratings_queues = config['ratings_queues']

        self.books_sender = Thread(target=self.__send_books)
        #self.ratings_sender = Thread(target=self.__send_ratings)

    def start(self):
        wait_rabbitmq()
        self.books_sender.start()
        #self.ratings_sender.start()
        
    def __send_books(self):
        rabbit = RabbitMQ()
        for queue in self.books_queues:
            rabbit.create_queue(queue)

        with open(self.books_path, 'r') as f:
            batch = []
            for line in f:
                batch.append(line.strip())
                if len(batch) >= BATCH_AMOUNT:
                    print("Publishing batch")
                    rabbit.publish_in_queues("hello", queues=self.books_queues)
                    batch = []
            if batch:            
                rabbit.publish_in_queues("hello", queues=self.books_queues)

        
        rabbit.close()


    # def __send_ratings(self):
    #     rabbit = RabbitMQ()

    #     for queue in self.ratings_queues:
    #         rabbit.create_queue(queue)

    #     with open(self.ratings_path, 'r') as f:
    #         batch = []
    #         for line in f:
    #             batch.append(line.strip())
    #             if len(batch) >= BATCH_AMOUNT:
    #                 rabbit.publish_in_queues("hello", queues=self.ratings_queues)
    #             batch = []

    #         if batch:
    #             rabbit.publish_in_queues("hello", queues=self.ratings_queues)
        
    #     rabbit.close()


from threading import Thread
from lib.utils import wait_rabbitmq, RabbitMQ, BOOKS_EXCHANGE, RATINGS_EXCHANGE

BATCH_AMOUNT = 200

class Client:
    def __init__(self, config):
        self.books_path = config['books_path']
        self.ratings_path = config['ratings_path']
        
        self.books_queues = config['books_queues']
        self.ratings_queues = config['ratings_queues']

        self.books_sender = Thread(target=self.__send_books)
        self.ratings_sender = Thread(target=self.__send_ratings)

    def start(self):
        wait_rabbitmq()
        self.books_sender.start()
        self.ratings_sender.start()
        
        
    def __send_books(self):
        rabbit = RabbitMQ()
        exchange_name = BOOKS_EXCHANGE
        rabbit.create_exchange(exchange_name, 'direct')
        for queue in self.books_queues:
            rabbit.create_queue(queue)
            rabbit.channel.queue_bind(queue=queue, exchange=exchange_name, routing_key=queue)

        with open(self.books_path, 'r') as f:
            batch = []
            for line in f:
                batch.append(line.strip())
                if len(batch) >= BATCH_AMOUNT:
                    for msg in batch:
                        rabbit.publish(msg, exchange=exchange_name)
                    batch = []
            if batch:
                for msg in batch:
                    rabbit.publish(msg, exchange=exchange_name)
        
        rabbit.close()

    def __send_ratings(self):
        rabbit = RabbitMQ()
        exchange_name = RATINGS_EXCHANGE
        rabbit.create_exchange(exchange_name, 'direct')
        for queue in self.ratings_queues:
            rabbit.create_queue(queue)
            rabbit.channel.queue_bind(queue=queue, exchange=exchange_name, routing_key=queue)

        with open(self.ratings_path, 'r') as f:
            batch = []
            for line in f:
                batch.append(line.strip())
                if len(batch) >= BATCH_AMOUNT:
                    for msg in batch:
                        rabbit.publish(msg, exchange=exchange_name)
                    batch = []

            if batch:
                for msg in batch:
                    rabbit.publish(msg, exchange=exchange_name)
        
        rabbit.close()


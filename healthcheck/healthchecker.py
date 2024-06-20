from multiprocessing import Process
from time import sleep
from lib.broker import MessageBroker
from pika.exchange_type import ExchangeType
import logging

class HealthChecker:
    def __init__(self, config):
        self._health = True
        self._id = config['id']
        self.hostname = config['hostname']
        self.receiver = Process(target=self.__receive)
        self.sender = Process(target=self.__send)
        self.gather = Process(target=self.__gather)

    def start(self):
        self.receiver.start()
        self.sender.start()
        self.gather.start()

    def __init_connection(self):
        connection = MessageBroker(self.hostname)
        connection.create_router("health_bully", ExchangeType.fanout)
        result = connection.create_queue("", False)
        connection.link_queue(result.method.queue, "health_bully")
        connection.set_consumer(result.method.queue, self.__callback)
        return connection

    def __receive(self):
        connection = self.__init_connection()
        try:
            connection.begin_consuming()
        finally:
            connection.close_connection()

    def __send(self):
        connection = MessageBroker(self.hostname)
        connection.create_router("health_bully", ExchangeType.fanout)
        try:
            while True:
                connection.send_message("health_bully", "", str(self._id))
                print(f"Sent {self._id}")
                sleep(1)
        finally:
            connection.close_connection()

    def __gather(self):
        pass

    def __callback(self, ch, method, properties, body):
        logging.warning(f"Received {body}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
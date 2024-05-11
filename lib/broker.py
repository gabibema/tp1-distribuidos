from abc import ABC, abstractmethod
import pika

class BrokerConnection(ABC):
    @abstractmethod
    def __init__(self, hostname):
        pass

    @abstractmethod
    def create_queue(self, queue_name, persistent):
        pass

    @abstractmethod
    def create_router(self, router_name, router_type):
        pass

    @abstractmethod
    def link_queue(self, queue_name, router_name, routing_key):
        pass

    @abstractmethod
    def set_consumer(self, queue_name, callback):
        pass

    @abstractmethod
    def send_message(self, router_name, routing_key, message):
        pass

    @abstractmethod
    def begin_consuming(self):
        pass

    @abstractmethod
    def acknowledge_message(self, message_id):
        pass

class RabbitMQConnection(BrokerConnection):
    def __init__(self, hostname):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname))
        self.channel = self.connection.channel()

    def create_queue(self, queue_name, persistent):
        self.channel.queue_declare(queue=queue_name, durable=persistent)

    def create_router(self, router_name, router_type):
        self.channel.exchange_declare(exchange=router_name, exchange_type=router_type)

    def link_queue(self, queue_name, router_name, routing_key):
        self.channel.queue_bind(queue=queue_name, exchange=router_name, routing_key=routing_key)

    def set_consumer(self, queue_name, callback):
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback)

    def send_message(self, router_name, routing_key, message):
        self.channel.basic_publish(exchange=router_name, routing_key=routing_key, body=message)

    def begin_consuming(self):
        self.channel.start_consuming()

    def acknowledge_message(self, message_id):
        self.channel.basic_ack(delivery_tag=message_id)
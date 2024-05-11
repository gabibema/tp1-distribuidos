from abc import ABC, abstractmethod

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
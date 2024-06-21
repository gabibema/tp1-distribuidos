from multiprocessing import Process, Manager, Event
from multiprocessing.managers import ListProxy
from time import sleep
from lib.broker import MessageBroker
from pika.exchange_type import ExchangeType
import logging

class PeerStorage:
    def __init__(self):
        self.manager = Manager()
        self.peers = self.manager.list()
        self.event = self.manager.Event()

    def add_peer(self, peer_id):
        self.peers.append(peer_id)
        self.event.set()  # Activar el evento cuando se añade un peer

    def peer_exists(self, peer_id):
        return peer_id in self.peers

    def get_peers(self):
        return self.peers

    def delete_peers(self):
        self.peers = self.manager.list()  # Reemplazar con una nueva lista

    def wait_for_peers(self):
        self.event.wait()  # Esperar hasta que se active el evento
        self.event.clear()  # Limpiar el evento para futuras esperas


class HealthChecker:
    def __init__(self, config):
        self.health = True
        self.id = config['id']
        self.hostname = config['hostname']
        self.stored_peers = PeerStorage()
        self.receiver = Process(target=self.__receive, args=(self.stored_peers,))
        self.sender = Process(target=self.__send, args=(self.stored_peers,))
        self.gather = Process(target=self.__gather, args=(self.stored_peers,))

    def start(self):
        try: 
            self.receiver.start()
            self.sender.start()
            self.gather.start()
        except Exception as e:
            logging.error(f"Error starting health checker: {e}")
        finally:
            self.receiver.join()
            self.sender.join()
            self.gather.join()

    def __init_connection(self, stored_peers):
        connection = MessageBroker(self.hostname)
        connection.create_router("health_bully", ExchangeType.fanout)
        result = connection.create_queue("", False)
        connection.link_queue(result.method.queue, "health_bully")

        connection_sender = MessageBroker(self.hostname)
        connection_sender.create_router("health_bully", ExchangeType.fanout)
        connection.set_consumer(result.method.queue, lambda ch, method, properties, body: self.__callback(ch, method, properties, body, connection_sender))
        return connection

    def __receive(self, stored_peers: ListProxy):
        connection = self.__init_connection(stored_peers)
        try:
            connection.begin_consuming()
        finally:
            connection.close_connection()

    def __send(self, stored_peers: PeerStorage):
        connection = MessageBroker(self.hostname)
        connection.create_router("health_bully", ExchangeType.fanout)
        try:
            while True:
                logging.warning(f"Already in stored_peers: {list(self.stored_peers.get_peers())}")
                connection.send_message("health_bully", "", str(self.id))
                logging.warning(f"Sent {self.id}") 
                sleep(5)
                peers = list(self.stored_peers.get_peers())
                logging.warning(f"Peers: {peers}")
                if len(peers) and self.id == max(peers):
                    logging.warning(f"Health i'm the bully")
                
                self.stored_peers.delete_peers()
                logging.warning(f"Deleted peers: {list(self.stored_peers.get_peers())}")
                sleep(30)
                

        finally:
            connection.close_connection()

    def __gather(self, stored_peers: PeerStorage):
        pass

    def __callback(self, ch, method, properties, body, connection):
        logging.warning(f"Received {body}")
        if not self.stored_peers.peer_exists(self.id):
            logging.warning(f"Sending {self.id} because {body} is not in stored_peers")
            connection.send_message("health_bully", "", str(self.id))
            self.stored_peers.add_peer(self.id)
        body = int(body)
        if not self.stored_peers.peer_exists(body):
            logging.warning(f"Adding {body}")
            self.stored_peers.add_peer(body)
            
        ch.basic_ack(delivery_tag=method.delivery_tag)

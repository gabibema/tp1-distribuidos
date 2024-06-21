from multiprocessing import Process, Manager, Event
from multiprocessing.managers import ListProxy
from time import sleep
from lib.broker import MessageBroker
from pika.exchange_type import ExchangeType
import logging

LEADER_ID = 1000000

class PeerStorage:
    def __init__(self):
        self.manager = Manager()
        self.peers = self.manager.dict()

    def add_peer(self, peer_id):
        self.peers[peer_id] = True

    def peer_exists(self, peer_id):
        return peer_id in self.get_peers()

    def get_peers(self):
        return [peer for peer in self.peers.keys() if self.peers[peer]]

    def delete_peers(self):
        for peer in self.peers.keys():
            self.peers[peer] = False


class Bully:
    def __init__(self, config):
        self.manager = Manager()
        self.health = True
        self.id = self.manager.Value('i', config['id'])
        self.hostname = config['hostname']
        self.stored_peers = PeerStorage()
        self.receiver = Process(target=self.__receive, args=(self.stored_peers,))
        self.sender = Process(target=self.__send, args=(self.stored_peers,))

    def start(self):
        try: 
            self.receiver.start()
            self.sender.start()
        except Exception as e:
            logging.error(f"Error starting health checker: {e}")
        finally:
            self.receiver.join()
            self.sender.join()

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
                #logging.warning(f"Already in stored_peers: {list(self.stored_peers.get_peers())}")
                connection.send_message("health_bully", "", str(self.id.value))
                #logging.warning(f"Sent {self.id.value}") 
                sleep(5)
                peers = list(self.stored_peers.get_peers())
                #logging.warning(f"Peers: {peers}")
                if len(peers) and self.id.value == max(peers):
                    #logging.warning(f"I'm the bully: {self.id.value}")
                    self.id.value = LEADER_ID
                
                self.stored_peers.delete_peers()
                sleep(30)
        finally:
            connection.close_connection()

    def __callback(self, ch, method, properties, body, connection):
        if not self.stored_peers.peer_exists(self.id.value):
            #logging.warning(f"Sending {self.id.value} because i'm not in stored_peers")
            connection.send_message("health_bully", "", str(self.id.value))
            self.stored_peers.add_peer(self.id.value)
        body = int(body)
        if not self.stored_peers.peer_exists(body):
            #logging.warning(f"Adding {body}")
            self.stored_peers.add_peer(body)
            
        ch.basic_ack(delivery_tag=method.delivery_tag)


class HealthChecker:
    def __init__(self, config):
        self.bully = Bully(config)

    def start(self):
        self.bully.start()
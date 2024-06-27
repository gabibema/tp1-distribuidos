from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process, Manager, Event
import socket
from time import sleep
from lib.broker import MessageBroker
from pika.exchange_type import ExchangeType
from lib.healthcheck import HEALTHCHECK_PORT, Healthcheck, HEALTH
import docker
import logging
import signal

EXCLUDED_SERVICES = ['rabbitmq', "client"]
HEALTHCHECK_FREQUENCY = 15
HEALTHCHECK_TIMEOUT = 5

def signal_handler(sig, frame):
    logging.warning(f"System shutdown received {sig}.")
    raise SystemExit("System shutdown.")
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

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
    def __init__(self, config, bully_leader_event, leadership_event):
        self.manager = Manager()
        self.id = self.manager.Value('i', config['id'])
        self.hostname = config['hostname']
        self.stored_peers = PeerStorage()
        self.receiver = Process(target=self.__receive)
        self.sender = Process(target=self.__send, args=(bully_leader_event, leadership_event))

    def start(self):
        try:
            self.receiver.start()
            self.sender.start()
        except Exception as e:
            HEALTH.set_broken()
            logging.error(f"Error starting health checker: {e}")
        finally:
            self.receiver.join()
            self.sender.join()

    def __init_connection(self):
        connection = MessageBroker(self.hostname)
        connection.create_router("health_bully", ExchangeType.fanout)
        result = connection.create_queue("", False, True)
        connection.link_queue(result.method.queue, "health_bully")

        connection_sender = MessageBroker(self.hostname)
        connection_sender.create_router("health_bully", ExchangeType.fanout)
        connection.set_consumer(result.method.queue, lambda ch, method, properties, body: self.__callback(ch, method, properties, body, connection_sender))
        return connection, connection_sender

    def __receive(self):
        connection, connection_aux = self.__init_connection()
        try:
            connection.begin_consuming()
        except:
            logging.error(f"Leaving the bully process.")
        finally:
            connection_aux.close_connection()
            connection.close_connection()

    def __send(self, leader_event, leadership_event):
        connection = MessageBroker(self.hostname)
        connection.create_router("health_bully", ExchangeType.fanout)
        try:
            while True:
                connection.send_message("health_bully", "", str(self.id.value))
                sleep(HEALTHCHECK_TIMEOUT)
                peers = list(self.stored_peers.get_peers())

                if self.__is_new_leader(peers):
                    leader_event.set()
                    if not leadership_event.is_set():
                        leadership_event.set()
                else:
                    if leadership_event.is_set():
                        leadership_event.clear()

                self.stored_peers.delete_peers()
                sleep(10)
        except:
            logging.error(f"Leaving the bully process.")
        finally:
            connection.close_connection()

    def __is_new_leader(self, peers):
        return len(peers) and self.id.value == max(peers)

    def __callback(self, ch, method, properties, body, connection):
        if not self.stored_peers.peer_exists(self.id.value):
            connection.send_message("health_bully", "", str(self.id.value))
            self.stored_peers.add_peer(self.id.value)
        body = int(body)

        if not self.stored_peers.peer_exists(body):
            self.stored_peers.add_peer(body)

        ch.basic_ack(delivery_tag=method.delivery_tag)
    
class HealthChecker:
    def __init__(self, config):
        self.bully_leader_event = Event()
        self.leadership_event = Event()
        self.bully = Bully(config, self.bully_leader_event, self.leadership_event)
        self.checker = Process(target=self.__healthcheck)
        self.healthchecker = Process(target=Healthcheck().listen_healthchecks)

    def start(self):
        try:
            self.healthchecker.start()
            self.checker.start()
            self.bully.start()
        except Exception as e:
            HEALTH.set_broken()
            logging.error(f"Error starting health checker: {e}")
        finally:
            self.healthchecker.join()
            self.checker.join()

    def __healthcheck(self):
        client = docker.DockerClient()
        try:
            self.bully_leader_event.wait()
            logging.warning(f"Healthchecker started with id: {self.bully.id.value}")
            while True:
                sleep(HEALTHCHECK_FREQUENCY)
                self.leadership_event.wait()
                self.check_containers(client, "tp1-distribuidos")
        except:
            logging.error(f"Leaving the healthchecker process.")
        finally:
            client.close()

    def check_containers(self, client: docker.DockerClient, project_name):
        containers = client.containers.list(filters={"label": f"com.docker.compose.project={project_name}"}, all=True)
        for container in containers:
            if any(service in container.name for service in EXCLUDED_SERVICES):
                continue
            self.check_container(container)


    def check_container(self, container):
        if container.status != 'running':
            logging.warning(f"Restarting container {container.name}.")
            self.start_container(container)
        health_status = self.get_healthcheck_message(container)
        if not health_status:
            logging.warning(f"Restarting container {container.name}.")
            self.start_container(container)

    def start_container(self, container):
        try:
            container.restart(timeout=1)
        except Exception as e:
            logging.error(f"Failed to start container {container.name}: {str(e)}")

    def get_healthcheck_message(self, container):
        try:
            ip_address = container.attrs['NetworkSettings']['Networks']['tp1-distribuidos_default']['IPAddress']
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(HEALTHCHECK_TIMEOUT)
                s.connect((ip_address, HEALTHCHECK_PORT))
                response = s.recv(1, )
                return int.from_bytes(response, byteorder='big')
        except Exception as e:
            logging.error(f"Error connecting to container {container.name}: {str(e)}")
            return 0

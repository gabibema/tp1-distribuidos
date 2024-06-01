from abc import ABC, abstractmethod
from time import sleep, time
from uuid import uuid4
import json
import pika

class MessageBroker():
    def __init__(self, hostname):
        self.wait_connection()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname))
        self.next_peer = None # Placeholder until the leader tells us who our next peer is.
        self.channel = self.connection.channel()

    def create_queue(self, queue_name, persistent):
        self.channel.queue_declare(queue=queue_name, durable=persistent)

    def create_control_queue(self, queue_prefix, callback):
        self.id = str(uuid4())
        try:
            # Acquire leader lock.
            self.channel.queue_declare(queue=queue_prefix + '_lock', exclusive=True)
            # This process is the leader.
            queue_name = queue_prefix + '_leader'
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.next_peer = queue_name
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
        except pika.exceptions.ChannelClosedByBroker:
            # RESOURCE_LOCKED - someone else is already the leader.
            queue_name = queue_prefix + '_' + self.id
            # open a new channel.
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            # Announce itself to the leader.
            message = {'type': 'NEW_PEER', 'peer_name': queue_name}
            self.send_message('', queue_prefix + '_leader', json.dumps(message))

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
    
    def wait_connection(self, host='rabbitmq', timeout=120, interval=10):
        """
        Waits for RabbitMQ to be available.
        """
        start_time = time()
        
        while time() - start_time < timeout:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
                connection.close()
                return
            except pika.exceptions.AMQPConnectionError:
                sleep(interval)

        raise TimeoutError("RabbitMQ did not become available within the timeout period.")

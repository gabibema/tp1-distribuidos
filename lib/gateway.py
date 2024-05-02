from io import StringIO
from csv import DictReader
import json
import pika
import logging
from .workers import wait_rabbitmq

MAX_KEY_LENGTH = 255

class BookPublisher():
    def __init__(self, rabbit_hostname, dst_exchange, dst_exchange_type):
        wait_rabbitmq()
        # init RabbitMQ channel
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_hostname))
        self.channel = connection.channel()
        self.channel.confirm_delivery()
        # init destination exchange
        self.exchange = dst_exchange
        self.channel.exchange_declare(exchange=dst_exchange, exchange_type=dst_exchange_type)

    def publish(self, message, routing_key_fn):
        csv_stream = StringIO(message)
        uid = csv_stream.readline().strip()
        reader = DictReader(csv_stream)

        processed_row = False
        for row in reader:
            processed_row = True
            row['request_id'] = uid
            routing_key = routing_key_fn(row)
            self.channel.basic_publish(exchange=self.exchange, routing_key=routing_key, body=json.dumps(row))
        if not processed_row:
            eof_msg = json.dumps({'request_id': uid, 'type': 'EOF'})
            logging.warning(f'{eof_msg}')
            self.channel.basic_publish(exchange=self.exchange, routing_key='EOF', body=eof_msg)


class ReviewPublisher():
    def __init__(self, rabbit_hostname):
        wait_rabbitmq()
        # init RabbitMQ channel
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_hostname))
        self.channel = connection.channel()
        self.channel.confirm_delivery()

    def publish(self, message, routing_key):
        csv_stream = StringIO(message)
        uid = csv_stream.readline().strip()
        reader = DictReader(csv_stream)

        processed_row = False
        for row in reader:
            processed_row = True
            row['request_id'] = uid
            self.channel.basic_publish(exchange='', routing_key=routing_key, body=json.dumps(row))
        if not processed_row:
            eof_msg = json.dumps({'request_id': uid, 'type': 'EOF'})
            logging.warning(f'{eof_msg}')
            self.channel.basic_publish(exchange='', routing_key=routing_key, body=eof_msg)

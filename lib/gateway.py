from io import StringIO
from csv import DictReader
import logging
import json
import pika
import logging
from .workers import wait_rabbitmq
from pika.exchange_type import ExchangeType

MAX_KEY_LENGTH = 255
NOT_EOF_VALUE = 0
EOF_VALUE = 1

class BookPublisher():
    def __init__(self, rabbit_hostname, dst_exchange, dst_exchange_type):

        # init RabbitMQ channel
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_hostname))
        self.channel = self.connection.channel()
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
            self.channel.basic_publish(exchange=self.exchange, routing_key='EOF', body=json.dumps({'request_id': uid, 'type': 'EOF'}))
        else: 
            logging.warning(f'Received message of length {len(message)}')
        
        return EOF_VALUE if not processed_row else NOT_EOF_VALUE
    
    def close(self):
        self.channel.close()
        self.connection.close()


class ReviewPublisher():
    def __init__(self, rabbit_hostname):
        # init RabbitMQ channel
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_hostname))
        self.channel = self.connection.channel()

    def publish(self, message, routing_key):
        csv_stream = StringIO(message)
        uid = csv_stream.readline().strip()
        reader = DictReader(csv_stream)
        rows = [{'request_id': uid, **row} for row in reader]
        if not rows:
            eof_msg = json.dumps({'request_id': uid, 'type': 'EOF'})
            logging.warning(f'{eof_msg}')
            self.channel.basic_publish(exchange='', routing_key=routing_key, body=eof_msg)
        else:
            full_message = json.dumps(rows)
            self.channel.basic_publish(exchange='', routing_key=routing_key, body=full_message)
            logging.warning(f'Received message of length {len(message)}')
        
        return EOF_VALUE if not rows else NOT_EOF_VALUE
    
    def close(self):
        self.channel.close()
        self.connection.close()


class ResultReceiver():
    def __init__(self, rabbit_hostname, queues, callback, callback_arg):
        # init RabbitMQ channel
        self.callback = callback
        self.callback_arg = callback_arg
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_hostname))
        self.channel = self.connection.channel()
        # init source queue and bind to exchange
        for queue_name in queues:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=lambda ch, method, properties, body, q=queue_name: callback(ch, method, properties, body, q, callback_arg),
            auto_ack=True
        )
        self.channel.exchange_declare('popular_90s_exchange', exchange_type=ExchangeType.direct)
        self.channel.queue_bind('popular_90s_books', 'popular_90s_exchange', routing_key='popular_90s_queue')
    
    def start(self):
        self.channel.start_consuming()
    
    def close(self):
        self.channel.close()
        self.connection.close()
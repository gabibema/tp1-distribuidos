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

        eof_received = NOT_EOF_VALUE
        for row in reader:
            row_filtered = {}
            row_filtered['request_id'] = uid
            if 'type' in row:
                eof_received = EOF_VALUE if 'EOF' in row['type'] else NOT_EOF_VALUE
                row_filtered['type'] = row['type']
            if 'Title' in row:
                row_filtered['Title'] = row['Title']
            if 'publishedDate' in row:
                row_filtered['publishedDate'] = row['publishedDate']
            if 'categories' in row:
                row_filtered['categories'] = row['categories']
            if 'authors' in row:
                row_filtered['authors'] = row['authors']
            
            routing_key = routing_key_fn(row_filtered)
            self.channel.basic_publish(exchange=self.exchange, routing_key=routing_key, body=json.dumps(row_filtered))

        if eof_received == EOF_VALUE:
            logging.warning(f'{uid} EOF received')
        else: 
            logging.warning(f'Received message of length {len(message)}')
        
        return eof_received
    
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

        eof_received = NOT_EOF_VALUE
        reader = DictReader(csv_stream)
        rows = []
        for row in reader:
            current_row = {'request_id': uid}
            if 'Title' in row:
                current_row['Title'] = row['Title']
            if 'review/text' in row:
                current_row['review/text'] = row['review/text']
            if 'type' in row:
                current_row['type'] = row['type']
            if len(current_row) > 1:
                rows.append(current_row)
    
        if len(rows) == 1 and 'type' in rows[0] and 'EOF' in rows[0]['type']:
            eof_received = EOF_VALUE
            logging.warning(f'{uid} EOF received')
            
        else:
            logging.warning(f'Received message of length {len(message)}')
        
        full_message = json.dumps(rows)
        self.channel.basic_publish(exchange='', routing_key=routing_key, body=full_message)
        return eof_received
    
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
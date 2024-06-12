from io import StringIO
from csv import DictReader
import logging
import json
import uuid

MAX_KEY_LENGTH = 255

class BookPublisher():
    def __init__(self, connection, dst_exchange, dst_exchange_type):
        self.connection = connection
        self.exchange = dst_exchange
        self.connection.create_router(dst_exchange, dst_exchange_type)

    def publish(self, client_id, message_id, message_csv, routing_key_fn):
        eof_received = False
        reader = DictReader(StringIO(message_csv))
        rows = []
        for row in reader:
            if row.get('type') == 'EOF':
                logging.warning(f'{client_id} EOF received')
                row = {'type': 'EOF'}
                eof_received = True
            else:
                row = {'Title': row['Title'], 'publishedDate': row['publishedDate'], 'categories': row['categories'], 'authors': row['authors']}
            routing_key = routing_key_fn(row)
            rows.append(row)
        batch_message = {'request_id': str(client_id), 'message_id': message_id, 'items': rows}
        self.connection.send_message(self.exchange, routing_key, json.dumps(batch_message))
        return eof_received

    def close(self):
        self.connection.channel.close()
        self.connection.connection.close()

class ReviewPublisher():
    def __init__(self, connection):
        self.connection = connection

    def publish(self, client_id, message_id, message_csv, routing_key):
        eof_received = False
        reader = DictReader(StringIO(message_csv))
        rows = []
        for row in reader:
            if row.get('type') == 'EOF':
                logging.warning(f'{client_id} EOF received')
                row = {'type': 'EOF'}
                eof_received = True
            else:
                row = {'Title': row['Title'], 'review/text': row['review/text']}
            rows.append(row)
        batch_message = {'request_id': str(client_id), 'message_id': message_id, 'items': rows}
        self.connection.send_message('', routing_key, json.dumps(batch_message))
        return eof_received

    def close(self):
        self.connection.channel.close()
        self.connection.connection.close()

class ResultReceiver():
    def __init__(self, connection, queues, callback, callback_arg):
        self.connection = connection
        self.callback = callback
        self.callback_arg = callback_arg
        self.id = uuid.uuid4()
        for queue_name in queues:
            self.connection.create_queue(queue_name, True)
            self.connection.set_consumer(
                queue_name,
                lambda ch, method, properties, body, q=queue_name: callback(self, ch, method, properties, body, q, callback_arg)
            )
        self.connection.create_router('popular_90s_exchange', 'direct')
        self.connection.link_queue('popular_90s_books', 'popular_90s_exchange', 'popular_90s_queue')
    
    def start(self):
        self.connection.begin_consuming()
    
    def close(self):
        self.connection.channel.close()
        self.connection.connection.close()

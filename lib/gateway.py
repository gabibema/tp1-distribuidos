from io import StringIO
from csv import DictReader
import logging
import json
import uuid

from lib.transfer.transfer_protocol import MESSAGE_FLAG

MAX_KEY_LENGTH = 255

class BookPublisher():
    def __init__(self, connection, dst_exchange, dst_exchange_type, data_saver):
        self.data_saver = data_saver
        self.connection = connection
        self.exchange = dst_exchange
        self.connection.create_router(dst_exchange, dst_exchange_type)

    def publish(self, client_id, message_id, message, routing_key_fn):
        eof_received = False
        last_row = None
        reader = DictReader(StringIO(message))

        for row in reader:
            last_row = row
            row_filtered = {'request_id': str(client_id), 'message_id': message_id}
            if 'type' in row:
                eof_received |= 'EOF' in row['type']
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
            self.connection.send_message(self.exchange, routing_key, json.dumps(row_filtered))

        if eof_received:
            logging.warning(f'{client_id} EOF received')
        else:
            logging.warning(f'Received message of length {len(message)}')

        if last_row:
            message = {'request_id': str(client_id), 'message_id': message_id, 'source': MESSAGE_FLAG['BOOK'], 'eof': eof_received}
            self.data_saver.save_message_to_json(message)
        return eof_received
    
    def close(self):
        self.connection.channel.close()
        self.connection.connection.close()

class ReviewPublisher():
    def __init__(self, connection, data_saver):
        self.data_saver = data_saver
        self.connection = connection

    def publish(self, client_id, message_id, message, routing_key):
        eof_received = False
        reader = DictReader(StringIO(message))
        rows = []
        for row in reader:
            current_row = {'request_id': str(client_id), 'message_id': message_id}
            if 'Title' in row:
                current_row['Title'] = row['Title']
            if 'review/text' in row:
                current_row['review/text'] = row['review/text']
            if 'type' in row:
                current_row['type'] = row['type']
            if len(current_row) > 1:
                rows.append(current_row)
    
        if len(rows) == 1 and rows[0].get('type') == 'EOF':
            eof_received = True
            logging.warning(f'{client_id} EOF received')
        else:
            logging.warning(f'Received message of length {len(message)}') 
        full_message = json.dumps(rows)
        self.connection.send_message('', routing_key, full_message)
        if rows:
            message = {'request_id': str(client_id), 'message_id': message_id, 'source': MESSAGE_FLAG['REVIEW'], 'eof': eof_received}
            self.data_saver.save_message_to_json(message)
        return eof_received
    
    def close(self):
        self.connection.channel.close()
        self.connection.connection.close()

class ResultReceiver():
    def __init__(self, connection, queues, callback, callback_arg1, callback_arg2, eof_count=0):
        self.connection = connection
        self.callback = callback
        self.callback_arg1 = callback_arg1
        self.callback_arg2 = callback_arg2
        self.eof_count = eof_count
        for queue_name in queues:
            self.connection.create_queue(queue_name, True)
            self.connection.set_consumer(
                queue_name,
                lambda ch, method, properties, body, q=queue_name: callback(self, ch, method, properties, body, q, callback_arg1, callback_arg2, self.eof_count)
            )
        self.connection.create_router('popular_90s_exchange', 'direct')
        self.connection.link_queue('popular_90s_books', 'popular_90s_exchange', 'popular_90s_queue')
    
    def start(self, eof_count=0):
        self.eof_count += eof_count
        logging.warning(f'Starting result receiver with eof_count {self.eof_count}')
        self.connection.begin_consuming()
    
    def close(self):
        self.connection.channel.close()
        self.connection.connection.close()

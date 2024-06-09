from io import StringIO
from csv import DictReader
import logging
import json

MAX_KEY_LENGTH = 255
NOT_EOF_VALUE = 0
EOF_VALUE = 1

class BookPublisher():
    def __init__(self, connection, dst_exchange, dst_exchange_type, data_saver):
        self.data_saver = data_saver
        self.connection = connection
        self.exchange = dst_exchange
        self.connection.create_router(dst_exchange, dst_exchange_type)

    def publish(self, message, routing_key_fn):
        csv_stream = StringIO(message)
        uid = csv_stream.readline().strip()
        reader = DictReader(csv_stream)

        last_row = None
        eof_received = NOT_EOF_VALUE
        for row in reader:
            last_row = row
            row_filtered = {'request_id': uid}
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
            self.connection.send_message(self.exchange, routing_key, json.dumps(row_filtered))

        if eof_received == EOF_VALUE:
            logging.warning(f'{uid} EOF received')
        else: 
            logging.warning(f'Received message of length {len(message)}')
        if last_row:
            self.data_saver.save_message_to_json(uid, last_row)
        return eof_received
    
    def close(self):
        self.connection.channel.close()
        self.connection.connection.close()

class ReviewPublisher():
    def __init__(self, connection, data_saver):
        self.data_saver = data_saver
        self.connection = connection

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
        self.connection.send_message('', routing_key, full_message)
        if rows:
            self.data_saver.save_message_to_json(uid, rows[-1])
        return eof_received
    
    def close(self):
        self.connection.channel.close()
        self.connection.connection.close()

class ResultReceiver():
    def __init__(self, connection, queues, callback, callback_arg):
        self.connection = connection
        self.callback = callback
        self.callback_arg = callback_arg
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

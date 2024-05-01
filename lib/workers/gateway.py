from io import StringIO
from csv import DictReader
import json
from .workers import Worker


class Proxy(Worker):
    def __init__(self, rabbit_hostname, exchanges: dict):
        self.new(rabbit_hostname)
        self.exchanges = exchanges

    def publish(self, message, exchange):
        csv_stream = StringIO(message)
        uid = csv_stream.readline().strip()
        reader = DictReader(csv_stream)

        processed_row = False
        for row in reader:
            processed_row = True
            row['request_id'] = uid
            routing_key = self.get_keys(row, exchange)
            self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=json.dumps(row))
        if not processed_row:
            # Pending: this logic only applies to book messages. Reviews should be sent to the predefined reviews routing_key
            self.channel.basic_publish(exchange=exchange, routing_key='EOF', body=json.dumps({'request_id': uid, 'type': 'EOF'}))

    def get_keys(self, row, exchange):
        if self.exchanges[exchange]:
            return self.exchanges[exchange](row)
        return ''
    
    def callback(self, ch, method, properties, body: bytes):
        pass

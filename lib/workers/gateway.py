from csv import DictReader
from io import StringIO
from json import dumps
from .workers import Worker


class Proxy(Worker):
    def __init__(self, rabbit_hostname, exchanges: dict):
        self.new(rabbit_hostname)
        self.exchanges = exchanges

    def publish(self, message, exchange):
        self.check_pending_messages()
        csv_stream = StringIO(message)
        uid = csv_stream.readline().strip()
        reader = DictReader(csv_stream)

        processed_row = False
        for row in reader:
            processed_row = True
            row['request_id'] = uid
            keys = self.get_keys(row, exchange)
            self.try_publish(exchange, keys, dumps(row))
        
        if not processed_row:
            self.try_publish(exchange, 'eof', dumps({'request_id': uid, 'type': 'EOF'}))


    def get_keys(self, row, exchange):
        if self.exchanges[exchange]:
            return self.exchanges[exchange](row)
        return ''
    
    def callback(self, ch, method, properties, body: bytes):
        pass

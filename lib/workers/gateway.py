from csv import DictReader
from io import StringIO
from json import dumps
from .workers import Worker

class Sender(Worker):
    def __init__(self, rabbit_hostname, dst_queue):
        self.new(rabbit_hostname, dst_routing_key=dst_queue)
        self.channel.queue_declare(queue=dst_queue, durable=True)
    
    def callback(self, ch, method, properties, body):
        pass

    def publish(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.routing_key, body=message)

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
            keys = self.get_keys(row, exchange)
            self.channel.basic_publish(exchange=exchange, routing_key=keys, body=dumps(row))
        
        if not processed_row:
            self.channel.basic_publish(exchange=exchange, routing_key='eof', body=dumps({'request_id': uid}))


    def get_keys(self, row, exchange):
        if self.exchanges[exchange]:
            return self.exchanges[exchange](row)
        return ''
    
    def callback(self, ch, method, properties, body: bytes):
        pass

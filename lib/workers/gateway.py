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
        reader = DictReader(csv_stream)

        for row in reader:
            keys = self.get_keys(row, exchange)
            self.channel.basic_publish(exchange=exchange, routing_key=keys, body=dumps(row))

    def get_keys(self, row, exchange):
        if self.exchanges[exchange]:
            return self.exchanges[exchange](row)
        return ''
    
    def callback(self, ch, method, properties, body: bytes):
        pass

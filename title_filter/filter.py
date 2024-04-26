# filter.py
from lib.utils import RabbitMQ

class MessageFilter:
    def __init__(self, queue_name):
        self.rabbit = RabbitMQ()
        self.queue_name = queue_name
        self.ensure_queue_exists()

    def ensure_queue_exists(self):
        """Ensures that the queue exists before starting to consume."""
        self.rabbit.create_queue(self.queue_name)


    def process_message(self, ch, method, properties, body):
        """Callback function to process messages."""
        print(f"Received message: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def start_consuming(self):
        """Starts consuming messages from the specified queue."""
        self.rabbit.consume(self.queue_name, self.process_message)


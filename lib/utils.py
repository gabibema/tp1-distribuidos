import pika
import time

WAIT_TIME_PIKA = 15
PERSISTENT_DELIVERY_MODE = 2
BOOKS_EXCHANGE = 'books_exchange'
RATINGS_EXCHANGE = 'ratings_exchange'

class RabbitMQ:
    def __init__(self, host='rabbitmq'):
        """Initialize the RabbitMQ connection with a host."""
        self.host = host
        self.connection = None
        self.channel = None
        self.setup()


    def setup(self):
        """Establishes connection and creates a channel."""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()


    def create_queue(self, queue):
        """Creates a queue."""
        self.channel.queue_declare(queue=queue, durable=True)


    def create_exchange(self, exchange, exchange_type):
        """Creates an exchange of a given type."""
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)


    def create_bind_queue(self, exchange, keys=[]):
        """Creates an anonymous durable queue and binds it to routing keys on an exchange."""
        result = self.channel.queue_declare(queue='', durable=True)
        queue_name = result.method.queue

        if not keys:
            self.channel.queue_bind(exchange=exchange, queue=queue_name)
        for key in keys:
            self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=key)

        return queue_name


    def publish(self, body, queue='', exchange=''):
        """Sends a message to a specified queue or exchange."""
        self.channel.basic_publish(
            exchange=exchange, 
            routing_key=queue, 
            body=body,
            properties=pika.BasicProperties(delivery_mode=PERSISTENT_DELIVERY_MODE)
        )

    def publish_in_queues(self, body, queues=[], exchange=''):
        """Publishes a message to multiple queues."""
        print(f"Publishing message to {queues}...")
        for queue in queues:
            self.publish(body, queue, exchange)


    def consume(self, queue, cb):
        """Consumes messages from a queue using a callback function."""
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=queue, on_message_callback=cb, auto_ack=True)
        self.channel.start_consuming()

    def close(self):
        """Closes the channel and connection."""
        if self.channel:
            self.channel.close()
        if self.connection:
            self.connection.close()


def wait_rabbitmq():
    """Pauses execution for few seconds in order start rabbitmq broker."""
    time.sleep(WAIT_TIME_PIKA)




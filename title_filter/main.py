from filter import MessageFilter
from lib.utils import wait_rabbitmq

def main():
    queue_name = 'computer_queue'
    wait_rabbitmq()
    message_filter = MessageFilter(queue_name)
    
    print(f"Starting to consume messages from {queue_name}...")
    message_filter.start_consuming()


if __name__ == '__main__':
    main()



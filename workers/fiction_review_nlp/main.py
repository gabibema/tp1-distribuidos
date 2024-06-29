import logging
from lib.broker import MessageBroker
from lib.workers import Map
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def sentiment(review):
    score = SentimentIntensityAnalyzer().polarity_scores(review['review/text'])['compound']
    return {'Title': review['Title'], 'score': score}

def main():
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_queue = f'fiction_rev_shard{shard_id}_queue'
    dst_exchange = 'nlp_revs_exchange'
    dst_routing_key = f'nlp_revs_shard{shard_id}'
    control_queue_prefix = 'ctrl_fiction_review_nlp'
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    connection = MessageBroker(rabbit_hostname)
    worker = Map(sentiment, control_queue_prefix, connection=connection, src_queue=src_queue, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

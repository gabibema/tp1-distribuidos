import json
from lib.broker import RabbitMQConnection
from lib.workers import Map
from nltk.sentiment.vader import SentimentIntensityAnalyzer
    
def sentiment(body):
    review = json.loads(body)
    score = SentimentIntensityAnalyzer().polarity_scores(review['review/text'])['compound']
    return json.dumps({'request_id': review['request_id'], 'Title': review['Title'], 'score': score})

def main():
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_queue = f'fiction_rev_shard{shard_id}_queue'
    dst_exchange = 'nlp_revs_exchange'
    dst_routing_key = f'nlp_revs_shard{shard_id}'
    connection = RabbitMQConnection(rabbit_hostname)
    worker = Map(sentiment, connection=connection, src_queue=src_queue, dst_exchange=dst_exchange, dst_routing_key=dst_routing_key)
    worker.start()

if __name__ == '__main__':
    main()

from lib.workers import Map
from nltk.sentiment.vader import SentimentIntensityAnalyzer
    
def sentiment(message):
    review = json.loads(message)
    score = SentimentIntensityAnalyzer().polarity_scores(review['review/text']).compound
    return json.dumps({'Title': review['Title'], 'score': score})

def main():
    # Pending: move variables to env.
    shard_id = 0
    rabbit_hostname = 'rabbitmq'
    src_queue = 'fiction_rev_queue'
    src_exchange = 'fiction_rev_exchange'
    src_routing_key = f'fiction_rev_shard{shard_id}'
    dst_exchange = 'nlp_revs_exchange'
    dst_routing_key = f'nlp_revs_shard{shard_id}'
    worker = Map(sentiment, rabbit_hostname, src_queue, src_exchange, dst_exchange=dst_exchange)
    worker.start()

if __name__ == '__main__':
    main()

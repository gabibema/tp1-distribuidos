from lib.workers import Map
from nltk.sentiment.vader import SentimentIntensityAnalyzer
    
def sentiment(message):
    review = json.loads(message)
    score = SentimentIntensityAnalyzer().polarity_scores(review['review/text']).compound
    return json.dumps({'Title': review['Title'], 'score': score})

def main():
    # Pending: move variables to env.
    rabbit_hostname = 'rabbitmq'
    src_queue = 'fiction_rev_queue'
    src_exchange = 'fiction_rev_exchange'
    dst_exchange = 'nlp_revs_exchange'
    worker = Map(sentiment, rabbit_hostname, src_queue, src_exchange, dst_exchange=dst_exchange)
    worker.start()

if __name__ == '__main__':
    main()

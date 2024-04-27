from lib.workers import Map
from nltk.sentiment.vader import SentimentIntensityAnalyzer
    
def sentiment(message):
    review = json.loads(message)
    score = SentimentIntensityAnalyzer().polarity_scores(review['review/text']).compound
    return json.dumps({'Title': review['Title'], 'score': score})

def main():
    rabbit_hostname = 'localhost'
    src_queue = 'fiction_rev_q'
    dst_queue = 'nlp_title_q'
    worker = Map(rabbit_hostname, src_queue, dst_queue, sentiment)
    worker.start()

if __name__ == '__main__':
    main()


FROM rabbitmq-python-base
WORKDIR /app
RUN python3 -m pip install nltk
RUN python3 -c "import nltk; nltk.download('vader_lexicon')"
COPY ./workers/fiction_review_nlp /app
COPY ./lib /app/lib

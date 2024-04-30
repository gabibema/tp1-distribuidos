
FROM rabbitmq-python-base
WORKDIR /app
RUN python3 -m pip install nltk
COPY ./workers/fiction_review_nlp /app
COPY ./lib /app/lib

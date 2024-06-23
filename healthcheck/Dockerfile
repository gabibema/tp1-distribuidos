FROM rabbitmq-python-base
RUN pip install docker
WORKDIR /app
COPY ./healthcheck /app
COPY ./lib /app/lib

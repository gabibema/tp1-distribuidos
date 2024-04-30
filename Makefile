SHELL := /bin/bash
PWD := $(shell pwd)

all:

docker-image:
	docker build -f ./base-image/Dockerfile -t "rabbitmq-python-base:latest" .
	docker build -f ./rabbitmq/Dockerfile -t "rabbitmq:latest" .
	docker build -f ./gateway/Dockerfile -t "gateway:latest" .

.PHONY: docker-image

docker-image-client:
	docker build -f ./client/Dockerfile -t "client:latest" .
.PHONY: docker-image-client

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-client: docker-image-client 
	docker compose -f docker-compose-client.yaml up -d --build
.PHONY: docker-compose-client

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-down-client:
	docker compose -f docker-compose-client.yaml stop -t 1
	docker compose -f docker-compose-client.yaml down
.PHONY: docker-compose-down-client

docker-compose-down-all:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
	docker compose -f docker-compose-client.yaml stop -t 1
	docker compose -f docker-compose-client.yaml down
.PHONY: docker-compose-down-all

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

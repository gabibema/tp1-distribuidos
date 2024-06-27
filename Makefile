SHELL := /bin/bash
PWD := $(shell pwd)

all:

docker-image:
	docker build -f ./base-image/Dockerfile -t "rabbitmq-python-base:latest" .
	docker build -f ./rabbitmq/Dockerfile -t "rabbitmq:latest" .
	docker build -f ./gateway/Dockerfile -t "gateway:latest" .
	docker build -f ./workers/90s_reviews_filter/Dockerfile -t "90s_reviews_filter:latest" .
	docker build -f ./workers/90s_title_barrier/Dockerfile -t "90s_title_barrier:latest" .
	docker build -f ./workers/90s_title_sharder/Dockerfile -t "90s_title_sharder:latest" .
	docker build -f ./workers/90s_top10_filter/Dockerfile -t "90s_top10_filter:latest" .
	docker build -f ./workers/author_decades_filter/Dockerfile -t "author_decades_filter:latest" .
	docker build -f ./workers/author_sharder/Dockerfile -t "author_sharder:latest" .
	docker build -f ./workers/computer_books_filter/Dockerfile -t "computer_books_filter:latest" .
	docker build -f ./workers/count_90s_revs_by_title/Dockerfile -t "count_90s_revs_by_title:latest" .
	docker build -f ./workers/fiction_avg_nlp_by_title/Dockerfile -t "fiction_avg_nlp_by_title:latest" .
	docker build -f ./workers/fiction_percentile_calculator/Dockerfile -t "fiction_percentile_calculator:latest" .
	docker build -f ./workers/fiction_percentile_filter/Dockerfile -t "fiction_percentile_filter:latest" .
	docker build -f ./workers/fiction_review_nlp/Dockerfile -t "fiction_review_nlp:latest" .
	docker build -f ./workers/fiction_category_filter/Dockerfile -t "fiction_category_filter:latest" .
	docker build -f ./workers/fiction_reviews_filter/Dockerfile -t "fiction_reviews_filter:latest" .
	docker build -f ./workers/fiction_title_barrier/Dockerfile -t "fiction_title_barrier:latest" .
	docker build -f ./workers/fiction_title_sharder/Dockerfile -t "fiction_title_sharder:latest" .
	docker build -f ./workers/90s_category_filter/Dockerfile -t "90s_category_filter:latest" .
	docker build -f ./workers/title_sharder/Dockerfile -t "title_sharder:latest" .


.PHONY: docker-image

docker-image-client:
	docker build -f ./client/Dockerfile -t "client:latest" .
.PHONY: docker-image-client

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-client: docker-image-client
	rm -r data/*/results || true
	docker compose -f docker-compose-client.yaml up -d --build
.PHONY: docker-compose-client

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
	rm -r gateway/backup || true
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

write-clients:
	@python3 docker-compose-writer.py $(N)
	@python3 batch-creator-clients.py
.PHONY: write-clients
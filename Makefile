SHELL := /bin/bash
PY ?= python3

CORE := kafka1 kafka2 kafka3 elasticsearch
ARMS := logstash-kafka logstash-direct logstash-direct-pq

.PHONY: help up down bootstrap smoke bench-all report clean-results reset ps logs

help:
	@echo "make up            build images, start Kafka + Elasticsearch, install template/topic"
	@echo "make smoke         short end-to-end check of all three arms (~2 min)"
	@echo "make bench-all     run the full scenario suite (~35 min)"
	@echo "make report        rebuild docs/report/ from results/"
	@echo "make down          stop everything and delete volumes"

up:
	docker compose build bench
	docker compose up -d $(CORE)
	# Arm containers are created but left stopped: scenarios start exactly the
	# one they are measuring, so the arms never compete for the same machine.
	docker compose create $(ARMS)
	docker compose run --rm -T bench python -u bench/bootstrap.py

bootstrap:
	docker compose run --rm -T bench python -u bench/bootstrap.py

smoke:
	$(PY) scenarios/00_smoke.py

# Capacity runs first on purpose: the failure scenarios are only meaningful at a
# rate every arm can sustain while healthy.
bench-all:
	$(PY) scenarios/00_capacity.py
	$(PY) scenarios/01_sink_outage.py
	$(PY) scenarios/01b_broker_failure.py
	$(PY) scenarios/01c_collector_outage.py
	$(PY) scenarios/02_burst.py
	$(PY) scenarios/03_latency.py
	$(PY) scenarios/04_scale_out.py
	$(PY) scenarios/05_dedup.py
	$(PY) scenarios/06_compression.py
	$(MAKE) report

report:
	docker compose run --rm -T bench python -u bench/report.py

ps:
	docker compose ps

logs:
	docker compose logs --tail=100 $(SVC)

clean-results:
	rm -rf results/*/ docs/report/

reset:
	docker compose down -v
	$(MAKE) up

down:
	docker compose down -v

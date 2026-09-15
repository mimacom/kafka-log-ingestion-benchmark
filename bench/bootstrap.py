"""Install the Elasticsearch template and ingest pipeline, and create the Kafka
topic. Idempotent: `make up` runs it every time."""

from __future__ import annotations

import argparse
import json
import os
import sys

from common import REPO_ROOT, es, es_ready

STACK = REPO_ROOT / "stack"


def setup_elasticsearch():
    if not es_ready(timeout=180):
        sys.exit("Elasticsearch never became ready")

    pipeline = json.loads((STACK / "elasticsearch" / "ingest-pipeline.json").read_text())
    status, body = es("PUT", "/_ingest/pipeline/bench-ingest-ts", pipeline)
    if status not in (200, 201):
        sys.exit(f"ingest pipeline install failed: {body}")

    template = json.loads((STACK / "elasticsearch" / "index-template.json").read_text())
    status, body = es("PUT", "/_index_template/bench-logs", template)
    if status not in (200, 201):
        sys.exit(f"index template install failed: {body}")

    print("elasticsearch: ingest pipeline and index template installed")


def setup_kafka(topic, partitions, replication):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]})
    existing = admin.list_topics(timeout=30).topics

    if topic in existing:
        have = len(existing[topic].partitions)
        print(f"kafka: topic {topic!r} already exists with {have} partitions")
        return

    futures = admin.create_topics([
        NewTopic(topic, num_partitions=partitions, replication_factor=replication)
    ])
    for name, future in futures.items():
        future.result()
        print(f"kafka: created topic {name!r} "
              f"partitions={partitions} replication={replication}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "logs"))
    parser.add_argument("--partitions", type=int,
                        default=int(os.environ.get("KAFKA_PARTITIONS", "6")))
    parser.add_argument("--replication", type=int,
                        default=int(os.environ.get("KAFKA_REPLICATION", "1")))
    parser.add_argument("--skip-kafka", action="store_true")
    args = parser.parse_args()

    setup_elasticsearch()
    if not args.skip_kafka:
        setup_kafka(args.topic, args.partitions, args.replication)


if __name__ == "__main__":
    main()

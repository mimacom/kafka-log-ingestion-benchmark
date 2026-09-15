"""Topic maintenance between runs.

Scenarios recreate the topic rather than reusing it: leftover records from a
previous run would be replayed by the next consumer group and counted as
duplicates that the pipeline never actually produced.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic


def recreate(admin, topic, partitions, replication, min_isr=None):
    existing = admin.list_topics(timeout=30).topics
    if topic in existing:
        for _, future in admin.delete_topics([topic], operation_timeout=60).items():
            future.result()
        # Deletion is asynchronous; recreating too early silently reuses the
        # old topic with its old records still in place.
        for _ in range(60):
            if topic not in admin.list_topics(timeout=20).topics:
                break
            time.sleep(1)
        else:
            sys.exit(f"topic {topic!r} was not deleted in time")

    for _ in range(30):
        try:
            config = {}
            if min_isr:
                # With acks=all this is what makes a write durable against the
                # loss of a broker: it is only acknowledged once a second
                # replica holds it.
                config["min.insync.replicas"] = str(min_isr)
            for _, future in admin.create_topics([
                NewTopic(topic, num_partitions=partitions,
                         replication_factor=replication, config=config)
            ]).items():
                future.result()
            break
        except Exception:
            time.sleep(1)
    else:
        sys.exit(f"could not recreate topic {topic!r}")

    print(f"kafka: topic {topic!r} recreated partitions={partitions} "
          f"replication={replication} min_isr={min_isr or 1}")


def describe(admin, topic):
    meta = admin.list_topics(topic, timeout=30).topics.get(topic)
    if meta is None:
        return {}
    return {
        "partitions": len(meta.partitions),
        "replicas": {str(p): len(info.replicas) for p, info in meta.partitions.items()},
        "isr": {str(p): len(info.isrs) for p, info in meta.partitions.items()},
    }


def log_size_bytes(bootstrap, topic):
    """Bytes the topic occupies on the brokers, used by the compression run."""
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({"bootstrap.servers": bootstrap,
                         "group.id": "size-probe", "enable.auto.commit": False})
    meta = consumer.list_topics(topic, timeout=20)
    total = 0
    for partition in meta.topics[topic].partitions:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10, cached=False)
        total += max(0, high - low)
    consumer.close()
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "logs"))
    parser.add_argument("--partitions", type=int,
                        default=int(os.environ.get("KAFKA_PARTITIONS", "6")))
    parser.add_argument("--replication", type=int,
                        default=int(os.environ.get("KAFKA_REPLICATION", "1")))
    parser.add_argument("--min-isr", type=int, default=None)
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--describe", action="store_true")
    # Written to a file rather than parsed off stdout: `docker compose run`
    # interleaves its own container lines with the program's output.
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]})
    if args.recreate:
        recreate(admin, args.topic, args.partitions, args.replication,
                 args.min_isr)
    if args.describe:
        info = describe(admin, args.topic)
        print(json.dumps(info, indent=2))
        if args.out:
            with open(args.out, "w") as handle:
                json.dump(info, handle, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())

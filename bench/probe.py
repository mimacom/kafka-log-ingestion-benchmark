"""Samples the pipeline from the outside while a scenario runs.

Two things are worth watching live and cannot be reconstructed afterwards:
how much data is queued but not yet consumed (Kafka lag), and how far behind
*searchable* Elasticsearch is. The second matters because indexing latency and
search visibility are different numbers -- the refresh interval sits between
them -- and conflating the two is the usual way end-to-end latency claims go
wrong.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from common import INDEX, es


def kafka_lag(topic, group, bootstrap):
    """Committed lag across all partitions of the topic, or None if Kafka is
    not part of the arm under test."""
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError:
        return None
    try:
        # Same group id as the real consumer, but this client never subscribes,
        # so it reads the group's committed offsets without joining it and
        # without triggering a rebalance of the pipeline under measurement.
        consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
        })
        meta = consumer.list_topics(topic, timeout=10)
        if topic not in meta.topics or meta.topics[topic].error is not None:
            consumer.close()
            return None
        partitions = [TopicPartition(topic, p) for p in meta.topics[topic].partitions]

        committed = consumer.committed(partitions, timeout=15)
        offsets = {tp.partition: tp.offset for tp in committed}

        total = 0
        for tp in partitions:
            _, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            offset = offsets.get(tp.partition)
            # -1001 is "no committed offset yet": nothing consumed, so the
            # whole log is lag.
            if offset is None or offset < 0:
                offset = 0
            total += max(0, high - offset)
        consumer.close()
        return total
    except Exception:
        return None


def searchable_state(index):
    """What search can currently see: document count, highest seq, and the age
    of the newest visible document.

    That last one is the honest measure of search lag. Inferring it from a
    sequence number means extrapolating from the offered rate, which breaks as
    soon as the rate varies or a warm-up pass has left higher sequence numbers
    in the index. Comparing the newest visible ingest timestamp against the
    clock needs neither assumption.
    """
    try:
        status, body = es("POST", f"/{index}/_search", {
            "size": 0,
            "track_total_hits": True,
            "aggs": {
                "max_seq": {"max": {"field": "seq"}},
                "max_ingest": {"max": {"field": "ingest_ts"}},
            },
        }, timeout=15)
    except Exception:
        # Elasticsearch being unreachable is a valid observation during an
        # outage scenario, not a probe failure.
        return None, None, None
    if status != 200:
        return None, None, None
    hits = body["hits"]["total"]["value"]
    peak = body["aggregations"]["max_seq"]["value"]
    newest = body["aggregations"]["max_ingest"]["value"]
    age_ms = None
    if newest is not None:
        age_ms = round(time.time() * 1000 - newest, 1)
    return hits, (int(peak) if peak is not None else None), age_ms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--index", default=INDEX)
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "logs"))
    parser.add_argument("--group", default=os.environ.get("KAFKA_GROUP", "logstash"))
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka1:9092"))
    parser.add_argument("--with-kafka", action="store_true",
                        help="sample consumer lag as well (Kafka arm only)")
    args = parser.parse_args()

    samples = []
    start = time.time()
    deadline = start + args.duration
    while time.time() < deadline:
        tick = time.time()
        docs, max_seq, age_ms = searchable_state(args.index)
        sample = {
            "t": round(tick - start, 2),
            "searchable_docs": docs,
            "max_searchable_seq": max_seq,
            "newest_searchable_age_ms": age_ms,
        }
        if args.with_kafka:
            sample["kafka_lag"] = kafka_lag(args.topic, args.group, args.bootstrap)
        samples.append(sample)

        # Written as the run goes, so a scenario that kills a container still
        # leaves usable evidence behind -- and written atomically, because the
        # collector kills this process and would otherwise read a half-written
        # file.
        partial = args.out + ".partial"
        with open(partial, "w") as handle:
            json.dump({"interval": args.interval, "samples": samples}, handle)
        os.replace(partial, args.out)

        sleep = args.interval - (time.time() - tick)
        if sleep > 0:
            time.sleep(sleep)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Scenario 06 -- compression, in transit and at rest.

Two independent measurements on the same data.

In transit: the same events are produced to Kafka four times, once per codec,
and the resulting topic size on the brokers' disks is read back. That number is
both the network cost and the buffer's storage cost, since a Kafka topic stores
exactly what was sent.

At rest: the same documents are held in Elasticsearch under the default codec
and under best_compression, force-merged so that segment counts do not confuse
the comparison.

Codec choice is a real trade: the cheapest bytes are not the cheapest CPU, so
producer throughput is reported next to the size.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

SCENARIO = "06-compression"
CODECS = ["none", "gzip", "lz4", "zstd"]
# Uncapped: at a fixed rate every codec simply hits the rate and the throughput
# column says nothing. Running flat out is what makes the CPU cost of a codec
# visible next to the bytes it saves.
RATE = 250000
SECONDS = 30
GENERATORS = 4
BEST_INDEX = "bench-logs-bestcomp"


def topic_bytes(topic="logs"):
    """Bytes the topic occupies across all brokers, read from Kafka itself."""
    raw = L.C.docker(
        "exec", f"{L.C.COMPOSE_PROJECT}-kafka1", "/opt/kafka/bin/kafka-log-dirs.sh",
        "--describe", "--bootstrap-server", "kafka1:9092",
        "--topic-list", topic, check=False)
    line = next((l for l in raw.splitlines() if l.startswith("{")), None)
    if not line:
        return None
    data = json.loads(line)
    total = 0
    for broker in data.get("brokers", []):
        for log_dir in broker.get("logDirs", []):
            for partition in log_dir.get("partitions", []):
                if partition["partition"].rsplit("-", 1)[0] == topic:
                    total += partition["size"]
    return total


def measure_codec(codec):
    L.log(f"--- producing with compression.type={codec} ---")
    L.C.stop_all_arms()   # nothing consumes: the topic must keep everything
    L.reset_kafka_topic(partitions=6, replication=1)

    started = time.time()
    gen = L.run_generator("kafka", run_id=f"{SCENARIO}-{codec}",
                          out_name="comp-gen.json", rate=RATE, duration=SECONDS,
                          generators=GENERATORS, compression=codec,
                          buffer=500000, drain_seconds=180)
    elapsed = time.time() - started
    time.sleep(5)

    size = topic_bytes()
    raw = gen["delivered"] * gen["payload_bytes"]
    result = {
        "codec": codec,
        "events": gen["delivered"],
        "nominal_payload_bytes": raw,
        "topic_bytes": size,
        # Kept for reference, but the headline ratio is computed against the
        # measured `none` run instead -- see ratio_vs_none below.
        "ratio_vs_nominal_payload": round(raw / size, 2) if size else None,
        "produce_seconds": round(elapsed, 1),
        "events_per_s": round(gen["delivered"] / elapsed, 1) if elapsed else None,
        "bytes_per_event": round(size / gen["delivered"], 1) if size and gen["delivered"] else None,
    }
    L.log(f"  {codec}: topic={size / 1e6:.1f}MB "
          f"({result['bytes_per_event']} B/event) "
          f"at {result['events_per_s']} events/s")
    return result


def measure_storage_codec():
    """Same documents, two Elasticsearch codecs."""
    L.log("--- Elasticsearch storage codecs ---")
    L.C.recreate_index()
    L.reset_kafka_topic(partitions=6, replication=1)
    L.start_arm("kafka")
    L.warm_up("kafka", seconds=30)
    gen = L.run_generator("kafka", run_id=f"{SCENARIO}-storage",
                          out_name="comp-store-gen.json", rate=8000, duration=60,
                          generators=4, drain_seconds=180)

    for _ in range(90):
        L.C.es_refresh()
        if L.C.es_count() >= gen["delivered"]:
            break
        time.sleep(2)
    L.stop_arm("kafka")

    L.C.es("DELETE", f"/{BEST_INDEX}")
    status, body = L.C.es("PUT", f"/{BEST_INDEX}", {
        "settings": {"number_of_shards": 3, "number_of_replicas": 0,
                     "index.codec": "best_compression",
                     "index.default_pipeline": "_none"},
    })
    if status not in (200, 201):
        raise RuntimeError(f"could not create {BEST_INDEX}: {body}")

    L.log("reindexing into the best_compression index")
    status, body = L.C.es("POST", "/_reindex?wait_for_completion=true", {
        "source": {"index": L.C.INDEX}, "dest": {"index": BEST_INDEX},
    }, timeout=1800)
    if status != 200:
        raise RuntimeError(f"reindex failed: {body}")

    for index in (L.C.INDEX, BEST_INDEX):
        L.C.es("POST", f"/{index}/_forcemerge?max_num_segments=1", timeout=900)
    time.sleep(5)
    L.C.es_refresh(L.C.INDEX)
    L.C.es_refresh(BEST_INDEX)

    default_bytes = L.C.index_size_bytes(L.C.INDEX)
    best_bytes = L.C.index_size_bytes(BEST_INDEX)
    docs = L.C.es_count(L.C.INDEX)
    L.log(f"  default={default_bytes / 1e6:.1f}MB "
          f"best_compression={best_bytes / 1e6:.1f}MB over {docs} docs")

    L.C.es("DELETE", f"/{BEST_INDEX}")
    return {
        "docs": docs,
        "default_codec_bytes": default_bytes,
        "best_compression_bytes": best_bytes,
        "bytes_per_doc_default": round(default_bytes / docs, 1) if docs else None,
        "bytes_per_doc_best": round(best_bytes / docs, 1) if docs else None,
        "pct_saved": (round(100.0 * (default_bytes - best_bytes) / default_bytes, 1)
                      if default_bytes else None),
    }


def add_ratio_vs_none(transport):
    """Ratios against the uncompressed run that was actually measured."""
    baseline = next((t["bytes_per_event"] for t in transport
                     if t["codec"] == "none" and t["bytes_per_event"]), None)
    for entry in transport:
        entry["ratio_vs_none"] = (
            round(baseline / entry["bytes_per_event"], 2)
            if baseline and entry["bytes_per_event"] else None)
    return transport


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-storage", action="store_true")
    args = parser.parse_args()

    transport = add_ratio_vs_none([measure_codec(codec) for codec in CODECS])

    if args.skip_storage:
        # Keep a storage measurement from an earlier run rather than dropping it
        # from the report; the two halves are independent.
        previous = L.C.RESULTS_DIR / SCENARIO / "kafka" / "run.json"
        storage = (json.loads(previous.read_text()).get("storage")
                   if previous.exists() else None)
        if storage:
            L.log("reusing the storage-codec measurement from the previous run")
    else:
        storage = measure_storage_codec()

    L.finish(SCENARIO, "kafka", {
        "config": {"rate": RATE, "seconds": SECONDS, "generators": GENERATORS,
                   "codecs": CODECS},
        "transport": transport,
        "storage": storage,
    })
    return 0


if __name__ == "__main__":
    sys.exit(main())

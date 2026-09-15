#!/usr/bin/env python3
"""Scenario 04 -- does adding consumers actually add throughput?

The architecture claims capacity is added by adding partitions and consumers.
That claim is only worth making if it measures.

A fixed backlog is loaded into a six-partition topic, and then drained by 1, 2
and 4 consumers in turn. Draining a fixed backlog rather than following a live
producer matters: it makes the consumers, not the generator, the thing being
measured.

Elasticsearch is replaced by a counting sink. With Elasticsearch on the end,
every consumer count would converge on Elasticsearch's indexing rate and the
answer would be about Elasticsearch instead. The trade-off is that this
measures the transport tier's ceiling, not the platform's -- which is exactly
the question asked here.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

SCENARIO = "04-scale-out"
PARTITIONS = 6
CONSUMER_COUNTS = [1, 2, 4]
PRELOAD_RATE = 200000     # uncapped in practice: the generator runs flat out
PRELOAD_SECONDS = 40
SINK = "http://localhost:8201/"
NULL_CONSUMER = f"{L.C.COMPOSE_PROJECT}-ls-null"


def sink_stats():
    status, body = L.C.request("GET", SINK, timeout=10)
    return body if status == 200 else {}


def sink_reset():
    L.C.request("GET", SINK + "reset", timeout=10)


def preload():
    """Fill the topic once; every consumer count then drains the same data."""
    L.C.stop_all_arms()
    L.C.docker("rm", "-f", NULL_CONSUMER, check=False)
    L.reset_kafka_topic(partitions=PARTITIONS, replication=1)
    L.log(f"preloading the topic (up to {PRELOAD_SECONDS}s at full speed)")
    gen = L.run_generator("kafka", run_id=f"{SCENARIO}-preload",
                          out_name="scale-gen.json", rate=PRELOAD_RATE,
                          duration=PRELOAD_SECONDS, generators=6,
                          buffer=500000, drain_seconds=180)
    L.log(f"backlog: {gen['delivered']} events")
    return gen


def drain_with(consumers, backlog):
    """Start N consumers in one group and time the drain of the backlog."""
    group = f"scale-{consumers}"
    L.C.docker("rm", "-f", NULL_CONSUMER, check=False)
    sink_reset()

    L.log(f"draining with {consumers} consumer thread(s)")
    L.C.compose("--profile", "null-sink", "up", "-d", "logstash-kafka-null",
                env={"CONSUMER_THREADS": consumers, "KAFKA_GROUP": group})

    started = time.time()
    last_events, idle = 0, 0
    stats = {}
    while time.time() - started < 900:
        time.sleep(2)
        stats = sink_stats()
        events = stats.get("events", 0)
        if events >= backlog:
            break
        if events == last_events:
            idle += 1
            # Two minutes without a single event means the consumers are not
            # slow, they are stuck, and the run is not worth waiting out.
            if idle > 60 and events > 0:
                break
        else:
            idle = 0
        last_events = events

    stats = sink_stats()
    events = stats.get("events", 0)
    span = (stats.get("last_ts") or 0) - (stats.get("first_ts") or 0)
    throughput = round(events / span, 1) if span > 0 else None
    L.log(f"  {consumers} consumer(s): {events} events in {span:.1f}s "
          f"= {throughput} events/s")

    L.C.docker("rm", "-f", NULL_CONSUMER, check=False)
    return {"consumers": consumers, "events": events,
            "seconds": round(span, 2), "events_per_s": throughput,
            "complete": events >= backlog}


def main():
    parser = argparse.ArgumentParser()
    parser.parse_args()

    L.C.compose("--profile", "null-sink", "up", "-d", "sink")
    time.sleep(3)

    gen = preload()
    backlog = gen["delivered"]

    runs = []
    for consumers in CONSUMER_COUNTS:
        runs.append(drain_with(consumers, backlog))

    baseline = next((r["events_per_s"] for r in runs if r["consumers"] == 1), None)
    for run in runs:
        run["speedup_vs_1"] = (round(run["events_per_s"] / baseline, 2)
                               if baseline and run["events_per_s"] else None)

    L.finish(SCENARIO, "kafka", {
        "config": {"partitions": PARTITIONS, "consumer_counts": CONSUMER_COUNTS,
                   "sink": "counting HTTP sink (Elasticsearch removed)"},
        "backlog_events": backlog,
        "preload": {k: v for k, v in gen.items() if k != "rate_series"},
        "runs": runs,
    })

    L.C.compose("--profile", "null-sink", "stop", "sink", check=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())

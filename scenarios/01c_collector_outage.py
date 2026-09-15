#!/usr/bin/env python3
"""Scenario 01c -- the collector tier restarts.

Scenario 01 takes Elasticsearch away. This one takes away the component the
sources actually talk to, which is a different question and, for this
architecture decision, the more important one.

With a broker in front, Logstash is a consumer: stopping it stops consumption,
the topic grows, and nothing is lost, because the sources were never talking to
Logstash in the first place. Without a broker, Logstash *is* the endpoint, so
while it is down the sources have nowhere to send. That is the decoupling
argument, and it is the one a persisted queue cannot answer: a queue on a node
that is not running accepts nothing.

A Logstash restart is not an exotic failure. It is what a version upgrade, a
pipeline config change or a node reboot looks like from the sources' side.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

SCENARIO = "01c-collector-outage"
RATE = 5000
SECONDS = 300
OUTAGE_AT = 60
OUTAGE_SECONDS = 90
GENERATORS = 4
SOURCE_BUFFER = 200_000


def drain(timeout=300):
    stable, previous = 0, -1
    deadline = time.time() + timeout
    while time.time() < deadline:
        L.C.es_refresh()
        count = L.C.es_count()
        if count == previous:
            stable += 1
            if stable >= 4:
                return count
        else:
            stable = 0
        previous = count
        time.sleep(2)
    return previous


def run_arm(arm):
    L.log(f"=== {SCENARIO}: {arm} ===")
    L.reset_pipeline_state(arm)
    L.start_arm(arm)
    L.warm_up(arm)

    container = L.C.ARM_CONTAINERS[arm]
    L.start_probe("col-probe.json", duration=SECONDS + 180,
                  with_kafka=(arm == "kafka"), interval=1.0)
    L.start_generator_detached(arm, run_id=f"{SCENARIO}-{arm}",
                               out_name="col-gen.json", rate=RATE,
                               duration=SECONDS, generators=GENERATORS,
                               buffer=SOURCE_BUFFER, drain_seconds=240)

    time.sleep(OUTAGE_AT)
    L.log(f"stopping the collector ({container})")
    stopped_at = time.time()
    L.C.stop_container(container, timeout=20)

    time.sleep(OUTAGE_SECONDS)
    L.log("starting the collector again")
    L.C.start_container(container)
    L.logstash_ready(arm, timeout=240)
    recovered_at = time.time()
    L.log(f"collector back after {recovered_at - stopped_at:.0f}s")

    gen = L.wait_generator("col-gen.json", timeout=1800)
    indexed = drain()
    probe = L.collect_probe("col-probe.json")
    result = L.run_verify("col-gen.json", "col-verify.json")

    L.log(f"produced={gen['produced']} dropped_at_source={gen['dropped_at_source']} "
          f"indexed={result['indexed_unique']} missing={result['missing']} "
          f"({result['loss_pct']}%)")

    L.finish(SCENARIO, arm, {
        "config": {"rate": RATE, "seconds": SECONDS, "generators": GENERATORS,
                   "outage_at_s": OUTAGE_AT, "outage_seconds": OUTAGE_SECONDS,
                   "source_buffer_events": SOURCE_BUFFER},
        "outage": {"component": container,
                   "stopped_at_ms": int(stopped_at * 1000),
                   "recovered_at_ms": int(recovered_at * 1000),
                   "actual_seconds": round(recovered_at - stopped_at, 1)},
        "generator": gen,
        "verify": result,
        "probe": probe,
        "logstash_events": L.logstash_events(arm),
        "es_docs_after_drain": indexed,
    })
    L.stop_arm(arm)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--arm", action="append", choices=L.ARMS)
    args = parser.parse_args()
    for arm in (args.arm or L.ARMS):
        run_arm(arm)
    return 0


if __name__ == "__main__":
    sys.exit(main())

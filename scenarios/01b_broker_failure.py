#!/usr/bin/env python3
"""Scenario 01b -- a Kafka broker dies mid-ingest.

Adding a broker to an architecture adds something that can itself fail, and
that objection deserves a measurement rather than a reassurance. With
replication factor 3, min.insync.replicas 2, acks=all and idempotent
producers, losing one of three brokers should cost a brief producer stall
while leadership moves, and no events.

Applies to the Kafka arm only: the other two arms have no broker to kill.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

SCENARIO = "01b-broker-failure"
RATE = 5000
SECONDS = 240
KILL_AT = 60
DOWN_SECONDS = 90
GENERATORS = 4
VICTIM = f"{L.C.COMPOSE_PROJECT}-kafka2"


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


def stall_seconds(gen):
    """Seconds where delivery dropped below a tenth of the offered rate."""
    target = gen["phases"][0]["rate"]
    return sum(1 for s in gen.get("rate_series", [])
               if s["delivered_per_s"] < target * 0.1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--replication", type=int, default=3)
    args = parser.parse_args()

    arm = "kafka"
    L.log(f"=== {SCENARIO}: replication factor {args.replication} ===")
    L.C.stop_all_arms()
    L.C.recreate_index()
    # min.insync.replicas=2 means a write is acknowledged only once a second
    # broker holds it, which is what makes the loss of one broker survivable.
    L.reset_kafka_topic(partitions=6, replication=args.replication, min_isr=2)
    L.C.run_bench("bench/kafka_admin.py", "--describe",
                  "--out", "results/tmp/topic-before.json")
    L.start_arm(arm)
    L.warm_up(arm)

    L.start_probe("brk-probe.json", duration=SECONDS + 180, with_kafka=True)
    L.start_generator_detached(arm, run_id=f"{SCENARIO}", out_name="brk-gen.json",
                               rate=RATE, duration=SECONDS, generators=GENERATORS,
                               drain_seconds=240)

    time.sleep(KILL_AT)
    L.log(f"killing broker {VICTIM}")
    killed_at = time.time()
    L.C.docker("kill", VICTIM)

    time.sleep(DOWN_SECONDS)
    L.log(f"restarting broker {VICTIM}")
    L.C.start_container(VICTIM)
    L.C.wait_container_healthy(VICTIM, timeout=240)
    recovered_at = time.time()
    L.log(f"broker back after {recovered_at - killed_at:.0f}s")

    gen = L.wait_generator("brk-gen.json", timeout=1800)
    indexed = drain()
    probe = L.collect_probe("brk-probe.json")
    result = L.run_verify("brk-gen.json", "brk-verify.json")
    L.C.run_bench("bench/kafka_admin.py", "--describe",
                  "--out", "results/tmp/topic-after.json")
    topic = json.loads(L.tmp_path("topic-after.json").read_text())

    L.log(f"produced={gen['produced']} dropped={gen['dropped_at_source']} "
          f"errors={gen['delivery_errors']} indexed={result['indexed_unique']} "
          f"missing={result['missing']} ({result['loss_pct']}%)")

    L.finish(SCENARIO, arm, {
        "config": {"rate": RATE, "seconds": SECONDS, "generators": GENERATORS,
                   "replication": args.replication, "victim": VICTIM,
                   "down_seconds": DOWN_SECONDS},
        "failure": {"killed_at_ms": int(killed_at * 1000),
                    "recovered_at_ms": int(recovered_at * 1000),
                    "actual_seconds": round(recovered_at - killed_at, 1)},
        "producer_stall_seconds": stall_seconds(gen),
        "topic_after": topic,
        "generator": gen,
        "verify": result,
        "probe": probe,
        "es_docs_after_drain": indexed,
    })
    L.stop_arm(arm)

    # Leave the topic as the other scenarios expect to find it.
    L.reset_kafka_topic(partitions=6, replication=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())

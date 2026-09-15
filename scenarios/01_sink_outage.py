#!/usr/bin/env python3
"""Scenario 01 -- Elasticsearch goes away for two minutes.

The load never stops, because in real life it does not: syslog keeps arriving,
Windows hosts keep logging, Event Hub keeps filling. The question is where
those events accumulate while the destination is unavailable, and whether they
are all still there afterwards.

The source is modelled with a bounded buffer (identical for every arm), which
is the honest model of an agent: it can hold back for a while, and then it
must start dropping. What each architecture does with that pressure is the
whole result.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

SCENARIO = "01-sink-outage"
RATE = 5000
SECONDS = 300
OUTAGE_AT = 60
OUTAGE_SECONDS = 120
GENERATORS = 4
SOURCE_BUFFER = 200_000   # 40 s of headroom at this rate


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

    L.start_probe("out-probe.json", duration=SECONDS + 180,
                  with_kafka=(arm == "kafka"), interval=1.0)
    L.start_generator_detached(arm, run_id=f"{SCENARIO}-{arm}",
                               out_name="out-gen.json", rate=RATE,
                               duration=SECONDS, generators=GENERATORS,
                               buffer=SOURCE_BUFFER, drain_seconds=240)

    time.sleep(OUTAGE_AT)
    L.log("stopping Elasticsearch")
    stopped_at = time.time()
    L.C.stop_container(L.C.ES_CONTAINER, timeout=20)

    time.sleep(OUTAGE_SECONDS)
    L.log("starting Elasticsearch")
    L.C.start_container(L.C.ES_CONTAINER)
    L.C.wait_container_healthy(L.C.ES_CONTAINER, timeout=240)
    recovered_at = time.time()
    L.log(f"Elasticsearch back after {recovered_at - stopped_at:.0f}s")

    gen = L.wait_generator("out-gen.json", timeout=1800)
    indexed = drain()
    probe = L.collect_probe("out-probe.json")
    result = L.run_verify("out-gen.json", "out-verify.json")

    L.log(f"produced={gen['produced']} dropped_at_source={gen['dropped_at_source']} "
          f"indexed={result['indexed_unique']} missing={result['missing']} "
          f"({result['loss_pct']}%)")

    L.finish(SCENARIO, arm, {
        "config": {"rate": RATE, "seconds": SECONDS, "generators": GENERATORS,
                   "outage_at_s": OUTAGE_AT, "outage_seconds": OUTAGE_SECONDS,
                   "source_buffer_events": SOURCE_BUFFER},
        "outage": {"stopped_at_ms": int(stopped_at * 1000),
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

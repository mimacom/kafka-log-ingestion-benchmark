#!/usr/bin/env python3
"""Scenario 03 -- what does the buffer cost in latency?

A broker between the source and Logstash is a hop that was not there before,
and a hop costs time. This measures how much, at a load comfortably below the
rig's capacity, so the queues are empty and what is left is the pipeline's own
service time rather than backlog.

Reported separately: time to *indexed* (generator to Elasticsearch, per
document) and time to *searchable* (when a query can see it). Those differ by
the refresh interval, and reporting one as the other is the usual way an
end-to-end latency figure ends up wrong.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

SCENARIO = "03-latency"
RATE = 5000
SECONDS = 120
GENERATORS = 4


def drain(expected, timeout=180):
    """Wait until Elasticsearch stops gaining documents."""
    stable = 0
    previous = -1
    deadline = time.time() + timeout
    while time.time() < deadline:
        L.C.es_refresh()
        count = L.C.es_count()
        if count == previous:
            stable += 1
            if stable >= 3:
                return count
        else:
            stable = 0
        previous = count
        time.sleep(2)
    return previous


def searchable_lag(probe, gen):
    """Milliseconds between a document being indexed and a search returning it.

    Measured directly: the probe records the age of the newest document search
    can see. The alternative -- inferring it from the highest visible sequence
    number and the offered rate -- assumes the rate is exactly as configured and
    breaks when a warm-up pass has left higher sequence numbers in the index.
    That inference is kept only as a fallback for runs recorded before the probe
    reported the age.
    """
    # Only while data is still arriving. The probe outlives the generator, and
    # once ingestion stops the "age of the newest visible document" simply grows
    # with the clock -- including those samples would measure how long the run
    # had been over, not how far behind search was.
    samples = probe.get("samples", [])
    ages = []
    previous_docs = None
    for sample in samples:
        docs = sample.get("searchable_docs")
        age = sample.get("newest_searchable_age_ms")
        if docs is not None and previous_docs is not None and docs > previous_docs \
                and age is not None:
            ages.append(age)
        previous_docs = docs if docs is not None else previous_docs
    if ages:
        ordered = sorted(ages)
        return round(ordered[len(ordered) // 2], 1)

    samples = [s for s in probe.get("samples", [])
               if s.get("max_searchable_seq") is not None]
    if not samples:
        return None
    per_gen_rate = gen["phases"][0]["rate"] / gen["generators"]
    lags = []
    for sample in samples:
        emitted = per_gen_rate * sample["t"]
        visible = sample["max_searchable_seq"] + 1
        # Warm-up events carry higher sequence numbers than the measured run has
        # reached yet, so anything "ahead of schedule" is discarded.
        if visible <= 0 or emitted <= 0 or visible > emitted:
            continue
        lags.append((emitted - visible) / per_gen_rate)
    if not lags:
        return None
    return round(sorted(lags)[len(lags) // 2] * 1000, 1)


def run_arm(arm):
    L.log(f"=== {SCENARIO}: {arm} ===")
    L.reset_pipeline_state(arm)
    L.start_arm(arm)
    L.warm_up(arm)

    L.start_probe("lat-probe.json", duration=SECONDS + 90,
                  with_kafka=(arm == "kafka"), interval=1.0)
    gen = L.run_generator(arm, run_id=f"{SCENARIO}-{arm}", out_name="lat-gen.json",
                          rate=RATE, duration=SECONDS, generators=GENERATORS,
                          drain_seconds=120)
    indexed = drain(gen["produced"])
    probe = L.collect_probe("lat-probe.json")
    result = L.run_verify("lat-gen.json", "lat-verify.json")

    L.log(f"delivered={gen['delivered']} indexed_unique={result['indexed_unique']} "
          f"p50={result['latency_ms']['50']}ms p95={result['latency_ms']['95']}ms "
          f"p99={result['latency_ms']['99']}ms")

    L.finish(SCENARIO, arm, {
        "config": {"rate": RATE, "seconds": SECONDS, "generators": GENERATORS},
        "generator": gen,
        "verify": result,
        "probe": probe,
        "searchable_lag_ms_median": searchable_lag(probe, gen),
        "searchable_lag_measured_directly": any(
            s.get("newest_searchable_age_ms") is not None
            for s in probe.get("samples", [])),
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

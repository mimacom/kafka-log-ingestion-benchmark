#!/usr/bin/env python3
"""Scenario 02 -- a burst well above what the pipeline can index.

Nothing is broken here. Everything is running; there is simply more data
arriving for two minutes than the downstream can absorb. Log platforms meet
this constantly -- an incident, a debug level left on, a batch job, a scan.

What is being compared is not whether the burst is absorbed instantly, because
no architecture does that. It is *where* the excess waits, and whether it is
still there when capacity frees up. A buffer converts an overload into a
bounded delay that can be watched and waited out. Without one, the overload
propagates back to the source, and the source is the only component that
cannot wait.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

SCENARIO = "02-burst"
BASE_RATE = 8000
GENERATORS = 6
SOURCE_BUFFER = 200_000
BURST_SECONDS = 40

# Hardcoding the burst rate is how this scenario silently stopped testing
# anything once the rig got faster: an earlier 25,000/s "burst" sat comfortably
# below capacity and every arm passed. The rate is therefore derived from what
# scenario 00 actually measured, with a floor in case it has not been run.
BURST_MULTIPLE = 1.5
FALLBACK_BURST_RATE = 150000
GENERATOR_CEILING = 250000   # beyond this the generator, not the pipeline, is the limit


def measured_burst_rate():
    caps = []
    for arm in L.ARMS:
        path = L.C.RESULTS_DIR / "00-capacity" / arm / "run.json"
        if not path.exists():
            continue
        rate = json.loads(path.read_text()).get("sustained_events_per_s")
        if rate:
            caps.append(rate)
    if not caps:
        L.log(f"no capacity results found, falling back to {FALLBACK_BURST_RATE}/s")
        return FALLBACK_BURST_RATE, None
    # The *fastest* arm sets the bar. Scenario 00 sizes the steady-state
    # scenarios below the slowest arm so every arm is healthy; this one needs
    # the opposite, because a burst that only overloads the weakest arm would
    # let the others pass without ever being tested.
    fastest = max(caps)
    rate = min(GENERATOR_CEILING, round(fastest * BURST_MULTIPLE / 1000) * 1000)
    if rate <= fastest:
        L.log(f"warning: capped at {rate:,}/s, which does not exceed the fastest "
              f"arm's {fastest:,.0f}/s; this run will not overload every arm")
    return rate, fastest


BURST_RATE, MEASURED_CEILING = measured_burst_rate()
PHASES = f"{BASE_RATE}:45,{BURST_RATE}:{BURST_SECONDS},{BASE_RATE}:90"


def drain(timeout=420):
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


def recovery_seconds(probe, produced):
    """How long after the burst the pipeline needed to catch up."""
    samples = [s for s in probe.get("samples", [])
               if s.get("searchable_docs") is not None]
    for sample in samples:
        if sample["searchable_docs"] >= produced * 0.999:
            return sample["t"]
    return None


def run_arm(arm):
    L.log(f"=== {SCENARIO}: {arm} ===")
    L.reset_pipeline_state(arm)
    L.start_arm(arm)
    L.warm_up(arm)

    total_seconds = 175
    L.start_probe("burst-probe.json", duration=total_seconds + 240,
                  with_kafka=(arm == "kafka"), interval=1.0)
    gen = L.run_generator(arm, run_id=f"{SCENARIO}-{arm}", out_name="burst-gen.json",
                          phases=PHASES, generators=GENERATORS,
                          buffer=SOURCE_BUFFER, drain_seconds=300)
    indexed = drain()
    probe = L.collect_probe("burst-probe.json")
    result = L.run_verify("burst-gen.json", "burst-verify.json")

    peak_lag = max((s.get("kafka_lag") or 0 for s in probe.get("samples", [])),
                   default=None)
    L.log(f"produced={gen['produced']} dropped_at_source={gen['dropped_at_source']} "
          f"indexed={result['indexed_unique']} missing={result['missing']} "
          f"({result['loss_pct']}%) peak_kafka_lag={peak_lag}")

    L.finish(SCENARIO, arm, {
        "config": {"phases": PHASES, "base_rate": BASE_RATE,
                   "burst_rate": BURST_RATE, "generators": GENERATORS,
                   "burst_seconds": BURST_SECONDS,
                   "source_buffer_events": SOURCE_BUFFER,
                   "fastest_arm_ceiling_events_per_s": MEASURED_CEILING,
                   "burst_multiple_of_ceiling": (
                       round(BURST_RATE / MEASURED_CEILING, 2)
                       if MEASURED_CEILING else None)},
        "generator": gen,
        "verify": result,
        "probe": probe,
        "peak_kafka_lag": peak_lag,
        "recovery_seconds": recovery_seconds(probe, gen["produced"]),
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

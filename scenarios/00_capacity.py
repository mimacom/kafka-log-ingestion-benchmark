#!/usr/bin/env python3
"""Scenario 00 -- how fast can each arm actually go on this rig?

This runs before anything else, and the other scenarios depend on it.

If a failure scenario is run at a rate one arm cannot sustain even while
healthy, that arm loses events for reasons that have nothing to do with the
failure being tested, and the comparison is worthless -- it would be measuring
an undersized rig and calling it an architectural finding. So each arm is
deliberately overloaded here, its sustained indexing rate is measured, and the
failure scenarios then run at a rate comfortably below the *slowest* arm.

That choice is deliberately unfavourable to the conclusion: it gives every
alternative enough headroom to be healthy, so whatever loss appears later is
caused by the failure and not by the load.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

SCENARIO = "00-capacity"
OFFERED = 150000      # deliberately beyond what the generator itself can emit
SECONDS = 90
GENERATORS = 6
SETTLE = 20           # ignore the first and last stretch of the window


def sustained_rate(probe, settle=SETTLE):
    """Documents per second indexed during the steady middle of the run."""
    samples = [s for s in probe.get("samples", [])
               if s.get("searchable_docs") is not None]
    window = [s for s in samples if settle <= s["t"] <= SECONDS - 5]
    if len(window) < 10:
        return None
    span = window[-1]["t"] - window[0]["t"]
    gained = window[-1]["searchable_docs"] - window[0]["searchable_docs"]
    return round(gained / span, 1) if span > 0 else None


def run_arm(arm):
    L.log(f"=== {SCENARIO}: {arm} ===")
    L.reset_pipeline_state(arm)
    L.start_arm(arm)
    L.warm_up(arm, seconds=40)
    L.C.recreate_index()   # warm-up documents would distort the slope

    L.start_probe("cap-probe.json", duration=SECONDS + 60,
                  with_kafka=(arm == "kafka"), interval=1.0)
    gen = L.run_generator(arm, run_id=f"{SCENARIO}-{arm}", out_name="cap-gen.json",
                          rate=OFFERED, duration=SECONDS, generators=GENERATORS,
                          buffer=200000, drain_seconds=120)
    probe = L.collect_probe("cap-probe.json")
    rate = sustained_rate(probe)

    # If the pipeline indexes everything that was offered, the number measured
    # is the generator's output rate, not the pipeline's ceiling. Saying so is
    # the difference between a capacity figure and a lower bound.
    produced_rate = gen["produced"] / SECONDS
    kept_up = bool(rate and rate >= produced_rate * 0.95)

    L.log(f"offered={OFFERED}/s emitted={produced_rate:,.0f}/s "
          f"indexed={rate}/s kept_up={kept_up}")

    L.finish(SCENARIO, arm, {
        "config": {"offered_rate": OFFERED, "seconds": SECONDS,
                   "generators": GENERATORS},
        "sustained_events_per_s": rate,
        "generator_emitted_per_s": round(produced_rate, 1),
        "pipeline_kept_up_with_generator": kept_up,
        "generator": {k: v for k, v in gen.items() if k != "rate_series"},
        "rate_series": gen["rate_series"],
        "probe": probe,
    })
    L.stop_arm(arm)
    return rate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--arm", action="append", choices=L.ARMS)
    args = parser.parse_args()

    rates = {}
    for arm in (args.arm or L.ARMS):
        rates[arm] = run_arm(arm)

    known = [r for r in rates.values() if r]
    print("\nsustained indexing rate per arm:")
    for arm, rate in rates.items():
        print(f"  {arm:12s} {rate}/s")
    if known:
        print(f"\nslowest arm: {min(known):,.0f}/s -- the failure scenarios run at "
              f"5,000/s, which leaves every arm {min(known) / 5000:.0f}x of headroom "
              f"before anything is broken")
    return 0


if __name__ == "__main__":
    sys.exit(main())

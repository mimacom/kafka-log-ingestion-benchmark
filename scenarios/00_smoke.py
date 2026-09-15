#!/usr/bin/env python3
"""Short end-to-end check of all three arms.

Not a measurement: a check that the rig is wired correctly before anyone
spends half an hour on the real suite. At this rate nothing should be lost
anywhere, so any loss here is a bug in the harness, not a finding.
"""

from __future__ import annotations

import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

# Overridable so a constrained machine can offer less load without the check
# losing its teeth.
RATE = int(os.environ.get("SMOKE_RATE", "2000"))
SECONDS = int(os.environ.get("SMOKE_SECONDS", "20"))


def main():
    if not L.C.es_ready(timeout=120):
        sys.exit("Elasticsearch is not up -- run `make up` first")

    failures = []
    for arm in L.ARMS:
        L.log(f"--- smoke: {arm} ---")
        L.reset_pipeline_state(arm)
        L.start_arm(arm)

        gen = L.run_generator(arm, run_id=f"smoke-{arm}", out_name="smoke-gen.json",
                              rate=RATE, duration=SECONDS, generators=2,
                              drain_seconds=60)
        L.log(f"produced={gen['produced']} delivered={gen['delivered']} "
              f"dropped={gen['dropped_at_source']}")

        L.C.es_refresh()
        # A moment for the tail of the pipeline to land before counting.
        for _ in range(30):
            time.sleep(1)
            L.C.es_refresh()
            if L.C.es_count() >= gen["produced"]:
                break

        result = L.run_verify("smoke-gen.json", "smoke-verify.json")
        L.log(f"expected={result['expected']} indexed={result['indexed_unique']} "
              f"missing={result['missing']} dups={result['duplicates']} "
              f"p50={result['latency_ms']['50']}ms p99={result['latency_ms']['99']}ms")

        if result["indexed_unique"] == 0:
            failures.append(f"{arm}: nothing reached Elasticsearch")
        elif result["missing"] > 0:
            failures.append(
                f"{arm}: {result['missing']} events missing at a rate that should "
                f"lose none -- the harness is wrong, not the architecture")
        L.stop_arm(arm)

    if failures:
        print("\nSMOKE FAILED")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("\nsmoke passed: all three arms ingest end to end with zero loss")
    return 0


if __name__ == "__main__":
    sys.exit(main())

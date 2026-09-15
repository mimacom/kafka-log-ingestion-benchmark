"""Proves the verifier actually detects loss and duplication.

Indexes a synthetic run into a throwaway index with a known number of
deliberately omitted and deliberately repeated events, then asserts the
verifier reports exactly those numbers. Every loss figure in the report rests
on this code path, so it gets tested rather than trusted.
"""

from __future__ import annotations

import json
import sys

from common import es, es_refresh
from verify import verify

INDEX = "bench-selftest"

GENERATORS = 3
PER_GEN = 5000
DROP = {0: [7, 8, 9, 4321], 1: [0], 2: []}           # 5 events never indexed
DUPLICATE = {0: [100, 101], 1: [], 2: [4999, 3, 2]}  # 5 events indexed twice


def build_docs():
    lines = []
    for gen in range(GENERATORS):
        dropped = set(DROP[gen])
        duplicated = set(DUPLICATE[gen])
        for seq in range(PER_GEN):
            if seq in dropped:
                continue
            emit = 1758000000000 + seq
            doc = {"@timestamp": emit, "emit_ts": emit,
                   "ingest_ts": emit + 12 + (seq % 7),
                   "gen_id": gen, "seq": seq, "arm": "selftest",
                   "run_id": "selftest", "log_level": "info",
                   "message": "selftest event"}
            copies = 2 if seq in duplicated else 1
            for _ in range(copies):
                lines.append(json.dumps({"index": {}}))
                lines.append(json.dumps(doc))
    return lines


def main():
    es("DELETE", f"/{INDEX}")
    status, body = es("PUT", f"/{INDEX}", {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0,
                     "index.default_pipeline": "_none"},
        "mappings": {"dynamic": False, "properties": {
            "@timestamp": {"type": "date"},
            "emit_ts": {"type": "date"},
            "ingest_ts": {"type": "date"},
            "gen_id": {"type": "short"},
            "seq": {"type": "long"},
        }},
    })
    if status not in (200, 201):
        sys.exit(f"could not create {INDEX}: {body}")

    lines = build_docs()
    chunk = 20000
    for start in range(0, len(lines), chunk):
        body = ("\n".join(lines[start:start + chunk]) + "\n").encode()
        status, resp = es("POST", f"/{INDEX}/_bulk", body, timeout=120)
        if status != 200 or resp.get("errors"):
            sys.exit(f"bulk failed: {json.dumps(resp)[:500]}")
    es_refresh(INDEX)

    gen_stats = {
        "dropped_at_source": 0,
        "per_generator": {
            str(gen): {"produced": PER_GEN, "first_seq": 0, "last_seq": PER_GEN - 1}
            for gen in range(GENERATORS)
        },
    }
    result = verify(gen_stats, INDEX)

    expected_missing = sum(len(v) for v in DROP.values())
    expected_duplicates = sum(len(v) for v in DUPLICATE.values())
    expected_total = GENERATORS * PER_GEN

    checks = [
        ("expected total", result["expected"], expected_total),
        ("missing", result["missing"], expected_missing),
        ("duplicates", result["duplicates"], expected_duplicates),
        ("indexed unique", result["indexed_unique"], expected_total - expected_missing),
        ("lost in transit", result["lost_in_transit"], expected_missing),
        ("first gap in generator 0", result["per_generator"]["0"]["first_gap"], 7),
    ]

    failures = [c for c in checks if c[1] != c[2]]
    for name, got, want in checks:
        print(f"{'ok  ' if got == want else 'FAIL'} {name}: got {got}, want {want}")

    latency = result["latency_ms"]
    if not (12 <= latency["50"] <= 19):
        failures.append(("latency p50 in range", latency["50"], "12..19"))
        print(f"FAIL latency p50: got {latency['50']}, want 12..19")
    else:
        print(f"ok   latency p50: {latency['50']} ms")

    es("DELETE", f"/{INDEX}")
    if failures:
        sys.exit(f"\n{len(failures)} accounting check(s) failed")
    print("\naccounting verified: loss, duplication and latency all reported exactly")
    return 0


if __name__ == "__main__":
    sys.exit(main())

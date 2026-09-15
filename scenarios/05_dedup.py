#!/usr/bin/env python3
"""Scenario 05 -- deduplicating at-least-once delivery.

Some sources cannot promise exactly-once. An Event Hub reader that fails
between reading and checkpointing re-reads; an agent that does not see an ack
resends. Kafka's idempotent producer removes duplicates created by its own
retries, but it cannot remove a duplicate the source genuinely sent twice --
to Kafka those are two different produce calls carrying identical content.

Removing them is therefore a pipeline job. A fingerprint of the event's
content becomes the document _id, so a redelivered event overwrites itself.
This measures what that is worth: how many duplicate documents appear with and
without it, and what they cost in storage.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import _lib as L  # noqa: E402

SCENARIO = "05-dedup"
RATE = 5000
SECONDS = 90
GENERATORS = 4
REPLAY_FRACTION = 0.2


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


def run_variant(dedup):
    label = "fingerprint-id" if dedup else "auto-id"
    L.log(f"--- {SCENARIO}: {label} ---")

    L.C.stop_all_arms()
    L.C.recreate_index()
    L.reset_kafka_topic(partitions=6, replication=1)

    # Recreating the container is what applies LS_DEDUP: it selects the
    # fingerprint filter and the document_id-based output.
    L.C.compose("up", "-d", "--force-recreate", "logstash-kafka",
                env={"LS_DEDUP": "true" if dedup else "false"})
    L.logstash_ready("kafka", timeout=240)
    time.sleep(8)
    L.warm_up("kafka", seconds=30)
    # Warm-up documents would be counted as stored documents and would inflate
    # the index size being compared, so the index starts empty for the measured
    # pass.
    L.C.recreate_index()

    gen = L.run_generator("kafka", run_id=f"{SCENARIO}-{label}",
                          out_name="dedup-gen.json", rate=RATE, duration=SECONDS,
                          generators=GENERATORS, replay_fraction=REPLAY_FRACTION,
                          drain_seconds=180)
    indexed = drain()
    result = L.run_verify("dedup-gen.json", "dedup-verify.json")

    # Merge first: comparing store size before a merge compares segment
    # counts as much as it compares content.
    L.C.es("POST", f"/{L.C.INDEX}/_forcemerge?max_num_segments=1", timeout=600)
    time.sleep(5)
    L.C.es_refresh()
    size = L.C.index_size_bytes()

    sent = gen["produced"] + gen["replays_sent"]
    L.log(f"unique={gen['produced']} sent={sent} replays={gen['replays_sent']} "
          f"docs_in_es={indexed} duplicate_docs={result['duplicates']} "
          f"size={size / 1e6:.1f}MB")

    L.C.stop_container(L.C.ARM_CONTAINERS["kafka"])
    return {
        "mode": label,
        "dedup_enabled": dedup,
        "unique_events": gen["produced"],
        "replays_sent": gen["replays_sent"],
        "events_on_the_wire": sent,
        "docs_in_elasticsearch": indexed,
        "duplicate_docs": result["duplicates"],
        "missing": result["missing"],
        "index_size_bytes": size,
        "generator": {k: v for k, v in gen.items() if k != "rate_series"},
        "verify": {k: v for k, v in result.items() if k != "latency_series"},
    }


def main():
    parser = argparse.ArgumentParser()
    parser.parse_args()

    variants = [run_variant(False), run_variant(True)]
    without, with_dedup = variants

    saved = without["index_size_bytes"] - with_dedup["index_size_bytes"]
    L.finish(SCENARIO, "kafka", {
        "config": {"rate": RATE, "seconds": SECONDS, "generators": GENERATORS,
                   "replay_fraction": REPLAY_FRACTION},
        "variants": variants,
        "duplicate_docs_removed": without["duplicate_docs"] - with_dedup["duplicate_docs"],
        "index_bytes_saved": saved,
        "index_pct_saved": (round(100.0 * saved / without["index_size_bytes"], 1)
                            if without["index_size_bytes"] else None),
    })

    # Leave the standard arm configuration behind.
    L.C.compose("up", "-d", "--force-recreate", "--no-start", "logstash-kafka",
                env={"LS_DEDUP": "false"})
    return 0


if __name__ == "__main__":
    sys.exit(main())

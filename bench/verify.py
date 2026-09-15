"""Exact loss, duplicate and latency accounting.

Every document carries (gen_id, seq) from a strictly increasing per-generator
counter, so the set of events that *should* be in Elasticsearch is known
precisely. This walks the whole index with a point-in-time cursor and marks a
bitset, which makes the answer exact rather than an estimate -- no cardinality
approximation, no sampling. An "about 0.3% loss" claim is not worth much; a
"142 of 1,204,880 events, and here are the gaps" claim is.

Latency is accumulated into a 1 ms-resolution histogram. The underlying
timestamps are millisecond-precision, so percentiles read off that histogram
are exact, not interpolated.
"""

from __future__ import annotations

import argparse
import array
import json
import sys

from common import INDEX, es

PAGE = 10000
MAX_LATENCY_MS = 300_000  # beyond five minutes goes to the overflow bucket
SECOND_BUCKET_MS = 10     # resolution of the per-second latency series
SECOND_BUCKETS = 1000     # up to 10s, then overflow


def histogram_percentiles(hist, total, pcts=(50, 90, 95, 99, 99.9)):
    if not total:
        return {str(p): None for p in pcts}
    targets = sorted((total - 1) * p / 100.0 for p in pcts)
    out = {}
    order = sorted(pcts)
    cumulative = 0
    idx = 0
    for value, count in enumerate(hist):
        if not count:
            continue
        cumulative += count
        while idx < len(targets) and cumulative > targets[idx]:
            out[str(order[idx])] = value
            idx += 1
        if idx >= len(targets):
            break
    for p in order:
        out.setdefault(str(p), None)
    return out


def scan(index, last_seq_by_gen, expected_by_gen):
    """Walk the index once, collecting everything the report needs."""
    status, body = es("POST", f"/{index}/_pit?keep_alive=5m")
    if status != 200:
        raise RuntimeError(f"could not open point-in-time: {body}")
    pit_id = body["id"]

    seen = {gid: bytearray(last + 2) for gid, last in last_seq_by_gen.items()}
    latency = array.array("q", bytes(8 * (MAX_LATENCY_MS + 2)))
    per_second = {}

    docs = 0
    duplicates = 0
    unknown_gen = 0
    negative_latency = 0
    latency_total = 0
    latency_sum = 0
    latency_max = 0
    first_emit_ms = None

    search_after = None
    try:
        while True:
            query = {
                "size": PAGE,
                "track_total_hits": False,
                "_source": False,
                "sort": [{"seq": "asc"}, {"gen_id": "asc"}],
                "docvalue_fields": [
                    {"field": "gen_id"},
                    {"field": "seq"},
                    {"field": "emit_ts", "format": "epoch_millis"},
                    {"field": "ingest_ts", "format": "epoch_millis"},
                ],
                "pit": {"id": pit_id, "keep_alive": "5m"},
            }
            if search_after:
                query["search_after"] = search_after
            status, body = es("POST", "/_search", query, timeout=120)
            if status != 200:
                raise RuntimeError(f"search failed: {body}")
            hits = body["hits"]["hits"]
            if not hits:
                break
            pit_id = body.get("pit_id", pit_id)

            for hit in hits:
                fields = hit["fields"]
                gid = int(fields["gen_id"][0])
                seq = int(fields["seq"][0])
                docs += 1

                bits = seen.get(gid)
                if bits is None or seq >= len(bits):
                    # Belongs to a warm-up pass, not to the measured run.
                    unknown_gen += 1
                    continue
                if bits[seq]:
                    duplicates += 1
                    continue
                bits[seq] = 1

                emit = fields.get("emit_ts")
                ingest = fields.get("ingest_ts")
                if not emit or not ingest:
                    continue
                emit_ms = int(emit[0])
                ingest_ms = int(ingest[0])
                delta = ingest_ms - emit_ms
                if delta < 0:
                    negative_latency += 1
                    delta = 0
                if first_emit_ms is None or emit_ms < first_emit_ms:
                    first_emit_ms = emit_ms

                latency_total += 1
                latency_sum += delta
                latency_max = max(latency_max, delta)
                latency[min(delta, MAX_LATENCY_MS)] += 1

                second = (emit_ms // 1000)
                slot = per_second.get(second)
                if slot is None:
                    slot = per_second[second] = [0, 0, 0, array.array(
                        "q", bytes(8 * (SECOND_BUCKETS + 1)))]
                slot[0] += 1
                slot[1] += delta
                slot[2] = max(slot[2], delta)
                slot[3][min(delta // SECOND_BUCKET_MS, SECOND_BUCKETS)] += 1

            search_after = hits[-1]["sort"]
    finally:
        es("DELETE", "/_pit", {"id": pit_id})

    per_gen = {}
    unique_total = 0
    missing_total = 0
    for gid, bits in seen.items():
        expected = expected_by_gen.get(gid, 0)
        unique = sum(bits[:expected]) if expected else 0
        per_gen[str(gid)] = {
            "expected": expected,
            "indexed_unique": unique,
            "missing": expected - unique,
            "first_gap": next((i for i in range(expected) if not bits[i]), None),
        }
        unique_total += unique
        missing_total += expected - unique

    series = []
    if first_emit_ms is not None:
        base = first_emit_ms // 1000
        for second in sorted(per_second):
            count, total_ms, peak, hist = per_second[second]
            pcts = histogram_percentiles(hist, count, pcts=(50, 95))
            series.append({
                "t": second - base,
                "count": count,
                "mean_ms": round(total_ms / count, 1),
                "p50_ms": (pcts["50"] * SECOND_BUCKET_MS) if pcts["50"] is not None else None,
                "p95_ms": (pcts["95"] * SECOND_BUCKET_MS) if pcts["95"] is not None else None,
                "max_ms": peak,
            })

    return {
        "index": index,
        "docs_scanned": docs,
        "indexed_unique": unique_total,
        "duplicates": duplicates,
        "unknown_generator_docs": unknown_gen,
        "expected": sum(expected_by_gen.values()),
        "missing": missing_total,
        "per_generator": per_gen,
        "latency_ms": dict(
            histogram_percentiles(latency, latency_total),
            mean=round(latency_sum / latency_total, 1) if latency_total else None,
            max=latency_max,
            samples=latency_total,
            negative_clamped=negative_latency,
        ),
        "latency_series": series,
    }


def verify(gen_stats, index=None):
    """`gen_stats` is the JSON the generator wrote."""
    per_gen = gen_stats["per_generator"]
    expected_by_gen = {int(gid): state["produced"] for gid, state in per_gen.items()}
    last_seq_by_gen = {int(gid): state["last_seq"] for gid, state in per_gen.items()}
    result = scan(index or INDEX, last_seq_by_gen, expected_by_gen)

    dropped = gen_stats.get("dropped_at_source", 0)
    result["dropped_at_source"] = dropped
    # Events the source handed to the transport and never saw again. This is
    # the number that separates "the buffer was too small" from "the pipeline
    # lost data it had accepted".
    result["lost_in_transit"] = max(0, result["missing"] - dropped)
    result["loss_pct"] = (
        round(100.0 * result["missing"] / result["expected"], 4)
        if result["expected"] else 0.0
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gen-stats", required=True)
    parser.add_argument("--index", default=None)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    with open(args.gen_stats) as handle:
        gen_stats = json.load(handle)

    result = verify(gen_stats, args.index)
    with open(args.out, "w") as handle:
        json.dump(result, handle, indent=2)

    summary = {k: v for k, v in result.items()
               if k not in ("latency_series", "per_generator")}
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

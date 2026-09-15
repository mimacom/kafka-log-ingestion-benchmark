"""Load generator.

Models a log source -- an agent, a syslog daemon, an Event Hub reader -- with a
*bounded* local buffer. That bound is the whole point: a real source cannot
block forever when the downstream is unavailable, because events keep arriving
whether or not anyone is ready to receive them. When the buffer is full the
generator drops, and counts the drop. Those drops are real, attributable data
loss, and they are reported separately from events that were accepted by the
transport but never reached Elasticsearch.

Every arm gets the same buffer bound, the same payload bytes and the same
pacing, so the only thing that differs is what sits between the source and
Elasticsearch.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
import http.client
import urllib.parse
from collections import deque

# ------------------------------------------------------------- Event shape ---

LEVELS = ["debug", "info", "info", "info", "notice", "warn", "error", "critical"]
SERVICES = ["auth-api", "payments", "gateway", "scheduler", "inventory", "search"]
METHODS = ["GET", "GET", "GET", "POST", "PUT", "DELETE"]
LOGGERS = ["com.example.http.Access", "com.example.db.Pool", "com.example.job.Runner"]
STATUSES = [200, 200, 200, 201, 204, 302, 400, 404, 500]

# Field names are deliberately flat. The Logstash HTTP input decorates events
# with `host`, `http`, `url` and `headers`; keeping payload names distinct from
# those means the shared filter can strip transport metadata without touching
# a single byte of the event itself.
# No @timestamp: Logstash sets its own, and sending a numeric one only produces
# a parse failure and a stray _@timestamp field. emit_ts is what latency is
# measured from, and it is carried explicitly.
TEMPLATE = (
    '{"emit_ts":%(ts)d,"gen_id":%(gen)d,"seq":%(seq)d,'
    '"arm":"%(arm)s","run_id":"%(run)s","log_level":"%(level)s",'
    '"logger":"%(logger)s","host_name":"host-%(host)02d.internal",'
    '"service_name":"%(service)s","src_ip":"10.%(o2)d.%(o3)d.%(o4)d",'
    '"user_name":"user%(user)04d","http_method":"%(method)s",'
    '"status_code":%(status)d,"bytes":%(bytes)d,"trace_id":"%(trace)s",'
    '"message":"%(message)s","filler":"%(filler)s"}'
)

HEX = "0123456789abcdef"


def build_variants(payload_bytes, count=512, seed=1234):
    """Pre-render the random parts of the payload.

    Randomising every field per event would make the generator, rather than the
    pipeline, the bottleneck. A fixed pool of variants keeps the byte
    distribution realistic while leaving the hot loop to a single string
    interpolation.
    """
    rng = random.Random(seed)
    variants = []
    for _ in range(count):
        base = {
            "level": rng.choice(LEVELS),
            "logger": rng.choice(LOGGERS),
            "host": rng.randrange(1, 40),
            "service": rng.choice(SERVICES),
            "o2": rng.randrange(0, 255), "o3": rng.randrange(0, 255),
            "o4": rng.randrange(1, 254),
            "user": rng.randrange(0, 9999),
            "method": rng.choice(METHODS),
            "status": rng.choice(STATUSES),
            "bytes": rng.randrange(120, 90000),
            "trace": "".join(rng.choice(HEX) for _ in range(32)),
            "message": "request completed in %dms" % rng.randrange(1, 4000),
            "filler": "",
        }
        # Pad to the requested event size with text-like filler so that
        # compression ratios in scenario 06 are representative of log data
        # rather than of random noise.
        rendered = TEMPLATE % dict(base, ts=1758000000000, gen=0, seq=0,
                                   arm="direct-mem", run="0" * 12)
        pad = max(0, payload_bytes - len(rendered))
        words = ["session", "retry", "cache", "miss", "hit", "upstream",
                 "timeout", "pool", "commit", "rollback", "shard", "queue"]
        filler = []
        size = 0
        while size < pad:
            word = rng.choice(words)
            filler.append(word)
            size += len(word) + 1
        base["filler"] = " ".join(filler)[:pad] if pad else ""
        variants.append(base)
    return variants


# ------------------------------------------------------------- Rate phases ---

def parse_phases(spec):
    """'5000:60,30000:120' -> [(5000.0, 60.0), (30000.0, 120.0)]"""
    phases = []
    for chunk in spec.split(","):
        rate, _, seconds = chunk.partition(":")
        phases.append((float(rate), float(seconds)))
    return phases


def target_by(phases, elapsed):
    """Events a single generator should have produced after `elapsed` seconds."""
    total = 0.0
    acc = 0.0
    for rate, seconds in phases:
        if elapsed >= acc + seconds:
            total += rate * seconds
            acc += seconds
        else:
            total += rate * max(0.0, elapsed - acc)
            break
    return total


# ----------------------------------------------------------------- Senders ---

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.produced = 0
        self.delivered = 0
        self.dropped_at_source = 0
        self.delivery_errors = 0
        self.bytes_out = 0
        self.replays = 0

    def snapshot(self):
        with self.lock:
            return (self.produced, self.delivered, self.dropped_at_source,
                    self.delivery_errors, self.bytes_out, self.replays)


class KafkaSender:
    """Produces to Kafka with idempotence on and acks=all.

    `queue.buffering.max.messages` is the source buffer bound, set to the same
    value the HTTP senders use, so neither arm gets a larger safety net.
    """

    def __init__(self, args, stats):
        from confluent_kafka import Producer

        self.stats = stats
        self.args = args
        conf = {
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": args.compression,
            "linger.ms": args.linger_ms,
            "batch.size": 262144,
            "queue.buffering.max.messages": args.buffer,
            "queue.buffering.max.kbytes": 1048576,
            "message.timeout.ms": args.message_timeout_ms,
            "socket.keepalive.enable": True,
        }
        self.producer = Producer(conf)
        self.stop = threading.Event()
        self.poller = threading.Thread(target=self._poll, daemon=True)
        self.poller.start()

    def _poll(self):
        while not self.stop.is_set():
            self.producer.poll(0.05)

    def _on_delivery(self, err, msg):
        with self.stats.lock:
            if err is None:
                self.stats.delivered += 1
                self.stats.bytes_out += len(msg)
            else:
                self.stats.delivery_errors += 1

    def send(self, payload):
        try:
            self.producer.produce(self.args.topic, value=payload,
                                  on_delivery=self._on_delivery)
            return True
        except BufferError:
            # The local buffer is full: this is the source dropping events.
            return False

    def close(self, drain_seconds):
        self.producer.flush(drain_seconds)
        self.stop.set()
        self.poller.join(timeout=2)


class HttpSender:
    """Posts newline-delimited JSON to the Logstash HTTP input.

    A failed batch goes back into the buffer rather than being thrown away,
    which is what an agent with a retry policy does. Events are lost only when
    the buffer itself has no room left.
    """

    def __init__(self, args, stats):
        self.args = args
        self.stats = stats
        self.buffer = deque()
        self.cv = threading.Condition()
        self.stop = threading.Event()
        self.threads = [
            threading.Thread(target=self._worker, daemon=True)
            for _ in range(args.sender_threads)
        ]
        for thread in self.threads:
            thread.start()

    def send(self, payload):
        with self.cv:
            if len(self.buffer) >= self.args.buffer:
                return False
            self.buffer.append(payload)
            self.cv.notify()
            return True

    def _take_batch(self):
        with self.cv:
            if not self.buffer:
                self.cv.wait(timeout=0.2)
            batch = []
            while self.buffer and len(batch) < self.args.batch:
                batch.append(self.buffer.popleft())
            return batch

    def _return_batch(self, batch):
        """Put a failed batch back, dropping only what no longer fits."""
        with self.cv:
            room = self.args.buffer - len(self.buffer)
            keep = batch[:room] if room > 0 else []
            dropped = len(batch) - len(keep)
            self.buffer.extendleft(reversed(keep))
        if dropped:
            with self.stats.lock:
                self.stats.dropped_at_source += dropped

    def _connect(self):
        parsed = urllib.parse.urlparse(self.args.url)
        return http.client.HTTPConnection(
            parsed.hostname, parsed.port or 80, timeout=self.args.http_timeout)

    def _worker(self):
        parsed = urllib.parse.urlparse(self.args.url)
        path = parsed.path or "/"
        headers = {"Content-Type": "application/x-ndjson"}
        # One persistent connection per worker, as a real shipper keeps. Opening
        # a fresh connection per batch turns ordinary backpressure into
        # connection churn and makes the client, not the pipeline, the subject
        # of the measurement.
        conn = None

        while not self.stop.is_set() or self.buffer:
            batch = self._take_batch()
            if not batch:
                if self.stop.is_set():
                    return
                continue
            body = b"\n".join(batch) + b"\n"
            ok = False
            try:
                if conn is None:
                    conn = self._connect()
                conn.request("POST", path, body=body, headers=headers)
                resp = conn.getresponse()
                resp.read()
                ok = 200 <= resp.status < 300
            except Exception:
                # A blocked input shows up here as a timeout. Drop the
                # connection so the retry starts from a clean socket.
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
            if ok:
                with self.stats.lock:
                    self.stats.delivered += len(batch)
                    self.stats.bytes_out += len(body)
            else:
                with self.stats.lock:
                    self.stats.delivery_errors += len(batch)
                self._return_batch(batch)
                time.sleep(self.args.retry_backoff)

    def close(self, drain_seconds):
        deadline = time.time() + drain_seconds
        while time.time() < deadline:
            with self.cv:
                remaining = len(self.buffer)
            if remaining == 0:
                break
            time.sleep(0.2)
        self.stop.set()
        with self.cv:
            self.cv.notify_all()
        for thread in self.threads:
            thread.join(timeout=self.args.http_timeout + 5)
        with self.cv:
            leftover = len(self.buffer)
            self.buffer.clear()
        if leftover:
            # Never delivered and no longer recoverable: loss, and counted.
            with self.stats.lock:
                self.stats.dropped_at_source += leftover


# --------------------------------------------------------------- Generation ---

def generate(args, sender, stats, variants, phases, gen_id, gen_state):
    """One source. Paces itself against wall clock so a slow transport cannot
    quietly reduce the offered load -- that would hide loss instead of
    measuring it."""
    seq = 0
    start = time.time()
    n_variants = len(variants)
    total_seconds = sum(seconds for _, seconds in phases)
    local_produced = 0
    local_replays = 0
    # Seeded per generator so a replay run is reproducible.
    replay_rng = random.Random(9000 + gen_id) if args.replay_fraction > 0 else None

    while True:
        elapsed = time.time() - start
        if elapsed >= total_seconds:
            break
        want = target_by(phases, elapsed) / args.generators
        if local_produced >= want:
            time.sleep(0.0005)
            continue

        burst = min(int(want - local_produced) + 1, 2000)
        burst_dropped = 0
        burst_replays = 0
        for _ in range(burst):
            variant = variants[(seq + gen_id) % n_variants]
            # Per event, not per burst: at preload rates a burst spans hundreds
            # of events, and one shared timestamp would quietly understate the
            # latency of everything after the first.
            now_ms = int(time.time() * 1000)
            payload = (TEMPLATE % dict(variant, ts=now_ms, gen=gen_id, seq=seq,
                                       arm=args.arm, run=args.run_id)).encode()
            if not sender.send(payload):
                burst_dropped += 1
            # At-least-once redelivery, as an Event Hub reader or a retrying
            # agent produces it: the identical event, sent a second time.
            if replay_rng and replay_rng.random() < args.replay_fraction:
                if sender.send(payload):
                    burst_replays += 1
            seq += 1
        local_produced += burst
        # Published per burst rather than at the end, so the sampled rate
        # series reflects what was happening second by second.
        local_replays += burst_replays
        with stats.lock:
            stats.produced += burst
            stats.dropped_at_source += burst_dropped
            stats.replays += burst_replays

    gen_state[gen_id] = {"produced": local_produced, "replays": local_replays,
                         "first_seq": 0, "last_seq": seq - 1}


def sampler(stats, series, stop, interval=1.0):
    prev = stats.snapshot()
    start = time.time()
    while not stop.wait(interval):
        now = stats.snapshot()
        series.append({
            "t": round(time.time() - start, 2),
            "produced_per_s": round((now[0] - prev[0]) / interval, 1),
            "delivered_per_s": round((now[1] - prev[1]) / interval, 1),
            "dropped_per_s": round((now[2] - prev[2]) / interval, 1),
        })
        prev = now


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--arm", required=True,
                        choices=["kafka", "direct-mem", "direct-pq"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--phases", help="rate:seconds[,rate:seconds...]")
    parser.add_argument("--rate", type=float, default=5000.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--generators", type=int, default=4)
    parser.add_argument("--gen-id-offset", type=int, default=0,
                        help="warm-up passes use an offset so the verifier can "
                             "tell their events apart from the measured ones")
    parser.add_argument("--payload-bytes", type=int, default=600)
    parser.add_argument("--buffer", type=int, default=200000,
                        help="source buffer bound, in events, identical for every arm")
    parser.add_argument("--drain-seconds", type=float, default=90.0)
    parser.add_argument("--replay-fraction", type=float, default=0.0,
                        help="fraction of events sent twice, modelling "
                             "at-least-once delivery from the source")

    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka1:9092"))
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "logs"))
    parser.add_argument("--compression", default="lz4",
                        choices=["none", "gzip", "snappy", "lz4", "zstd"])
    parser.add_argument("--linger-ms", type=int, default=5)
    parser.add_argument("--message-timeout-ms", type=int, default=300000)

    parser.add_argument("--url", default="http://logstash-direct:8080/")
    parser.add_argument("--sender-threads", type=int, default=8)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--http-timeout", type=float, default=15.0)
    parser.add_argument("--retry-backoff", type=float, default=0.25)

    args = parser.parse_args()
    phases = parse_phases(args.phases) if args.phases else [(args.rate, args.duration)]

    variants = build_variants(args.payload_bytes)
    stats = Stats()
    sender = KafkaSender(args, stats) if args.arm == "kafka" else HttpSender(args, stats)

    series = []
    stop = threading.Event()
    sample_thread = threading.Thread(target=sampler, args=(stats, series, stop), daemon=True)
    sample_thread.start()

    gen_state = {}
    started_at = int(time.time() * 1000)
    threads = [
        threading.Thread(target=generate,
                         args=(args, sender, stats, variants, phases,
                               args.gen_id_offset + gid, gen_state))
        for gid in range(args.generators)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    generation_ended_at = int(time.time() * 1000)

    sender.close(args.drain_seconds)
    stop.set()
    sample_thread.join(timeout=2)
    ended_at = int(time.time() * 1000)

    produced, delivered, dropped, errors, bytes_out, replays = stats.snapshot()
    result = {
        "arm": args.arm,
        "run_id": args.run_id,
        "phases": [{"rate": r, "seconds": s} for r, s in phases],
        "generators": args.generators,
        "payload_bytes": args.payload_bytes,
        "buffer_events": args.buffer,
        "compression": args.compression if args.arm == "kafka" else None,
        "produced": produced,
        "delivered": delivered,
        "dropped_at_source": dropped,
        "delivery_errors": errors,
        "bytes_out": bytes_out,
        "replays_sent": replays,
        "replay_fraction": args.replay_fraction,
        "started_at_ms": started_at,
        "generation_ended_at_ms": generation_ended_at,
        "ended_at_ms": ended_at,
        "per_generator": gen_state,
        "rate_series": series,
    }
    with open(args.out, "w") as handle:
        json.dump(result, handle, indent=2)

    print(json.dumps({k: v for k, v in result.items()
                      if k not in ("rate_series", "per_generator")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

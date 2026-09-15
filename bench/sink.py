"""Counting sink.

Scenario 04 asks how far Logstash consumers scale over a partitioned topic.
With Elasticsearch on the end of the pipeline that question gets answered by
Elasticsearch's indexing capacity instead, so this stands in for it: accepts
batches, counts events, discards them. Whatever throughput ceiling shows up
here belongs to Kafka and Logstash.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE = {"events": 0, "batches": 0, "bytes": 0, "first_ts": None, "last_ts": None}
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        count = 0
        try:
            parsed = json.loads(body) if body else []
            count = len(parsed) if isinstance(parsed, list) else 1
        except Exception:
            count = body.count(b"\n")

        now = time.time()
        with LOCK:
            STATE["events"] += count
            STATE["batches"] += 1
            STATE["bytes"] += len(body)
            if STATE["first_ts"] is None:
                STATE["first_ts"] = now
            STATE["last_ts"] = now

        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path == "/reset":
            with LOCK:
                STATE.update({"events": 0, "batches": 0, "bytes": 0,
                              "first_ts": None, "last_ts": None})
        with LOCK:
            payload = json.dumps(dict(STATE)).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        pass  # a per-request log line would itself become the bottleneck


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8200)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"counting sink listening on {args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()

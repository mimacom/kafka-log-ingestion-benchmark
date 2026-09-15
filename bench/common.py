"""Shared helpers. Standard library only, so the same module works on the host
(where the scenario drivers run) and inside the bench container."""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import time
import urllib.error
import urllib.request

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "results"

# Inside the bench container Elasticsearch is a service name; on the host it is
# the published port.
ES_URL = os.environ.get("BENCH_ES", "http://localhost:9200")
INDEX = os.environ.get("BENCH_INDEX", "bench-logs")

# Must match `name:` in docker-compose.yml. Container and volume names are
# derived from it so that renaming the project cannot leave the scenarios
# quietly driving containers that no longer exist.
COMPOSE_PROJECT = "klib"

ARM_CONTAINERS = {
    "kafka": f"{COMPOSE_PROJECT}-ls-kafka",
    "direct-mem": f"{COMPOSE_PROJECT}-ls-direct",
    "direct-pq": f"{COMPOSE_PROJECT}-ls-direct-pq",
}

PQ_VOLUME = f"{COMPOSE_PROJECT}_ls-pq-data"
ES_CONTAINER = f"{COMPOSE_PROJECT}-elasticsearch"

# Human-readable arm names live in theme.py, next to the colours the report
# pairs them with.


# --------------------------------------------------------------- HTTP / ES ---

def request(method, url, body=None, timeout=30, headers=None, retries=2):
    """Minimal JSON HTTP call. Returns (status, parsed_body).

    An HTTP error status is returned to the caller; a *connection* error is
    retried briefly first. Scenarios poll Elasticsearch immediately after
    restarting containers, and a single refused connection there would abort a
    run that had already taken minutes.
    """
    data = None
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()

    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                return exc.code, json.loads(raw)
            except Exception:
                return exc.code, {"error": raw.decode(errors="replace")}
        except (urllib.error.URLError, OSError):
            if attempt == retries:
                raise
            time.sleep(0.5 * (attempt + 1))


def es(method, path, body=None, timeout=30):
    return request(method, f"{ES_URL}{path}", body=body, timeout=timeout)


def es_ready(timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, body = es("GET", "/_cluster/health", timeout=5)
            if status == 200 and body.get("status") in ("green", "yellow"):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def es_count(index=None):
    status, body = es("GET", f"/{index or INDEX}/_count")
    if status != 200:
        return 0
    return body.get("count", 0)


def es_refresh(index=None):
    es("POST", f"/{index or INDEX}/_refresh")


def recreate_index(index=None, settings_override=None):
    """Drop and recreate the benchmark index so every run starts from zero."""
    idx = index or INDEX
    es("DELETE", f"/{idx}")
    body = {}
    if settings_override:
        body["settings"] = settings_override
    status, resp = es("PUT", f"/{idx}", body or None)
    if status not in (200, 201):
        raise RuntimeError(f"could not create index {idx}: {resp}")


def index_size_bytes(index=None):
    status, body = es("GET", f"/{index or INDEX}/_stats/store")
    if status != 200:
        return 0
    return body["_all"]["primaries"]["store"]["size_in_bytes"]


# ----------------------------------------------------------------- Docker ---

def docker(*args, check=True, capture=True):
    proc = subprocess.run(
        ["docker", *args],
        capture_output=capture,
        text=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout.strip() if capture else ""


def container_running(name):
    out = docker("ps", "--filter", f"name=^{name}$", "--format", "{{.Names}}", check=False)
    return name in out.splitlines()


def start_container(name, wait_healthy=False, timeout=180):
    docker("start", name)
    if wait_healthy:
        wait_container_healthy(name, timeout)


def stop_container(name, timeout=30):
    if container_running(name):
        docker("stop", "-t", str(timeout), name)


def wait_container_healthy(name, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = docker("inspect", "-f", "{{.State.Health.Status}}", name, check=False)
        if state == "healthy":
            return True
        if state in ("", "<no value>"):  # no healthcheck defined
            return container_running(name)
        time.sleep(2)
    raise RuntimeError(f"{name} did not become healthy within {timeout}s")


def stop_all_arms():
    for name in ARM_CONTAINERS.values():
        stop_container(name)


def compose(*args, check=True, env=None):
    """`env` overrides values that docker-compose.yml interpolates, which is how
    scenarios vary consumer threads, deduplication and codecs without needing a
    separate service definition for every combination."""
    environment = dict(os.environ)
    if env:
        environment.update({k: str(v) for k, v in env.items()})
    proc = subprocess.run(
        ["docker", "compose", *args],
        cwd=str(REPO_ROOT), capture_output=True, text=True, env=environment,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def run_bench(*args, env=None, timeout=1800):
    """Run a bench tool inside the tooling container."""
    cmd = ["compose", "run", "--rm", "-T"]
    for key, value in (env or {}).items():
        cmd += ["-e", f"{key}={value}"]
    cmd += ["bench", "python", "-u", *args]
    proc = subprocess.run(
        ["docker", *cmd],
        cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"bench {' '.join(args)} failed (rc={proc.returncode})\n"
            f"stdout: {proc.stdout[-4000:]}\nstderr: {proc.stderr[-4000:]}"
        )
    return proc.stdout


# ---------------------------------------------------------------- Results ---

def write_result(scenario, arm, payload):
    out_dir = RESULTS_DIR / scenario / arm
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "run.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def read_results(scenario=None):
    """Load every run.json, optionally filtered to one scenario."""
    out = {}
    root = RESULTS_DIR / scenario if scenario else RESULTS_DIR
    if not root.exists():
        return out
    for path in sorted(root.rglob("run.json")):
        rel = path.relative_to(RESULTS_DIR)
        out[str(rel.parent)] = json.loads(path.read_text())
    return out


def host_facts():
    """Recorded with every run so a result can be read in context."""
    facts = {"cpus": None, "memory_bytes": None, "docker_version": None}
    try:
        info = json.loads(docker("info", "--format", "{{json .}}", check=False) or "{}")
        facts["cpus"] = info.get("NCPU")
        facts["memory_bytes"] = info.get("MemTotal")
        facts["docker_version"] = info.get("ServerVersion")
    except Exception:
        pass
    return facts

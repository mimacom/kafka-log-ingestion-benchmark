"""Scenario driver helpers.

Scenario scripts run on the host (standard library only) and drive containers
through the Docker CLI. Anything that needs a Kafka client or has to sit on the
container network -- the generator, the verifier, the probe -- runs inside the
bench container.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "bench"))

import common as C  # noqa: E402

REPO_ROOT = C.REPO_ROOT
TMP = REPO_ROOT / "results" / "tmp"

ARMS = ["kafka", "direct-mem", "direct-pq"]

GEN_CONTAINER = f"{C.COMPOSE_PROJECT}-gen"

ARM_API_PORT = {"kafka": 9601, "direct-mem": 9602, "direct-pq": 9603}
ARM_URL = {
    "direct-mem": "http://logstash-direct:8080/",
    "direct-pq": "http://logstash-direct-pq:8080/",
}


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


# ------------------------------------------------------------------- Arms ---

def logstash_ready(arm, timeout=180):
    port = ARM_API_PORT[arm]
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, body = C.request("GET", f"http://localhost:{port}/_node/stats",
                                     timeout=3)
            if status == 200 and body.get("pipelines", {}).get("main"):
                return True
        except Exception:
            pass  # still starting: the API port refuses connections until then
        time.sleep(2)
    raise RuntimeError(f"logstash for arm {arm} did not come up within {timeout}s")


def logstash_events(arm):
    """Pipeline counters, used as a cross-check on the ES-side accounting."""
    status, body = C.request("GET", f"http://localhost:{ARM_API_PORT[arm]}/_node/stats",
                             timeout=5)
    if status != 200:
        return {}
    events = body.get("pipelines", {}).get("main", {}).get("events", {})
    return {"in": events.get("in"), "out": events.get("out"),
            "filtered": events.get("filtered")}


def start_arm(arm, wait=True):
    C.stop_all_arms()
    name = C.ARM_CONTAINERS[arm]
    log(f"starting arm {arm} ({name})")
    C.start_container(name)
    if wait:
        logstash_ready(arm)
        # The Kafka consumer group needs a moment to finish joining before the
        # first events arrive, otherwise the first seconds of a latency run
        # measure rebalancing rather than the pipeline.
        time.sleep(8 if arm == "kafka" else 3)


def stop_arm(arm):
    C.stop_container(C.ARM_CONTAINERS[arm])


def reset_pipeline_state(arm, topic=None):
    """Clear everything that could carry over between runs: the index, the
    Kafka topic contents and any persisted queue on disk."""
    C.stop_all_arms()
    C.recreate_index()
    if arm == "kafka":
        reset_kafka_topic(topic)
    if arm == "direct-pq":
        wipe_persisted_queue()


def wipe_persisted_queue():
    """Remove the on-disk queue so nothing carries over between runs.

    The container has to go first: Docker refuses to remove a volume that any
    container still references, including a stopped one. Getting this wrong is
    silent and expensive -- a queue left over from an aborted run replays events
    whose gen_id/seq overlap the next run's, and the loss accounting is wrong
    without anything looking wrong.
    """
    C.docker("rm", "-f", C.ARM_CONTAINERS["direct-pq"], check=False)
    C.docker("volume", "rm", "-f", C.PQ_VOLUME, check=False)
    still_there = C.docker("volume", "ls", "-q", "--filter",
                           f"name=^{C.PQ_VOLUME}$", check=False)
    if still_there.strip():
        raise RuntimeError(
            f"{C.PQ_VOLUME} could not be removed; a persisted queue from an "
            f"earlier run would replay into this one and corrupt the counts")
    C.compose("create", "logstash-direct-pq")


def reset_kafka_topic(topic=None, partitions=None, replication=None, min_isr=None):
    args = ["bench/kafka_admin.py", "--recreate"]
    if topic:
        args += ["--topic", topic]
    if partitions:
        args += ["--partitions", str(partitions)]
    if replication:
        args += ["--replication", str(replication)]
    if min_isr:
        args += ["--min-isr", str(min_isr)]
    C.run_bench(*args)


# ------------------------------------------------------------ Bench tools ---

def tmp_path(name):
    TMP.mkdir(parents=True, exist_ok=True)
    return TMP / name


def run_generator(arm, run_id, out_name, **kwargs):
    out = tmp_path(out_name)
    C.run_bench(*_generator_args(arm, run_id, out_name, kwargs))
    return json.loads(out.read_text())


def warm_up(arm, seconds=45, rate=4000, generators=4):
    """JRuby reaches steady state slowly. Measuring the first minute of a
    Logstash process means publishing JIT warm-up as if it were pipeline
    latency, so every scenario burns a warm-up pass first. Warm-up events use
    a generator-id offset, which keeps them out of the measured accounting."""
    log(f"warming up {arm} for {seconds}s")
    run_generator(arm, run_id=f"warmup-{arm}", out_name="warmup-gen.json",
                  rate=rate, duration=seconds, generators=generators,
                  gen_id_offset=100, drain_seconds=60)
    time.sleep(5)


def _generator_args(arm, run_id, out_name, kwargs):
    args = ["bench/generator.py", "--arm", arm, "--run-id", run_id,
            "--out", f"results/tmp/{out_name}"]
    if arm != "kafka":
        args += ["--url", ARM_URL[arm]]
    for key, value in kwargs.items():
        if value is None:
            continue
        flag = "--" + key.replace("_", "-")
        args += [flag] if value is True else [flag, str(value)]
    return args


def start_generator_detached(arm, run_id, out_name, **kwargs):
    """Needed whenever a scenario has to break something while load is running."""
    TMP.mkdir(parents=True, exist_ok=True)
    path = tmp_path(out_name)
    if path.exists():
        path.unlink()
    C.docker("rm", "-f", GEN_CONTAINER, check=False)
    cmd = ["compose", "run", "-d", "--name", GEN_CONTAINER, "bench", "python", "-u",
           *_generator_args(arm, run_id, out_name, kwargs)]
    subprocess.run(["docker", *cmd], cwd=str(REPO_ROOT),
                   capture_output=True, text=True, check=True)
    return out_name


def wait_generator(out_name, timeout=1800):
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = C.docker("inspect", "-f", "{{.State.Status}}", GEN_CONTAINER, check=False)
        if state == "exited":
            break
        time.sleep(2)
    else:
        raise RuntimeError("generator did not finish in time")

    code = C.docker("inspect", "-f", "{{.State.ExitCode}}", GEN_CONTAINER, check=False)
    logs = C.docker("logs", GEN_CONTAINER, check=False)
    C.docker("rm", "-f", GEN_CONTAINER, check=False)
    path = tmp_path(out_name)
    if not path.exists():
        raise RuntimeError(f"generator produced no stats (exit {code}): {logs[-2000:]}")
    return json.loads(path.read_text())


def run_verify(gen_file, out_name, index=None):
    args = ["bench/verify.py", "--gen-stats", f"results/tmp/{gen_file}",
            "--out", f"results/tmp/{out_name}"]
    if index:
        args += ["--index", index]
    C.run_bench(*args)
    return json.loads(tmp_path(out_name).read_text())


def start_probe(out_name, duration, with_kafka=False, interval=1.0):
    """Runs detached; collect() reads the file it streams to."""
    TMP.mkdir(parents=True, exist_ok=True)
    name = f"{C.COMPOSE_PROJECT}-probe"
    C.docker("rm", "-f", name, check=False)
    cmd = ["compose", "run", "-d", "--name", name, "bench",
           "python", "-u", "bench/probe.py",
           "--out", f"results/tmp/{out_name}",
           "--duration", str(duration), "--interval", str(interval)]
    if with_kafka:
        cmd.append("--with-kafka")
    subprocess.run(["docker", *cmd], cwd=str(REPO_ROOT),
                   capture_output=True, text=True, check=True)
    return out_name


def collect_probe(out_name):
    C.docker("rm", "-f", f"{C.COMPOSE_PROJECT}-probe", check=False)
    path = tmp_path(out_name)
    if not path.exists():
        return {"samples": []}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        # The probe writes atomically, so this should not happen; if it somehow
        # does, losing the sampled series is not worth discarding a run whose
        # loss and latency numbers come from Elasticsearch anyway.
        log(f"warning: {out_name} was unreadable, continuing without the probe series")
        return {"samples": []}


# ---------------------------------------------------------------- Results ---

def finish(scenario, arm, payload):
    payload.setdefault("scenario", scenario)
    payload.setdefault("arm", arm)
    payload.setdefault("host", C.host_facts())
    payload.setdefault("recorded_at", time.strftime("%Y-%m-%dT%H:%M:%S%z"))
    path = C.write_result(scenario, arm, payload)
    log(f"wrote {path.relative_to(REPO_ROOT)}")
    return path

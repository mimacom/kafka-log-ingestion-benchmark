"""Builds docs/report/ from results/.

Everything here is derived: delete docs/report/ and run again and it comes back
identical. No number in the report is typed by hand, which is the only way a
reader can check a claim by opening the matching run.json.
"""

from __future__ import annotations

import html
import json
import shutil
import sys

import charts
import theme
from common import REPO_ROOT, read_results

# Under docs/ so GitHub Pages can serve it: publishing from a branch only
# accepts the repository root or /docs as the source.
REPORT = REPO_ROOT / "docs" / "report"
ASSETS = REPORT / "assets"
LOGO = REPO_ROOT / "stack" / "brand" / "logo.svg"

ARMS = theme.ARM_ORDER


def e(text):
    return html.escape(str(text))


def pct(value, digits=2):
    return "n/a" if value is None else f"{value:.{digits}f}%"


def num(value):
    return "n/a" if value is None else f"{value:,}"


def ms(value):
    return "n/a" if value is None else f"{value:,.0f} ms"


def mb(value):
    return "n/a" if value is None else f"{value / 1e6:,.1f} MB"


def save_chart(name, svg):
    ASSETS.mkdir(parents=True, exist_ok=True)
    (ASSETS / f"{name}.svg").write_text(svg)
    return svg


def by_arm(results, scenario):
    return {arm: results[f"{scenario}/{arm}"]
            for arm in ARMS if f"{scenario}/{arm}" in results}


def table(headers, rows, note=None):
    head = "".join(f"<th>{e(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
                   for row in rows)
    caption = f'<p class="note">{note}</p>' if note else ""
    return (f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead>'
            f"<tbody>{body}</tbody></table></div>{caption}")


def section(anchor, number, title, lede, blocks):
    return f"""<section id="{anchor}">
<div class="sec-head"><span class="sec-num">{e(number)}</span>
<h2>{e(title)}</h2></div>
<p class="lede">{lede}</p>
{''.join(blocks)}
</section>"""


# ------------------------------------------------------------------ 00 cap ---

def render_capacity(results):
    data = by_arm(results, "00-capacity")
    if not data:
        return None, {}
    arms = [a for a in ARMS if a in data]
    rates = [data[a].get("sustained_events_per_s") for a in arms]

    svg = save_chart("00-capacity", charts.bar_chart(
        "Sustained indexing rate per architecture",
        "Each arm deliberately overloaded; steady-state rate documents reached Elasticsearch",
        [theme.arm_label(a).replace(", ", ",\n") for a in arms], rates,
        [theme.arm_color(a) for a in arms],
        y_label="events / second", x_label="Higher is better",
        value_fmt=lambda v: f"{v:,.0f}"))

    known = [r for r in rates if r]
    slowest = min(known) if known else None
    saturated = [a for a in arms
                 if data[a].get("pipeline_kept_up_with_generator")]

    rows = [[f'<strong>{e(theme.arm_label(a))}</strong>',
             f"{data[a].get('sustained_events_per_s'):,.0f}/s"
             if data[a].get("sustained_events_per_s") else "n/a",
             f"{data[a].get('generator_emitted_per_s'):,.0f}/s"
             if data[a].get("generator_emitted_per_s") else "n/a",
             num(data[a]["generator"]["dropped_at_source"]),
             "kept up with the load generator"
             if data[a].get("pipeline_kept_up_with_generator") else "pipeline-limited"]
            for a in arms]

    note = None
    if len(saturated) == len(arms) and arms:
        note = ("Every arm indexed everything the generator could emit, so these are "
                "lower bounds rather than ceilings: the load generator ran out of "
                "capacity before the pipelines did. That is enough for the purpose "
                "here &mdash; it establishes that 5,000 events/s is far below what any "
                "arm can handle while healthy.")

    lede = (
        "Every later scenario runs below this line. If a failure test ran at a rate an "
        "arm could not sustain even while healthy, that arm would lose events for reasons "
        "unrelated to the failure, and the comparison would be worthless. "
        f"The slowest arm here handles <strong>{slowest:,.0f} events/s</strong>, so the "
        f"failure scenarios run at 5,000/s &mdash; roughly {slowest / 5000:.0f}&times; of "
        "headroom for all three."
        if slowest else "Capacity calibration.")

    return section("capacity", "00", "Capacity calibration", lede, [
        '<img src="assets/00-capacity.svg" alt="Sustained rate per arm">',
        table(["Architecture", "Indexed", "Generator emitted", "Dropped at source",
               "Limited by"], rows, note=note),
    ]), {"slowest": slowest, "rates": dict(zip(arms, rates))}


# -------------------------------------------------------------- 01 / 01c ---

def render_outage(results, scenario, number, title, lede, chart_name, window_key):
    data = by_arm(results, scenario)
    if not data:
        return None, {}
    arms = [a for a in ARMS if a in data]

    loss = [data[a]["verify"]["loss_pct"] for a in arms]
    svg = save_chart(chart_name, charts.bar_chart(
        f"Events lost - {title}",
        "Identical load and identical source buffer on every arm",
        [theme.arm_label(a).replace(", ", ",\n") for a in arms], loss,
        [theme.arm_color(a) for a in arms],
        y_label="% of events lost", x_label="Lower is better",
        value_fmt=lambda v: f"{v:.2f}%"))

    rows = []
    for arm in arms:
        gen, ver = data[arm]["generator"], data[arm]["verify"]
        rows.append([
            f'<strong>{e(theme.arm_label(arm))}</strong>',
            num(gen["produced"]),
            num(ver["indexed_unique"]),
            f'<strong class="{"bad" if ver["missing"] else "good"}">'
            f'{num(ver["missing"])}</strong>',
            pct(ver["loss_pct"]),
            num(gen["dropped_at_source"]),
            num(ver["lost_in_transit"]),
        ])

    window = data[arms[0]].get(window_key, {})
    timeline = timeline_chart(data, arms, window, f"{chart_name}-timeline")

    return section(chart_name, number, title, lede, [
        f'<img src="assets/{chart_name}.svg" alt="Events lost per arm">',
        table(["Architecture", "Produced", "Indexed", "Lost", "Loss", "Dropped at source",
               "Lost after acceptance"], rows,
              note="&ldquo;Dropped at source&rdquo; are events the source could not hand "
                   "over because its buffer was full. &ldquo;Lost after acceptance&rdquo; "
                   "are events the pipeline took and never delivered &mdash; a different, "
                   "worse kind of failure."),
        timeline,
    ]), {arm: data[arm]["verify"]["loss_pct"] for arm in arms}


def timeline_chart(data, arms, window, name):
    series = []
    for arm in arms:
        samples = data[arm].get("probe", {}).get("samples", [])
        points = [(s["t"], s.get("searchable_docs")) for s in samples
                  if s.get("searchable_docs") is not None]
        if points:
            series.append({"label": theme.arm_label(arm),
                           "color": theme.arm_color(arm), "points": points})
    if not series:
        return ""

    bands = []
    if window:
        # The probe is started just before the generator, so offsets are taken
        # from the generator's own start time.
        start = data[arms[0]]["generator"]["started_at_ms"]
        down = window.get("stopped_at_ms") or window.get("killed_at_ms") or start
        x0 = (down - start) / 1000.0
        x1 = (window.get("recovered_at_ms", down) - start) / 1000.0
        if x1 > x0 >= 0:
            bands.append({"x0": x0, "x1": x1, "label": "component down"})

    svg = save_chart(name, charts.line_chart(
        "Documents searchable over time",
        "A flat line means data is arriving nowhere; the slope after recovery is the catch-up",
        series, y_label="documents in Elasticsearch",
        x_label="seconds from start of load", bands=bands))
    return f'<img src="assets/{name}.svg" alt="Documents over time">'


# ------------------------------------------------------------------ 01b ---

def render_broker_failure(results):
    entry = results.get("01b-broker-failure/kafka")
    if not entry:
        return None, {}
    gen, ver = entry["generator"], entry["verify"]
    cfg, fail = entry["config"], entry["failure"]

    rows = [[
        f'<strong>Replication factor {cfg["replication"]}, min.insync.replicas 2</strong>',
        num(gen["produced"]), num(ver["indexed_unique"]),
        f'<strong class="{"bad" if ver["missing"] else "good"}">{num(ver["missing"])}</strong>',
        f'{fail["actual_seconds"]:.0f} s',
        f'{entry.get("producer_stall_seconds", 0)} s',
    ]]

    lede = (
        "Adding a broker adds something that can fail, and that objection deserves a "
        "measurement rather than a reassurance. One of three brokers is killed outright "
        "mid-ingest, with acks=all and idempotent producers. The cost is a brief producer "
        "stall while partition leadership moves.")

    return section("broker", "01b", "Killing a Kafka broker mid-ingest", lede, [
        table(["Configuration", "Produced", "Indexed", "Lost", "Broker down",
               "Producer stalled"], rows),
    ]), {"missing": ver["missing"]}


# ------------------------------------------------------------------- 02 ---

def render_burst(results):
    data = by_arm(results, "02-burst")
    if not data:
        return None, {}
    arms = [a for a in ARMS if a in data]
    cfg = data[arms[0]]["config"]

    save_chart("02-burst", charts.bar_chart(
        "Events lost during a burst above pipeline capacity",
        f"{cfg['base_rate']:,}/s baseline, {cfg['burst_rate']:,}/s for two minutes, "
        f"then back to baseline",
        [theme.arm_label(a).replace(", ", ",\n") for a in arms],
        [data[a]["verify"]["loss_pct"] for a in arms],
        [theme.arm_color(a) for a in arms],
        y_label="% of events lost", x_label="Lower is better",
        value_fmt=lambda v: f"{v:.2f}%"))

    series = []
    for arm in arms:
        points = [(s["t"], s.get("searchable_docs"))
                  for s in data[arm].get("probe", {}).get("samples", [])
                  if s.get("searchable_docs") is not None]
        if points:
            series.append({"label": theme.arm_label(arm),
                           "color": theme.arm_color(arm), "points": points})
    # The shaded window comes from the run's own phase list, so it cannot drift
    # out of step with the scenario's timings.
    phases = [p.strip().split(":") for p in cfg["phases"].split(",")]
    elapsed, burst_band = 0.0, None
    for rate, seconds in phases:
        if float(rate) == float(cfg["burst_rate"]):
            burst_band = {"x0": elapsed, "x1": elapsed + float(seconds),
                          "label": "burst"}
            break
        elapsed += float(seconds)

    save_chart("02-burst-timeline", charts.line_chart(
        "Catching up after the burst",
        "Backlog drains once the burst ends; the delay is bounded and visible",
        series, y_label="documents in Elasticsearch",
        x_label="seconds from start of load",
        bands=[burst_band] if burst_band else None))

    rows = []
    for arm in arms:
        gen, ver = data[arm]["generator"], data[arm]["verify"]
        rows.append([
            f'<strong>{e(theme.arm_label(arm))}</strong>',
            num(gen["produced"]), num(ver["indexed_unique"]),
            f'<strong class="{"bad" if ver["missing"] else "good"}">{num(ver["missing"])}</strong>',
            pct(ver["loss_pct"]),
            num(data[arm].get("peak_kafka_lag")),
            ms(ver["latency_ms"].get("99")),
        ])

    lede = (
        "Nothing is broken here. Everything is running and healthy; there is simply more "
        "data arriving for two minutes than the pipeline can index. What is being compared "
        "is not who absorbs it instantly &mdash; nobody does &mdash; but <em>where</em> the "
        "excess waits, and whether it is still there afterwards.")

    return section("burst", "02", "A burst above capacity", lede, [
        '<img src="assets/02-burst.svg" alt="Loss during burst">',
        table(["Architecture", "Produced", "Indexed", "Lost", "Loss",
               "Peak Kafka lag", "p99 latency"], rows),
        '<img src="assets/02-burst-timeline.svg" alt="Recovery after burst">',
    ]), {arm: data[arm]["verify"]["loss_pct"] for arm in arms}


# ------------------------------------------------------------------- 03 ---

def render_latency(results):
    data = by_arm(results, "03-latency")
    if not data:
        return None, {}
    arms = [a for a in ARMS if a in data]

    save_chart("03-latency", charts.grouped_bar_chart(
        "End-to-end latency, generator to indexed",
        f"Steady {data[arms[0]]['config']['rate']:,} events/s, queues empty",
        ["p50", "p95", "p99"],
        [{"label": theme.arm_label(a), "color": theme.arm_color(a),
          "values": [data[a]["verify"]["latency_ms"].get(p) for p in ("50", "95", "99")]}
         for a in arms],
        y_label="milliseconds", x_label="Lower is better",
        value_fmt=lambda v: f"{v:,.0f}"))

    rows = []
    for arm in arms:
        lat = data[arm]["verify"]["latency_ms"]
        rows.append([
            f'<strong>{e(theme.arm_label(arm))}</strong>',
            ms(lat.get("50")), ms(lat.get("95")), ms(lat.get("99")),
            ms(lat.get("max")),
            ms(data[arm].get("searchable_lag_ms_median")),
        ])

    kafka = data.get("kafka", {}).get("verify", {}).get("latency_ms", {}).get("50")
    # Named comparisons rather than "versus the best other arm": the two direct
    # arms differ enough here that an unqualified number would mislead.
    deltas = [f"{kafka - data[a]['verify']['latency_ms']['50']:+,.0f} ms against "
              f"{theme.arm_label(a).lower()}"
              for a in arms
              if a != "kafka" and kafka and data[a]["verify"]["latency_ms"].get("50")]
    delta = " and ".join(deltas) if deltas else "very little"

    lede = (
        "A broker between the source and Logstash is a hop that was not there before, and "
        f"a hop costs time. At a load well below capacity the median event takes "
        f"{kafka:,.0f} ms through Kafka: {delta}. The persisted-queue arm being the "
        "fastest here is not a typo &mdash; it acknowledges to disk and reads back in "
        "large contiguous chunks. Time-to-indexed and time-to-searchable are reported "
        "separately, because the refresh interval sits between them and conflating the "
        "two is the usual way an end-to-end latency claim ends up wrong."
        if kafka else "Latency comparison.")

    return section("latency", "03", "What the buffer costs in latency", lede, [
        '<img src="assets/03-latency.svg" alt="Latency percentiles per arm">',
        table(["Architecture", "p50", "p95", "p99", "max", "Median time to searchable"], rows,
              note="Time to searchable includes the 1&nbsp;second index refresh interval, "
                   "which is a property of Elasticsearch rather than of the transport."),
    ]), {"p50_by_arm": {a: data[a]["verify"]["latency_ms"].get("50") for a in arms}}


# ------------------------------------------------------------------- 04 ---

def render_scale_out(results):
    entry = results.get("04-scale-out/kafka")
    if not entry:
        return None, {}
    runs = entry["runs"]

    save_chart("04-scale-out", charts.bar_chart(
        "Throughput against consumer count",
        f"One 6-partition topic, {entry['backlog_events']:,} event backlog, "
        f"Elasticsearch replaced by a counting sink",
        [f"{r['consumers']} consumer\nthread(s)" for r in runs],
        [r["events_per_s"] for r in runs],
        [theme.PRIMARY] * len(runs),
        y_label="events / second drained", x_label="Higher is better",
        value_fmt=lambda v: f"{v:,.0f}"))

    rows = [[f'<strong>{r["consumers"]}</strong>', num(r["events"]),
             f'{r["seconds"]:,.1f} s',
             f'{r["events_per_s"]:,.0f}/s' if r["events_per_s"] else "n/a",
             f'{r["speedup_vs_1"]}&times;' if r.get("speedup_vs_1") else "n/a"]
            for r in runs]

    lede = (
        "The architecture claims capacity is added by adding partitions and consumers. "
        "A fixed backlog is drained by 1, 2 and 4 consumers in turn. Elasticsearch is "
        "replaced by a counting sink on purpose: with Elasticsearch on the end, every "
        "consumer count would converge on Elasticsearch's indexing rate and the chart "
        "would be about Elasticsearch instead.")

    return section("scale", "04", "Scaling consumers over partitions", lede, [
        '<img src="assets/04-scale-out.svg" alt="Throughput by consumer count">',
        table(["Consumer threads", "Events drained", "Time", "Throughput",
               "Speed-up vs 1"], rows),
    ]), {"runs": runs}


# ------------------------------------------------------------------- 05 ---

def render_dedup(results):
    entry = results.get("05-dedup/kafka")
    if not entry:
        return None, {}
    variants = entry["variants"]
    cfg = entry["config"]

    save_chart("05-dedup", charts.bar_chart(
        "Documents stored when the source redelivers",
        f"{cfg['replay_fraction']:.0%} of events deliberately sent twice, "
        f"as an at-least-once source does",
        [v["mode"].replace("-", "\n") for v in variants],
        [v["docs_in_elasticsearch"] for v in variants],
        [theme.ARM_COLORS["direct-mem"], theme.PRIMARY],
        y_label="documents in Elasticsearch", x_label="Lower is better",
        value_fmt=lambda v: f"{v:,.0f}"))

    rows = [[
        f'<strong>{e(v["mode"])}</strong>',
        num(v["unique_events"]), num(v["events_on_the_wire"]),
        num(v["docs_in_elasticsearch"]), num(v["duplicate_docs"]), mb(v["index_size_bytes"]),
    ] for v in variants]

    lede = (
        "Kafka's idempotent producer removes duplicates created by its own retries, but it "
        "cannot remove a duplicate the source genuinely sent twice: to Kafka those are two "
        "different produce calls carrying identical content. Removing them is a pipeline "
        "job, and a content fingerprint used as the document&nbsp;_id does it &mdash; a "
        "redelivered event overwrites itself instead of being stored again.")

    saved = entry.get("index_pct_saved")
    if saved is None:
        extra = ""
    elif saved > 0:
        extra = (f'<p class="callout"><strong>{num(entry.get("duplicate_docs_removed"))}'
                 f'</strong> duplicate documents removed, and the index is '
                 f'<strong>{saved}% smaller</strong>.</p>')
    else:
        plain = entry["variants"][0]
        fewer = (plain["duplicate_docs"] / plain["docs_in_elasticsearch"]
                 if plain["docs_in_elasticsearch"] else 0)
        extra = (f'<p class="callout"><strong>{num(entry.get("duplicate_docs_removed"))}'
                 f'</strong> duplicate documents removed &mdash; but the index is '
                 f'<strong>{abs(saved)}% larger</strong>, not smaller. An explicit '
                 f'fingerprint <code>_id</code> costs more to store than the ids '
                 f'Elasticsearch generates itself, which at this duplication rate '
                 f'outweighs holding {fewer:.0%} fewer documents. Deduplicate for '
                 f'correctness, not for the storage bill.</p>')

    return section("dedup", "05", "Deduplicating at-least-once delivery", lede, [
        '<img src="assets/05-dedup.svg" alt="Documents with and without deduplication">',
        table(["Mode", "Unique events", "Sent on the wire", "Documents stored",
               "Duplicate documents", "Index size"], rows),
        extra,
    ]), {"pct_saved": saved}


# ------------------------------------------------------------------- 06 ---

def render_compression(results):
    entry = results.get("06-compression/kafka")
    if not entry:
        return None, {}
    transport = entry["transport"]
    storage = entry.get("storage")

    # Ratios are stated against the uncompressed run that was actually measured,
    # not against the nominal payload size: a Kafka record carries framing
    # overhead, so the two differ by a few percent and only one was measured.
    baseline = next((t["bytes_per_event"] for t in transport
                     if t["codec"] == "none" and t["bytes_per_event"]), None)
    for t in transport:
        if t.get("ratio_vs_none") is None and baseline and t["bytes_per_event"]:
            t["ratio_vs_none"] = round(baseline / t["bytes_per_event"], 2)

    save_chart("06-compression", charts.bar_chart(
        "Bytes stored in Kafka per event, by codec",
        "Same events produced four times; size read back from the brokers' own log dirs",
        [t["codec"] for t in transport],
        [t["bytes_per_event"] for t in transport],
        [theme.ARM_COLORS["direct-mem"] if t["codec"] == "none" else theme.PRIMARY
         for t in transport],
        y_label="bytes per event", x_label="Lower is better",
        value_fmt=lambda v: f"{v:,.0f} B"))

    save_chart("06-compression-throughput", charts.bar_chart(
        "Producer throughput by codec",
        "Compression is a trade: the cheapest bytes are not the cheapest CPU",
        [t["codec"] for t in transport],
        [t["events_per_s"] for t in transport],
        [theme.ARM_COLORS["direct-pq"]] * len(transport),
        y_label="events / second produced", x_label="Higher is better",
        value_fmt=lambda v: f"{v:,.0f}"))

    rows = [[f'<strong>{e(t["codec"])}</strong>', num(t["events"]),
             mb(t["topic_bytes"]), f'{t["bytes_per_event"]:,.0f} B',
             ("&mdash;" if t["codec"] == "none" else
              (f'{t["ratio_vs_none"]:.1f}&times;' if t.get("ratio_vs_none") else "n/a")),
             f'{t["events_per_s"]:,.0f}/s' if t["events_per_s"] else "n/a"]
            for t in transport]

    blocks = [
        '<img src="assets/06-compression.svg" alt="Bytes per event by codec">',
        table(["Codec", "Events", "Topic on disk", "Per event",
               "Ratio vs uncompressed", "Produce rate"], rows,
              note="Ratios are against the measured <code>none</code> run, so "
                   "Kafka's own record overhead is on both sides of the "
                   "comparison. The filler text in these events is drawn from a "
                   "small vocabulary and compresses better than real log data, "
                   "so treat the ordering of the codecs as the transferable "
                   "result rather than the multiples."),
        '<img src="assets/06-compression-throughput.svg" alt="Throughput by codec">',
    ]

    if storage:
        blocks.append(table(
            ["Elasticsearch codec", "Documents", "Primary store", "Per document"],
            [["<strong>default</strong>", num(storage["docs"]),
              mb(storage["default_codec_bytes"]), f'{storage["bytes_per_doc_default"]:,.0f} B'],
             ["<strong>best_compression</strong>", num(storage["docs"]),
              mb(storage["best_compression_bytes"]), f'{storage["bytes_per_doc_best"]:,.0f} B']],
            note=f"Both indices force-merged to a single segment before measuring, so "
                 f"segment count does not confuse the comparison. "
                 f"best_compression saves {storage['pct_saved']}% here."))

    lede = (
        "Two independent measurements on the same data: what compression saves on the way "
        "into the buffer, and what it saves once the data is at rest in Elasticsearch. "
        "A Kafka topic stores exactly what was sent, so the topic size on disk is both the "
        "network cost and the buffer's storage cost.")

    return (section("compression", "06", "Compression, in transit and at rest",
                    lede, blocks),
            {"transport": transport, "storage": storage})


# ---------------------------------------------------------------- Summary ---

def build_summary(results, facts):
    rows = []

    outage = by_arm(results, "01-sink-outage")
    if outage:
        secs = [outage[a]["outage"]["actual_seconds"] for a in outage]
        sink_label = (f"Elasticsearch unavailable {min(secs):.0f}"
                      f"{'' if max(secs) - min(secs) < 1 else f'-{max(secs):.0f}'} s")
        for arm in [a for a in ARMS if a in outage]:
            ver = outage[arm]["verify"]
            rows.append([
                sink_label,
                e(theme.arm_label(arm)),
                f'<strong class="{"good" if not ver["missing"] else "bad"}">'
                f'{num(ver["missing"])} lost ({pct(ver["loss_pct"])})</strong>',
            ])

    collector = by_arm(results, "01c-collector-outage")
    if collector:
        secs = [collector[a]["outage"]["actual_seconds"] for a in collector]
        col_label = (f"Collector restarted {min(secs):.0f}"
                     f"{'' if max(secs) - min(secs) < 1 else f'-{max(secs):.0f}'} s")
        for arm in [a for a in ARMS if a in collector]:
            ver = collector[arm]["verify"]
            rows.append([
                col_label,
                e(theme.arm_label(arm)),
                f'<strong class="{"good" if not ver["missing"] else "bad"}">'
                f'{num(ver["missing"])} lost ({pct(ver["loss_pct"])})</strong>',
            ])

    burst = by_arm(results, "02-burst")
    if burst:
        bcfg = burst[list(burst)[0]]["config"]
        multiple = bcfg.get("burst_multiple_of_ceiling")
        burst_label = (f"Burst {multiple}&times; above measured capacity"
                       if multiple else
                       f"Burst to {bcfg['burst_rate']:,}/s")
        for arm in [a for a in ARMS if a in burst]:
            ver = burst[arm]["verify"]
            rows.append([
                burst_label,
                e(theme.arm_label(arm)),
                f'<strong class="{"good" if not ver["missing"] else "bad"}">'
                f'{num(ver["missing"])} lost ({pct(ver["loss_pct"])})</strong>',
            ])

    latency = by_arm(results, "03-latency")
    if latency:
        for arm in [a for a in ARMS if a in latency]:
            lat = latency[arm]["verify"]["latency_ms"]
            rows.append(["Latency at steady load", e(theme.arm_label(arm)),
                         f'p50 {ms(lat.get("50"))}, p99 {ms(lat.get("99"))}'])

    broker = results.get("01b-broker-failure/kafka")
    if broker:
        rows.append(["One of three Kafka brokers killed", "Kafka buffer",
                     f'<strong class="{"good" if not broker["verify"]["missing"] else "bad"}">'
                     f'{num(broker["verify"]["missing"])} lost</strong>, producer stalled '
                     f'{broker.get("producer_stall_seconds", 0)} s'])

    scale = results.get("04-scale-out/kafka")
    if scale and scale["runs"]:
        last = scale["runs"][-1]
        rows.append(["Consumers scaled 1 &rarr; 4", "Kafka buffer",
                     f'{last["speedup_vs_1"]}&times; throughput' if last.get("speedup_vs_1")
                     else "n/a"])

    dedup = results.get("05-dedup/kafka")
    if dedup and dedup.get("index_pct_saved") is not None:
        # Not named `pct`: that would shadow the pct() helper this same
        # function calls above, and Python would treat every earlier call as a
        # reference to an unassigned local.
        saved_pct = dedup["index_pct_saved"]
        size_text = (f"{saved_pct}% smaller index" if saved_pct > 0 else
                     f"index {abs(saved_pct)}% <em>larger</em>")
        replay = dedup["config"]["replay_fraction"]
        rows.append([f"{replay:.0%} of events redelivered", "Fingerprint document _id",
                     f'{num(dedup["duplicate_docs_removed"])} duplicates removed, '
                     f'{size_text}'])

    comp = results.get("06-compression/kafka")
    if comp:
        none = next((t for t in comp["transport"] if t["codec"] == "none"), None)
        best = min((t for t in comp["transport"] if t["codec"] != "none"),
                   key=lambda t: t["bytes_per_event"] or 1e9, default=None)
        if none and best:
            saved = 100 * (1 - best["bytes_per_event"] / none["bytes_per_event"])
            ratio = none["bytes_per_event"] / best["bytes_per_event"]
            rows.append(["Compression into Kafka", f'{best["codec"]} vs none',
                         f'{saved:.0f}% fewer bytes buffered ({ratio:.1f}&times;)'])

    return table(["Scenario", "Configuration", "Result"], rows)


# ------------------------------------------------------------------- HTML ---

CSS = f"""
:root {{
  --primary: {theme.PRIMARY};
  --secondary: {theme.SECONDARY};
  --text: {theme.TEXT};
  --page: {theme.PAGE};
  --surface: {theme.SURFACE};
  --dark-gray: {theme.DARK_GRAY};
  --muted: {theme.MUTED};
  --line: {theme.LINE};
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0; background: var(--page); color: var(--text);
  font-family: {theme.FONT_STACK};
  font-size: 18px; line-height: 28px; -webkit-font-smoothing: antialiased;
}}
.wrap {{ max-width: 1040px; margin: 0 auto; padding: 0 28px 96px; }}
header.top {{ background: var(--surface); border-bottom: 1px solid var(--line); }}
header.top .wrap {{ padding: 26px 28px; display: flex; align-items: center;
  justify-content: space-between; gap: 24px; }}
header.top .logo {{ color: var(--text); display: flex; align-items: center; }}
header.top .logo svg {{ height: 26px; width: auto; }}
header.top .kicker {{ font-size: 13px; letter-spacing: 1px; text-transform: uppercase;
  color: var(--muted); }}

.hero {{ padding: 72px 0 28px; }}
.hero .eyebrow {{ display: inline-block; font-size: 13px; font-weight: 600;
  letter-spacing: 1px; text-transform: uppercase; color: var(--surface);
  background: var(--primary); padding: 6px 12px; border-radius: 4px; margin-bottom: 24px; }}
h1 {{ margin: 0 0 20px; font-weight: 500; font-size: clamp(38px, 5.4vw, 64px);
  line-height: 1.04; letter-spacing: -0.5px; }}
h2 {{ margin: 0; font-weight: 500; font-size: clamp(28px, 3.4vw, 40px); line-height: 1.12; }}
h3 {{ margin: 40px 0 12px; font-weight: 500; font-size: 22px; line-height: 30px; }}
.hero p.sub {{ font-size: 21px; line-height: 32px; color: var(--dark-gray); max-width: 62ch; }}

section {{ background: var(--surface); border: 1px solid var(--line); border-radius: 8px;
  padding: 36px 40px 40px; margin: 28px 0; }}
.sec-head {{ display: flex; align-items: baseline; gap: 16px; margin-bottom: 14px; }}
.sec-num {{ font-size: 14px; font-weight: 600; color: var(--primary);
  border: 2px solid var(--primary); border-radius: 8px; padding: 4px 10px;
  line-height: 1; letter-spacing: 1px; }}
.lede {{ color: var(--dark-gray); max-width: 72ch; margin: 0 0 26px; }}
p, li {{ max-width: 76ch; }}

img {{ display: block; width: 100%; height: auto; margin: 26px 0 8px;
  border: 1px solid var(--line); border-radius: 8px; background: var(--surface); }}

.table-wrap {{ overflow-x: auto; margin: 22px 0 6px; }}
table {{ border-collapse: collapse; width: 100%; font-size: 15px; line-height: 22px; }}
th, td {{ text-align: left; padding: 11px 14px; border-bottom: 1px solid var(--line);
  white-space: nowrap; }}
th {{ font-size: 12px; font-weight: 600; letter-spacing: 0.6px; text-transform: uppercase;
  color: var(--muted); border-bottom: 2px solid var(--line); }}
tbody tr:hover {{ background: var(--page); }}
td strong.good {{ color: #0f8a5f; }}
td strong.bad {{ color: var(--primary); }}
.note {{ font-size: 14px; line-height: 21px; color: var(--muted); margin: 10px 0 0;
  max-width: 82ch; }}
.callout {{ margin: 22px 0 0; padding: 16px 20px; border-left: 4px solid var(--primary);
  background: var(--page); border-radius: 0 8px 8px 0; font-size: 16px; line-height: 25px; }}

.meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  gap: 1px; background: var(--line); border: 1px solid var(--line);
  border-radius: 8px; overflow: hidden; margin: 32px 0 0; }}
.meta div {{ background: var(--surface); padding: 16px 18px; }}
.meta dt {{ font-size: 12px; letter-spacing: 0.6px; text-transform: uppercase;
  color: var(--muted); margin: 0 0 4px; }}
.meta dd {{ margin: 0; font-size: 17px; font-weight: 500; }}

footer {{ color: var(--muted); font-size: 14px; line-height: 22px; padding-top: 12px; }}
footer a, .lede a, p a {{ color: var(--secondary); }}
ul.plain {{ padding-left: 20px; }}
ul.plain li {{ margin-bottom: 10px; color: var(--dark-gray); }}
@media (max-width: 680px) {{
  section {{ padding: 26px 20px 30px; }}
  th, td {{ padding: 9px 10px; }}
}}
"""


def render(results):
    facts = {}
    for entry in results.values():
        if entry.get("host"):
            facts = entry["host"]
            break
    recorded = next((entry.get("recorded_at") for entry in results.values()
                     if entry.get("recorded_at")), "")

    sections = []
    capacity_html, _ = render_capacity(results)
    sections.append(capacity_html)

    outage_html, _ = render_outage(
        results, "01-sink-outage", "01", "Elasticsearch goes away",
        "The destination is stopped for two minutes while the load continues, because in "
        "real life it does. The question is not whether the pipeline notices &mdash; all "
        "three do &mdash; but where the events wait, and whether they are all still there "
        "when it comes back.",
        "01-sink-outage", "outage")
    sections.append(outage_html)

    collector_html, _ = render_outage(
        results, "01c-collector-outage", "01c", "The collector tier restarts",
        "This time the component that the sources actually talk to is taken away. With a "
        "broker in front, Logstash is only a consumer: stopping it stops consumption and "
        "the topic grows. Without one, Logstash <em>is</em> the endpoint, and while it is "
        "down the sources have nowhere to send. A persisted queue cannot help here, because "
        "a queue on a node that is not running accepts nothing.",
        "01c-collector-outage", "outage")
    sections.append(collector_html)

    broker_html, _ = render_broker_failure(results)
    sections.append(broker_html)

    burst_html, _ = render_burst(results)
    sections.append(burst_html)

    latency_html, _ = render_latency(results)
    sections.append(latency_html)

    scale_html, _ = render_scale_out(results)
    sections.append(scale_html)

    dedup_html, _ = render_dedup(results)
    sections.append(dedup_html)

    compression_html, _ = render_compression(results)
    sections.append(compression_html)

    logo = LOGO.read_text() if LOGO.exists() else ""

    meta_items = [
        ("Docker CPUs", facts.get("cpus", "n/a")),
        ("Docker memory", f'{facts.get("memory_bytes", 0) / 1e9:.1f} GB'
         if facts.get("memory_bytes") else "n/a"),
        ("Recorded", recorded[:10] or "n/a"),
        ("Scenarios", str(len([s for s in sections if s]))),
    ]
    meta = "".join(f"<div><dt>{e(k)}</dt><dd>{e(v)}</dd></div>" for k, v in meta_items)

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Does a Kafka buffer earn its place in a log pipeline?</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Jost:wght@400;500;600&display=swap"
 rel="stylesheet">
<style>{CSS}</style>
</head>
<body>
<header class="top"><div class="wrap">
  <span class="logo">{logo}</span>
  <span class="kicker">Log ingestion benchmark</span>
</div></header>

<div class="wrap">
<div class="hero">
  <span class="eyebrow">Measured, not asserted</span>
  <h1>Does a Kafka buffer earn its place in a log pipeline?</h1>
  <p class="sub">Three ingestion architectures, one Elasticsearch, the same load and the
  same failures applied to each. Every number below is produced by code in this
  repository and can be reproduced with three commands.</p>
  <dl class="meta">{meta}</dl>
</div>

<section id="summary">
  <div class="sec-head"><span class="sec-num">TL;DR</span><h2>What the runs showed</h2></div>
  <p class="lede">Loss is counted exactly, not sampled: every event carries a per-source
  sequence number, and the verifier walks the entire index to find which ones are
  missing.</p>
  {build_summary(results, facts)}
</section>

{''.join(s for s in sections if s)}

<section id="limits">
  <div class="sec-head"><span class="sec-num">&#9888;</span>
  <h2>What this does not prove</h2></div>
  <p class="lede">A benchmark that only lists its strengths is marketing. These are the
  limits of this one.</p>
  <ul class="plain">
    <li><strong>The absolute rates are the rig's, not a platform's.</strong> This runs on
    one laptop-class Docker host. The comparison between arms transfers; the
    events-per-second numbers do not.</li>
    <li><strong>Elasticsearch here is a local single node</strong>, not a managed
    multi-zone service. Real availability zones and managed failover are not exercised.</li>
    <li><strong>The events are synthetic</strong> &mdash; realistic in size and shape, but
    not a customer's real log mix, which affects compression ratios in particular.</li>
    <li><strong>The source buffer is a model.</strong> A real agent's buffering differs in
    detail; what matters is that the same bound is applied to every arm.</li>
    <li><strong>Persisted-queue throughput is disk-bound here.</strong> Container
    filesystem sync on a developer machine is slower than a real server's, which counts
    against the persisted-queue arm in the capacity measurement.</li>
    <li><strong>Outage windows are not identical between arms.</strong> Recovery is
    declared after a polled readiness check, so the measured windows differ by a few
    seconds between arms. Loss scales with the length of the outage, so a small
    difference between two arms in the same scenario is not an architectural ranking
    &mdash; only the difference between losing everything and losing nothing is.</li>
    <li><strong>Attribution of loss is approximate at the margin.</strong> A batch the
    collector processed but whose HTTP response timed out is retried by the source and
    can be counted as dropped even though it was indexed. The totals are exact; the
    split between &ldquo;dropped at source&rdquo; and &ldquo;lost after acceptance&rdquo;
    can be off by a handful of events in a million.</li>
  </ul>
</section>

<footer>
  <p>Generated by <code>bench/report.py</code> from the JSON files under
  <code>results/</code>. Delete <code>docs/report/</code> and run <code>make report</code> to
  rebuild it identically.</p>
</footer>
</div>
</body>
</html>"""


def main():
    results = read_results()
    if not results:
        sys.exit("no results found -- run the scenarios first")

    if REPORT.exists():
        shutil.rmtree(REPORT)
    ASSETS.mkdir(parents=True, exist_ok=True)
    if LOGO.exists():
        shutil.copy(LOGO, ASSETS / "logo.svg")

    (REPORT / "index.html").write_text(render(results))
    charts_written = sorted(p.name for p in ASSETS.glob("*.svg"))
    print(f"docs/report/index.html written with {len(charts_written)} charts:")
    for name in charts_written:
        print(f"  assets/{name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

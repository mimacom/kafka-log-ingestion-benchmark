"""Hand-built SVG charts.

No plotting library: the charts are written directly as SVG so they can be
inlined into the HTML report and committed as files the README renders, from
one renderer with no binary artefacts and no extra dependency to install.
"""

from __future__ import annotations

import math

import theme

W, H = 820, 380
PAD_L, PAD_R, PAD_T, PAD_B = 96, 26, 82, 58


def esc(text):
    return (str(text).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def nice_ticks(lo, hi, count=5):
    """Round tick values, so axes read 0/50/100 rather than 0/47/94."""
    if hi <= lo:
        hi = lo + 1
    span = hi - lo
    raw = span / max(1, count)
    magnitude = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    for multiple in (1, 2, 2.5, 5, 10):
        step = magnitude * multiple
        if step >= raw:
            break
    value = math.floor(lo / step) * step
    ticks = []
    # Runs until the last tick is at or above `hi`, so the axis always covers
    # the data. Stopping one tick early silently clips the tallest bar.
    while True:
        ticks.append(round(value, 10))
        if value >= hi - step * 1e-9:
            return ticks
        value += step


def fmt_number(value):
    if value is None:
        return "n/a"
    if isinstance(value, float) and value != int(value):
        if abs(value) >= 100:
            return f"{value:,.0f}"
        if abs(value) >= 1:
            return f"{value:,.1f}"
        return f"{value:,.2f}"
    return f"{int(value):,}"


def _frame(title, subtitle, y_label, x_label, body, width=W, height=H):
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" \
width="{width}" height="{height}" role="img" aria-label="{esc(title)}" \
font-family='{theme.FONT_STACK}'>
<rect width="{width}" height="{height}" fill="{theme.SURFACE}"/>
<text x="{PAD_L - 48}" y="30" font-size="17" font-weight="500" \
fill="{theme.TEXT}">{esc(title)}</text>
<text x="{PAD_L - 48}" y="50" font-size="13" fill="{theme.MUTED}">{esc(subtitle)}</text>
<text x="{PAD_L - 48}" y="{height - 12}" font-size="12" fill="{theme.MUTED}">{esc(x_label)}</text>
<text transform="translate(17,{PAD_T + 4}) rotate(-90)" font-size="12" \
fill="{theme.MUTED}" text-anchor="end">{esc(y_label)}</text>
{body}
</svg>"""


def bar_chart(title, subtitle, categories, values, colors=None, y_label="",
              x_label="", value_fmt=fmt_number, width=W, height=H, y_max=None):
    plot_w = width - PAD_L - PAD_R
    plot_h = height - PAD_T - PAD_B
    clean = [v for v in values if v is not None]
    top = y_max if y_max is not None else (max(clean) if clean else 1)
    ticks = nice_ticks(0, top or 1)
    top = max(ticks) or 1

    parts = []
    for tick in ticks:
        y = PAD_T + plot_h - (tick / top) * plot_h
        parts.append(f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{PAD_L + plot_w}" '
                     f'y2="{y:.1f}" stroke="{theme.LINE}" stroke-width="1"/>')
        parts.append(f'<text x="{PAD_L - 10}" y="{y + 4:.1f}" font-size="11.5" '
                     f'fill="{theme.MUTED}" text-anchor="end">{fmt_number(tick)}</text>')

    slot = plot_w / max(1, len(categories))
    bar_w = min(96, slot * 0.56)
    for index, (label, value) in enumerate(zip(categories, values)):
        cx = PAD_L + slot * (index + 0.5)
        color = (colors or [theme.PRIMARY] * len(categories))[index]
        if value is None:
            parts.append(f'<text x="{cx:.1f}" y="{PAD_T + plot_h - 8:.1f}" '
                         f'font-size="12" fill="{theme.MUTED}" '
                         f'text-anchor="middle">n/a</text>')
        else:
            bar_h = max(1.5, (value / top) * plot_h) if top else 1.5
            y = PAD_T + plot_h - bar_h
            parts.append(f'<rect x="{cx - bar_w / 2:.1f}" y="{y:.1f}" '
                         f'width="{bar_w:.1f}" height="{bar_h:.1f}" '
                         f'fill="{color}" rx="3"/>')
            parts.append(f'<text x="{cx:.1f}" y="{y - 8:.1f}" font-size="13" '
                         f'font-weight="500" fill="{theme.TEXT}" '
                         f'text-anchor="middle">{esc(value_fmt(value))}</text>')
        for line_no, chunk in enumerate(str(label).split("\n")):
            parts.append(f'<text x="{cx:.1f}" y="{PAD_T + plot_h + 20 + line_no * 14:.1f}" '
                         f'font-size="12" fill="{theme.DARK_GRAY}" '
                         f'text-anchor="middle">{esc(chunk)}</text>')

    parts.append(f'<line x1="{PAD_L}" y1="{PAD_T + plot_h}" x2="{PAD_L + plot_w}" '
                 f'y2="{PAD_T + plot_h}" stroke="{theme.DARK_GRAY}" stroke-width="1.2"/>')
    return _frame(title, subtitle, y_label, x_label, "\n".join(parts), width, height)


def grouped_bar_chart(title, subtitle, groups, series, y_label="", x_label="",
                      value_fmt=fmt_number, width=W, height=H, y_max=None):
    """`groups` are the x categories; `series` is [{label, color, values}] with
    one value per group."""
    plot_w = width - PAD_L - PAD_R
    plot_h = height - PAD_T - PAD_B
    clean = [v for s in series for v in s["values"] if v is not None]
    top = y_max if y_max is not None else (max(clean) if clean else 1)
    ticks = nice_ticks(0, top or 1)
    top = max(ticks) or 1

    parts = []
    for tick in ticks:
        y = PAD_T + plot_h - (tick / top) * plot_h
        parts.append(f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{PAD_L + plot_w}" '
                     f'y2="{y:.1f}" stroke="{theme.LINE}" stroke-width="1"/>')
        parts.append(f'<text x="{PAD_L - 10}" y="{y + 4:.1f}" font-size="11.5" '
                     f'fill="{theme.MUTED}" text-anchor="end">{fmt_number(tick)}</text>')

    slot = plot_w / max(1, len(groups))
    inner = slot * 0.72
    bar_w = inner / max(1, len(series))
    for gi, label in enumerate(groups):
        base = PAD_L + slot * gi + (slot - inner) / 2
        for si, entry in enumerate(series):
            value = entry["values"][gi]
            x = base + bar_w * si
            if value is None:
                continue
            bar_h = max(1.5, (value / top) * plot_h)
            y = PAD_T + plot_h - bar_h
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(2, bar_w - 4):.1f}" '
                         f'height="{bar_h:.1f}" fill="{entry["color"]}" rx="2.5"/>')
            parts.append(f'<text x="{x + (bar_w - 4) / 2:.1f}" y="{y - 6:.1f}" '
                         f'font-size="11" fill="{theme.DARK_GRAY}" '
                         f'text-anchor="middle">{esc(value_fmt(value))}</text>')
        for line_no, chunk in enumerate(str(label).split("\n")):
            parts.append(f'<text x="{PAD_L + slot * (gi + 0.5):.1f}" '
                         f'y="{PAD_T + plot_h + 20 + line_no * 14:.1f}" font-size="12" '
                         f'fill="{theme.DARK_GRAY}" text-anchor="middle">{esc(chunk)}</text>')

    parts.append(f'<line x1="{PAD_L}" y1="{PAD_T + plot_h}" x2="{PAD_L + plot_w}" '
                 f'y2="{PAD_T + plot_h}" stroke="{theme.DARK_GRAY}" stroke-width="1.2"/>')

    x = PAD_L + plot_w
    for entry in reversed(series):
        est_w = 8 * 0.62 * len(entry["label"]) + 26
        x -= est_w
        parts.append(f'<rect x="{x:.1f}" y="{PAD_T - 26}" width="12" height="12" '
                     f'rx="2.5" fill="{entry["color"]}"/>')
        parts.append(f'<text x="{x + 18:.1f}" y="{PAD_T - 16}" font-size="12" '
                     f'fill="{theme.DARK_GRAY}">{esc(entry["label"])}</text>')
        x -= 12

    return _frame(title, subtitle, y_label, x_label, "\n".join(parts), width, height)


def line_chart(title, subtitle, series, y_label="", x_label="", bands=None,
               width=W, height=H, y_max=None, y_log=False, value_fmt=fmt_number):
    """`series` is [{label, color, points: [(x, y), ...]}]; `bands` shade an
    interval of the x axis, which is how failure windows are marked."""
    plot_w = width - PAD_L - PAD_R
    plot_h = height - PAD_T - PAD_B

    xs = [p[0] for s in series for p in s["points"]]
    ys = [p[1] for s in series for p in s["points"] if p[1] is not None]
    if not xs or not ys:
        return _frame(title, subtitle, y_label, x_label,
                      f'<text x="{width / 2}" y="{height / 2}" font-size="13" '
                      f'fill="{theme.MUTED}" text-anchor="middle">no data</text>',
                      width, height)

    x_lo, x_hi = min(xs), max(xs)
    if x_hi <= x_lo:
        x_hi = x_lo + 1
    y_hi = y_max if y_max is not None else max(ys)
    ticks = nice_ticks(0, y_hi or 1)
    y_hi = max(ticks) or 1

    def px(x):
        return PAD_L + (x - x_lo) / (x_hi - x_lo) * plot_w

    def py(y):
        return PAD_T + plot_h - (min(y, y_hi) / y_hi) * plot_h

    parts = []
    for band in (bands or []):
        x0, x1 = px(band["x0"]), px(band["x1"])
        parts.append(f'<rect x="{x0:.1f}" y="{PAD_T}" width="{max(1, x1 - x0):.1f}" '
                     f'height="{plot_h}" fill="{theme.PRIMARY}" opacity="0.07"/>')
        parts.append(f'<text x="{(x0 + x1) / 2:.1f}" y="{PAD_T - 4}" font-size="11.5" '
                     f'fill="{theme.SECONDARY}" text-anchor="middle">'
                     f'{esc(band.get("label", ""))}</text>')

    for tick in ticks:
        y = py(tick)
        parts.append(f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{PAD_L + plot_w}" '
                     f'y2="{y:.1f}" stroke="{theme.LINE}" stroke-width="1"/>')
        parts.append(f'<text x="{PAD_L - 10}" y="{y + 4:.1f}" font-size="11.5" '
                     f'fill="{theme.MUTED}" text-anchor="end">{fmt_number(tick)}</text>')

    for tick in nice_ticks(x_lo, x_hi, 6):
        if tick < x_lo or tick > x_hi:
            continue
        x = px(tick)
        parts.append(f'<text x="{x:.1f}" y="{PAD_T + plot_h + 20:.1f}" font-size="11.5" '
                     f'fill="{theme.MUTED}" text-anchor="middle">{fmt_number(tick)}</text>')

    for entry in series:
        points = [(px(x), py(y)) for x, y in entry["points"] if y is not None]
        if not points:
            continue
        path = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}"
                        for i, (x, y) in enumerate(points))
        parts.append(f'<path d="{path}" fill="none" stroke="{entry["color"]}" '
                     f'stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')

    parts.append(f'<line x1="{PAD_L}" y1="{PAD_T + plot_h}" x2="{PAD_L + plot_w}" '
                 f'y2="{PAD_T + plot_h}" stroke="{theme.DARK_GRAY}" stroke-width="1.2"/>')

    # Legend, top right, one row.
    x = PAD_L + plot_w
    for entry in reversed(series):
        label = entry["label"]
        est_w = 8 * 0.62 * len(label) + 26
        x -= est_w
        parts.append(f'<rect x="{x:.1f}" y="{PAD_T - 20}" width="14" height="4" '
                     f'rx="2" fill="{entry["color"]}"/>')
        parts.append(f'<text x="{x + 20:.1f}" y="{PAD_T - 15}" font-size="12" '
                     f'fill="{theme.DARK_GRAY}">{esc(label)}</text>')
        x -= 12

    return _frame(title, subtitle, y_label, x_label, "\n".join(parts), width, height)

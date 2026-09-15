"""Visual tokens for the generated report.

Colours and type scale are taken from the live mimacom.com stylesheet rather
than eyeballed, so the report reads as part of the same system.

Neue-Campton is a licensed face and is not redistributed here. Jost is the
closest freely available geometric sans; anyone with the corporate font
installed will get the real one from the first entry in the stack.
"""

from __future__ import annotations

PRIMARY = "#ff0651"
SECONDARY = "#b80037"
TEXT = "#242424"
PAGE = "#f4f5f6"
SURFACE = "#ffffff"
DARK_GRAY = "#414141"
MUTED = "#787878"
LINE = "#d8dade"

FONT_STACK = '"Neue-Campton", Jost, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif'
FONT_MONO = 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, monospace'

# The Kafka arm is the one carrying the argument, so it gets the brand colour
# and the others get greys. Hue and lightness both differ, which keeps the
# charts readable in greyscale and for colour-blind readers.
ARM_COLORS = {
    "kafka": PRIMARY,
    "direct-pq": "#5b6b7f",
    "direct-mem": "#a8b0ba",
}

ARM_LABELS = {
    "kafka": "Kafka buffer",
    "direct-pq": "Direct, persisted queue",
    "direct-mem": "Direct, in-memory queue",
}

ARM_ORDER = ["kafka", "direct-pq", "direct-mem"]

GOOD = "#0f8a5f"
BAD = PRIMARY
NEUTRAL = MUTED


def arm_color(arm):
    return ARM_COLORS.get(arm, MUTED)


def arm_label(arm):
    return ARM_LABELS.get(arm, arm)

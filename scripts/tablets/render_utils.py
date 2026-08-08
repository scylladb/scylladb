#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Shared terminal rendering helpers.
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from dataclasses import dataclass
from typing import Any
from tablets.topology import Host
from tablets.topology import RackId

from tabulate import SEPARATING_LINE
from tabulate import tabulate

HISTOGRAM_WIDTH = 16
PARTIAL_BLOCKS = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"]

# Do not use whitespace as the zero-height glyph: tabulate trims leading
# whitespace in cells, which distorts the histogram.
# Keep the tallest glyph at ▇ to leave one line of headroom.
SPARKLINE_BLOCKS = "▁▁▂▃▄▅▆▇"
ZERO_GLYPH = SPARKLINE_BLOCKS[0]

ANSI_BLUE = "\033[34m"
ANSI_GREEN = "\033[32m"
ANSI_DARK_GRAY = "\033[90m"
ANSI_RESET = "\033[0m"
ENABLE_COLOR = sys.stdout.isatty()
ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


@dataclass(frozen=True)
class PresentationOptions:
    """
    What the shared presentation flags selected. See add_presentation_options().
    """

    # Render as CSV rather than a table, dropping bar columns.
    csv: bool = False
    # Format sizes with binary units instead of raw bytes.
    human_readable: bool = True
    # Identify a host by its id rather than its ip.
    host_id: bool = False
    # Identify a table by its id rather than keyspace.table.
    table_id: bool = False


DEFAULT_PRESENTATION = PresentationOptions()


def format_host(host: Host, options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Renders a host the way a report identifies one: its ip, or its id when asked for or when
    the host has no ip.
    """
    if options.host_id or not host.ip:
        return str(host.id)
    return host.ip


def add_presentation_options(parser: argparse.ArgumentParser, *,
                             has_hosts: bool = False, has_tables: bool = False) -> None:
    parser.add_argument("--csv", action="store_true", help="Render tabular output as CSV")
    parser.add_argument("--hr", action=argparse.BooleanOptionalAction, default=True,
                        help="Format sizes with binary units (default: enabled)")
    if has_hosts:
        parser.add_argument("--host-id", action="store_true", help="Show host id instead of ip")
    if has_tables:
        parser.add_argument("--table-id", action="store_true", help="Print table id instead of keyspace.table in the first column")


def get_presentation_options_from_args(args: argparse.Namespace) -> PresentationOptions:
    """
    Reads the presentation flags a parser was given, defaulting the ones it left out.
    """
    return PresentationOptions(
        csv=getattr(args, "csv", DEFAULT_PRESENTATION.csv),
        human_readable=getattr(args, "hr", DEFAULT_PRESENTATION.human_readable),
        host_id=getattr(args, "host_id", DEFAULT_PRESENTATION.host_id),
        table_id=getattr(args, "table_id", DEFAULT_PRESENTATION.table_id),
    )


def strip_ansi(value: str) -> str:
    return ANSI_ESCAPE_RE.sub("", value)


def normalize_csv_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, str):
        return strip_ansi(value)
    return value


def normalize_csv_header(value: Any) -> str:
    return strip_ansi(str(value)).replace("\n", " ")


def render_table(rows: list[Any], headers: list[Any], *, csv_output: bool = False,
                 colalign: list[str] | None = None) -> str:
    # Number parsing is always disabled: cells arrive pre-formatted, and letting
    # tabulate re-parse them drops the OVC sign and trailing zeros ("+1.60" -> "1.6").
    if not csv_output:
        # tabulate sizes its alignment list from the rows, so passing colalign for a
        # table a filter emptied would index past it. Headers alone still render.
        return tabulate(rows, headers, tablefmt="psql", colalign=colalign if rows else None,
                        disable_numparse=True)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([normalize_csv_header(header) for header in headers])
    for row in rows:
        if row == SEPARATING_LINE:
            continue
        writer.writerow([normalize_csv_cell(cell) for cell in row])
    return output.getvalue().rstrip("\r\n")


@dataclass(frozen=True)
class Column:
    """
    Describes one table column: its header, alignment, and whether it appears in CSV output.

    Bar/histogram columns set ``csv=False`` so they are dropped from CSV output,
    where their sibling numeric columns carry the same information.
    """
    header: str = ""
    align: str = "left"
    csv: bool = True


def print_table(rows: list[Any], columns: list[Column],
                options: PresentationOptions = DEFAULT_PRESENTATION) -> bool:
    """
    Renders ``rows`` using column metadata, deriving headers, alignment, and CSV
    column filtering from a single list of :class:`Column` descriptors.

    Columns with ``csv=False`` (typically bar/histogram columns) are dropped from
    CSV output, where their sibling numeric columns carry the same information.
    """
    if options.csv:
        kept = [idx for idx, col in enumerate(columns) if col.csv]
        columns = [columns[idx] for idx in kept]
        rows = [row if row == SEPARATING_LINE else [row[idx] for idx in kept] for row in rows]

    try:
        print(render_table(
            rows,
            [col.header for col in columns],
            csv_output=options.csv,
            colalign=[col.align for col in columns],
        ))
    except BrokenPipeError:
        return False
    return True


def render_hbar(frac: float, max_frac: float, width: int = HISTOGRAM_WIDTH) -> str:
    """
    Renders a fixed-width horizontal bar.

    Args:
        frac: Value to render.
        max_frac: Reference value that fills the whole bar.
        width: Bar width in terminal cells.

    Returns:
        A left-aligned Unicode block bar padded to ``width``.

    Examples:
        >>> render_hbar(0.5, 1.0, width=4)
        '██  '
    """
    cells = (float(frac) * width / max_frac) if max_frac else 0
    full = int(cells)
    partial = int((cells - full) * len(PARTIAL_BLOCKS))
    bar = "█" * full
    if partial > 0 and full < width:
        bar += PARTIAL_BLOCKS[partial]
    return bar.ljust(width)


def format_size(size: int | float, options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Formats a size in bytes, with binary units unless the report asked for raw ones.

    Examples:
        >>> format_size(1536)
        '1.500 Ki'
        >>> format_size(1536, PresentationOptions(human_readable=False))
        '1536'
    """
    if not options.human_readable:
        if isinstance(size, int) or (isinstance(size, float) and size.is_integer()):
            return str(int(size))
        return f"{size:.3f}"

    value = float(size)
    for unit in ["B", "Ki", "Mi", "Gi", "Ti"]:
        if value < 1024.0 or unit == "Ti":
            padded_unit = f"{unit:>2}"
            if unit == "B":
                return f"{int(value)} {padded_unit}"
            return f"{value:.3f} {padded_unit}"
        value /= 1024.0


def format_rack_id(rack_id: RackId) -> str:
    return f"{rack_id[0]}/{rack_id[1]}"


def color_replica_histogram(bar: str, replica_idx: int) -> str:
    """
    Applies replica-specific ANSI coloring to a rendered bar fragment.

    Replica 0 keeps the default color, replica 1 is light blue, and higher
    replica indexes are light green.
    """
    if not ENABLE_COLOR:
        return bar
    if replica_idx == 0:
        return bar
    if replica_idx == 1:
        return f"{ANSI_BLUE}{bar}{ANSI_RESET}"
    return f"{ANSI_GREEN}{bar}{ANSI_RESET}"


def color_zero_glyph(glyph: str) -> str:
    """
    Highlights a zero-height sparkline glyph in dark gray.
    """
    if not ENABLE_COLOR:
        return glyph
    return f"{ANSI_DARK_GRAY}{glyph}{ANSI_RESET}"


def format_ovc_pct(value: float | None) -> str:
    """
    Formats overcommit as a percentage deviation from 1.0.

    Coloring is based on absolute deviation:
    - < 3%: default
    - < 10%: yellow/orange
    - >= 10%: red

    Examples:
        >>> format_ovc_pct(1.05)
        '+5.00'
    """
    if value is None:
        return ""
    pct = (value - 1) * 100
    text = f"{pct:+.2f}"
    if not ENABLE_COLOR:
        return text
    abs_pct = abs(pct)
    if abs_pct < 3:
        return text
    if abs_pct < 10:
        return f"\033[33m{text}\033[0m"
    return f"\033[31m{text}\033[0m"


def format_util_pct(value: float) -> str:
    """
    Formats utilization as a percentage.

    Coloring is based on utilization level:
    - < 75%: default
    - < 85%: yellow/orange
    - >= 85%: red
    """
    text = f"{value:.2f}"
    if not ENABLE_COLOR:
        return text
    if value < 75:
        return text
    if value < 85:
        return f"\033[33m{text}\033[0m"
    return f"\033[31m{text}\033[0m"


def format_tablets_per_shard(value: float | None) -> str:
    """
    Formats tablets-per-shard with threshold-based coloring.

    Coloring is based on the absolute value:
    - < 200: default
    - < 500: yellow/orange
    - >= 500: red
    """
    if value is None:
        return ""
    text = f"{value:.3f}"
    if not ENABLE_COLOR:
        return text
    if value < 200:
        return text
    if value < 500:
        return f"\033[33m{text}\033[0m"
    return f"\033[31m{text}\033[0m"


def format_shard_location(host_part: str, shard_id: int) -> str:
    """
    Formats a host/shard location label.

    Examples:
        >>> format_shard_location('10.0.0.1', 3)
        '10.0.0.1:3'
    """
    return f"{host_part}:{shard_id}"


def render_vbar(value: int | float, max_value: int | float, blocks: str = SPARKLINE_BLOCKS, color_zero: bool = True,
                level_adjust: int = 0) -> str:
    """
    Renders a single vertical bar glyph chosen from ``blocks``.

    Args:
        value: Value to render.
        max_value: Reference value that maps to the tallest glyph.
        blocks: Ordered glyph set from shortest to tallest.

    Returns:
        A single Unicode character.

    Examples:
        >>> render_vbar(5, 10, color_zero=False)
        '▄'
    """
    if not max_value:
        return color_zero_glyph(blocks[0]) if color_zero else blocks[0]

    level = round((value / max_value) * (len(blocks) - 1)) + level_adjust
    level = max(0, level)
    level = min(level, len(blocks) - 1)
    glyph = blocks[level]
    if glyph == ZERO_GLYPH and color_zero:
        return color_zero_glyph(glyph)
    return glyph


def render_replica_sparkline(replica_sizes: list[int], max_replica_size: int) -> str:
    """
    Renders one vertical glyph per replica, color-coded by replica index.

    Args:
        replica_sizes: Per-replica sizes for one tablet or aggregate row.
        max_replica_size: Reference size for the tallest glyph.

    Returns:
        A compact ANSI-colored sparkline.

    Examples:
        >>> render_replica_sparkline([10, 20, 30], 30)  # doctest: +SKIP
        '...'
    """
    if not replica_sizes:
        return ""

    chars = []
    for idx, replica_size in enumerate(replica_sizes):
        glyph = render_vbar(replica_size, max_replica_size, color_zero=False)
        if glyph == ZERO_GLYPH:
            chars.append(color_zero_glyph(glyph))
        else:
            chars.append(color_replica_histogram(glyph, idx))
    return "".join(chars)


def render_sparkline(values: list[int], width: int = None) -> str:
    """
    Renders a compact sparkline for a sequence of values.

    While the value count is even and still above ``width``, adjacent values are
    merged in pairs to make the sparkline fit. If the count is still above
    ``width`` afterward, only the first ``width - 1`` values are rendered and
    the last character is ``>`` to indicate truncation.

    Args:
        values: Input values.
        width: Maximum number of glyphs to emit.

    Returns:
        A Unicode sparkline string.

    Examples:
        >>> render_sparkline([1, 2, 4, 8], width=4)
        '▁▂▄▇'
        >>> render_sparkline([1, 2, 3, 4, 5], width=4)
        '▁▃▄>'
    """
    if not values:
        return ""

    if width is None:
        width = len(values)

    if width <= 0:
        return ""

    while len(values) > width and len(values) % 2 == 0:
        values = [sum(values[i:i + 2]) / 2 for i in range(0, len(values), 2)]

    truncated = len(values) > width
    rendered_values = values[:width - 1] if truncated and width > 0 else values[:width]
    max_value = max(values)

    chars = []
    for value in rendered_values:
        chars.append(render_vbar(value, max_value))
    if truncated and width > 0:
        chars.append(">")
    return "".join(chars)

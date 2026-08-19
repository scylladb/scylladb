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
from dataclasses import dataclass, replace
from typing import Any
from tablets.stats import overcommit
from tablets.topology import Host
from tablets.topology import RackId

import tabulate as tabulate_module
from tabulate import SEPARATING_LINE
from tabulate import tabulate

TABLE_FORMAT = "rounded_outline"

# tabulate only splits embedded newlines for formats it lists as multiline, and no *_outline
# format is listed, so a two-line header would be emitted raw and break the row. The rendering
# path itself is format-agnostic, so registering the format is enough.
tabulate_module.multiline_formats.setdefault(TABLE_FORMAT, TABLE_FORMAT)

HISTOGRAM_WIDTH = 16
PARTIAL_BLOCKS = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"]

# Do not use whitespace as the zero-height glyph: tabulate trims leading
# whitespace in cells, which distorts the histogram.
# Keep the tallest glyph at ▇ to leave one line of headroom.
SPARKLINE_BLOCKS = "▁▁▂▃▄▅▆▇"
ZERO_GLYPH = SPARKLINE_BLOCKS[0]

ANSI_BLUE = "\033[34m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_RED = "\033[31m"
ANSI_DARK_GRAY = "\033[90m"
ANSI_RESET = "\033[0m"
ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

# The Box Drawing block, which the table frame is built from. It stops short of the Block
# Elements the bars and sparklines use, so cell content is never matched.
BOX_DRAWING_RE = re.compile(r"[─-╿]+")


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
    # Emit ANSI coloring. Off by default so that a caller which does not pass options,
    # such as a doctest, gets plain text.
    colors: bool = False


DEFAULT_PRESENTATION = PresentationOptions()


def format_host(host: Host, options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Renders a host the way a report identifies one: its ip, or its id when asked for or when
    the host has no ip.
    """
    if options.host_id or not host.ip:
        return str(host.id)
    return host.ip


def format_host_status(host: Host | None) -> str:
    """
    What a report has to say about a node beyond its load: D when the cluster cannot reach
    it, X when it is left out of load balancing, DX when both.

    Empty for a working node, for a row which is of no one node, such as a rack, and for a
    snapshot taken before system.load_per_node carried the columns, which leaves both
    unknown rather than false.

    Examples:
        >>> host = Host(id="e1a4", shard_count=1)
        >>> format_host_status(replace(host, up=False, excluded=True))
        'DX'
        >>> format_host_status(replace(host, up=True, excluded=False))
        ''
        >>> format_host_status(host), format_host_status(None)
        ('', '')
    """
    if host is None:
        return ""
    return ("D" if host.up is False else "") + ("X" if host.excluded else "")


def positive_int(value: str) -> int:
    """
    An argparse type for counts which have to be at least one.
    """
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def add_presentation_options(parser: argparse.ArgumentParser, *,
                             has_hosts: bool = False, has_tables: bool = False) -> None:
    parser.add_argument("--csv", action="store_true", help="Render tabular output as CSV")
    parser.add_argument("--hr", action=argparse.BooleanOptionalAction, default=True,
                        help="Format sizes with binary units (default: enabled)")
    # Defaults to None rather than to the detected value so that --colors can force coloring
    # on for a pipe, which "auto plus an override" could not express.
    parser.add_argument("--colors", action=argparse.BooleanOptionalAction, default=None,
                        help="Colorize output (default: only when stdout is a terminal)")
    if has_hosts:
        parser.add_argument("--host-id", action="store_true", help="Show host id instead of ip")
    if has_tables:
        parser.add_argument("--table-id", action="store_true", help="Print table id instead of keyspace.table in the first column")


def get_presentation_options_from_args(args: argparse.Namespace) -> PresentationOptions:
    """
    Reads the presentation flags a parser was given, defaulting the ones it left out.

    --colors/--no-colors overrides coloring; left out, it follows whether stdout is a terminal.
    """
    colors = getattr(args, "colors", None)
    return PresentationOptions(
        csv=getattr(args, "csv", DEFAULT_PRESENTATION.csv),
        human_readable=getattr(args, "hr", DEFAULT_PRESENTATION.human_readable),
        host_id=getattr(args, "host_id", DEFAULT_PRESENTATION.host_id),
        table_id=getattr(args, "table_id", DEFAULT_PRESENTATION.table_id),
        colors=colors if colors is not None else sys.stdout.isatty(),
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


def color_frame(text: str, options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Dims a rendered table's frame to dark gray, leaving cell content as it was.

    Colors whole runs of frame characters, so a row costs a handful of escapes rather
    than one per character.
    """
    if not options.colors:
        return text
    return BOX_DRAWING_RE.sub(lambda m: f"{ANSI_DARK_GRAY}{m.group()}{ANSI_RESET}", text)


def render_table(rows: list[Any], headers: list[Any], *, csv_output: bool = False,
                 colalign: list[str] | None = None,
                 options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    # Number parsing is always disabled: cells arrive pre-formatted, and letting
    # tabulate re-parse them drops the OVC sign and trailing zeros ("+1.60" -> "1.6").
    if not csv_output:
        # tabulate sizes its alignment list from the rows, so passing colalign for a
        # table a filter emptied would index past it. Headers alone still render.
        return color_frame(tabulate(rows, headers, tablefmt=TABLE_FORMAT,
                                    colalign=colalign if rows else None, disable_numparse=True), options)

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

    A cell with no value is None, which renders blank in a table and as an empty field in
    CSV. The format_*() helpers return "" for a value they were given as None, so a caller
    can hand them one rather than testing first.

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
            options=options,
        ))
    except BrokenPipeError:
        return False
    return True


def render_hbar(value: float, max_value: float, width: int = HISTOGRAM_WIDTH) -> str:
    """
    Renders a fixed-width horizontal bar.

    Args:
        value: Value to render.
        max_value: Reference value that fills the whole bar.
        width: Bar width in terminal cells.

    Returns:
        A left-aligned Unicode block bar padded to ``width``.

    Examples:
        >>> render_hbar(0.5, 1.0, width=4)
        '██  '
    """
    cells = (float(value) * width / max_value) if max_value else 0
    full = int(cells)
    partial = int((cells - full) * len(PARTIAL_BLOCKS))
    bar = "█" * full
    if partial > 0 and full < width:
        bar += PARTIAL_BLOCKS[partial]
    return bar.ljust(width)


def dim_fraction(text: str, options: PresentationOptions = DEFAULT_PRESENTATION, color: str = "") -> str:
    """
    Dims a formatted number down to what is worth reading: the decimal point and the digits
    after it go dark gray, while the significant digits and any unit keep ``color``, or the
    default color when a caller passes none.

    ``color`` is how a formatter which judges its value, such as format_util_pct(), keeps
    that judgement on the digits that carry it. A number with no fraction is just colored.

    Returns the text unchanged when color is off, so CSV and pipes stay clean.
    """
    if not options.colors:
        return text
    number, sep, unit = text.partition(" ")
    whole, point, fraction = number.partition(".")
    if color:
        whole = f"{color}{whole}{ANSI_RESET}"
    number = f"{whole}{ANSI_DARK_GRAY}{point}{fraction}{ANSI_RESET}" if point else whole
    return f"{number}{sep}{unit}"


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
        return dim_fraction(f"{size:.3f}", options)

    value = float(size)
    for unit in ["B", "Ki", "Mi", "Gi", "Ti"]:
        if value < 1024.0 or unit == "Ti":
            padded_unit = f"{unit:>2}"
            if unit == "B":
                return dim_fraction(f"{int(value)} {padded_unit}", options)
            return dim_fraction(f"{value:.3f} {padded_unit}", options)
        value /= 1024.0


def format_count(value: float, precision: int = 2,
                 options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Formats an averaged count, dimming the fraction as every other number does.

    Examples:
        >>> format_count(1.5)
        '1.50'
    """
    return dim_fraction(f"{value:.{precision}f}", options)


def format_pct(fraction: float | None, precision: int = 2,
               options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Formats a share of a total as a percentage, dimming the fraction the way a size is dimmed.

    Takes the share itself, in [0, 1], so a caller never scales one by hand.

    For the columns whose value is a share of a total, such as size [%] and tokens [%].
    The columns carrying a judgement, ovc [%] and util [%], have their own coloring.

    Examples:
        >>> format_pct(0.123456)
        '12.35'
        >>> format_pct(None)
        ''
    """
    if fraction is None:
        return ""
    return dim_fraction(f"{fraction * 100:.{precision}f}", options)


def format_rack_id(rack_id: RackId) -> str:
    return f"{rack_id[0]}/{rack_id[1]}"


def color_replica_histogram(bar: str, replica_idx: int | None,
                            options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Applies replica-specific ANSI coloring to a rendered bar fragment.

    Replica 0 keeps the default color, replica 1 is light blue, and higher
    replica indexes are light green. A bar standing for no single replica, such as one
    of a whole range, keeps the default color too.
    """
    if not options.colors or replica_idx is None:
        return bar
    if replica_idx == 0:
        return bar
    if replica_idx == 1:
        return f"{ANSI_BLUE}{bar}{ANSI_RESET}"
    return f"{ANSI_GREEN}{bar}{ANSI_RESET}"


def red(text: str, options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Marks text as something wrong, such as a node the cluster cannot reach.

    Returns the text unchanged when color is off, so CSV and pipes stay clean.
    """
    if not options.colors:
        return text
    return f"{ANSI_RED}{text}{ANSI_RESET}"


def color_zero_glyph(glyph: str, options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Highlights a zero-height sparkline glyph in dark gray.
    """
    if not options.colors:
        return glyph
    return f"{ANSI_DARK_GRAY}{glyph}{ANSI_RESET}"


def format_ovc_pct(value: float | None, options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
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
    if not options.colors:
        return text
    abs_pct = abs(pct)
    if abs_pct < 3:
        return text
    if abs_pct < 10:
        return f"{ANSI_YELLOW}{text}{ANSI_RESET}"
    return f"{ANSI_RED}{text}{ANSI_RESET}"


def format_ovc_ratio(value: float, average: float,
                     options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Formats a value's overcommit against an average, as format_ovc_pct() formats the ratio.

    An average of zero leaves nothing to deviate from, so the cell stays empty.

    Examples:
        >>> format_ovc_ratio(105, 100)
        '+5.00'
        >>> format_ovc_ratio(105, 0)
        ''
    """
    return format_ovc_pct(overcommit(value, average), options)


def format_util_pct(used: float, options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Formats utilization as a percentage, coloring the significant digits by level and
    dimming the fraction as every other number does.

    Takes the share of capacity in use, in [0, 1], as format_pct() does.

    Coloring is based on utilization level:
    - < 75%: default
    - < 85%: yellow/orange
    - >= 85%: red
    """
    color = ANSI_RED if used >= 0.85 else ANSI_YELLOW if used >= 0.75 else ""
    return dim_fraction(f"{used * 100:.2f}", options, color)


def format_tablets_per_shard(value: float | None, precision: int = 2,
                             options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
    """
    Formats tablets-per-shard with threshold-based coloring.

    Coloring is based on the absolute value:
    - < 200: default
    - < 500: yellow/orange
    - >= 500: red

    A shard row counts whole tablets, so it asks for no decimals; rack and node rows
    average over shards and keep two, dimmed as every other fraction is.
    """
    if value is None:
        return ""
    color = ANSI_RED if value >= 500 else ANSI_YELLOW if value >= 200 else ""
    return dim_fraction(f"{value:.{precision}f}", options, color)


def format_shard_location(host_part: str, shard_id: int) -> str:
    """
    Formats a host/shard location label.

    Examples:
        >>> format_shard_location('10.0.0.1', 3)
        '10.0.0.1:3'
    """
    return f"{host_part}:{shard_id}"


def render_vbar(value: int | float, max_value: int | float, blocks: str = SPARKLINE_BLOCKS,
                color_zero: bool = True, options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
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
        return color_zero_glyph(blocks[0], options) if color_zero else blocks[0]

    # Clamped, so a value above the reference tops the glyph out rather than indexing past it.
    level = min(max(round((value / max_value) * (len(blocks) - 1)), 0), len(blocks) - 1)
    glyph = blocks[level]
    if glyph == ZERO_GLYPH and color_zero:
        return color_zero_glyph(glyph, options)
    return glyph


def render_sparkline(values: list[int], width: int | None = None,
                     options: PresentationOptions = DEFAULT_PRESENTATION) -> str:
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

    # What is left over does not fit, so the last glyph gives way to a mark saying so.
    truncated = len(values) > width
    max_value = max(values)
    chars = [render_vbar(value, max_value, options=options)
             for value in (values[:width - 1] if truncated else values)]
    if truncated:
        chars.append(">")
    return "".join(chars)

#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""ScyllaDB Compression Advisor.

Calls the estimate_compression_ratios REST API and produces a
color-coded heatmap grid + summary table comparing the current
compression configuration against recommended LZ4 and ZSTD configs.

Usage:
    python3 tools/compression_advisor.py --host 127.0.0.1 --keyspace ks --table tbl
    python3 tools/compression_advisor.py --host 127.0.0.1 --keyspace ks --table tbl --no-color
"""

import argparse
import json
import re
import sys
import urllib.request
import urllib.error
import urllib.parse
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

IMPROVEMENT_THRESHOLD = 0.05  # 5% minimum to justify added complexity

# Compressor name normalization: Java fully-qualified names → short names
_COMPRESSOR_ALIASES = {
    "org.apache.cassandra.io.compress.LZ4Compressor": "LZ4Compressor",
    "org.apache.cassandra.io.compress.ZstdCompressor": "ZstdCompressor",
    "org.apache.cassandra.io.compress.SnappyCompressor": "SnappyCompressor",
    "org.apache.cassandra.io.compress.DeflateCompressor": "DeflateCompressor",
    "LZ4Compressor": "LZ4Compressor",
    "ZstdCompressor": "ZstdCompressor",
    "SnappyCompressor": "SnappyCompressor",
    "DeflateCompressor": "DeflateCompressor",
    "LZ4WithDictsCompressor": "LZ4WithDictsCompressor",
    "ZstdWithDictsCompressor": "ZstdWithDictsCompressor",
}

# Map non-dict compressor names to their dict-aware equivalents
_TO_DICT_AWARE = {
    "LZ4Compressor": "LZ4WithDictsCompressor",
    "ZstdCompressor": "ZstdWithDictsCompressor",
    "LZ4WithDictsCompressor": "LZ4WithDictsCompressor",
    "ZstdWithDictsCompressor": "ZstdWithDictsCompressor",
}

_DICT_CAPABLE = {
    "LZ4WithDictsCompressor",
    "ZstdWithDictsCompressor",
}

# Keys every estimation-result entry returned by the REST API must contain.
_RESULT_KEYS = frozenset(
    {"sstable_compression", "chunk_length_in_kb", "level", "dict", "ratio"}
)


# ---------------------------------------------------------------------------
# ANSI color helpers
# ---------------------------------------------------------------------------


class _Colors:
    """ANSI escape code container.  Codes are empty strings when disabled."""

    def __init__(self, enabled: bool):
        if enabled:
            self.RESET = "\033[0m"
            self.BOLD = "\033[1m"
            self.DIM = "\033[2m"
            self.GREEN = "\033[32m"
            self.RED = "\033[31m"
            self.YELLOW = "\033[1;33m"
            self.CYAN = "\033[1;36m"
            self.BOLD_GREEN = "\033[1;32m"
            self.BOLD_RED = "\033[1;31m"
        else:
            self.RESET = ""
            self.BOLD = ""
            self.DIM = ""
            self.GREEN = ""
            self.RED = ""
            self.YELLOW = ""
            self.CYAN = ""
            self.BOLD_GREEN = ""
            self.BOLD_RED = ""


# ---------------------------------------------------------------------------
# Pure logic: parsing
# ---------------------------------------------------------------------------


def parse_current_config(cql_compression_map: dict) -> dict:
    """Normalize the CQL ``system_schema.tables`` compression map.

    Input example (from CQL)::

        {'chunk_length_in_kb': '4',
         'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}

    Returns a dict::

        {'algorithm': 'LZ4WithDictsCompressor',
         'chunk_length_kb': 4,
         'level': 1,
         'dict_aware': False,
         'uncompressed': False}

    An empty/absent ``sstable_compression`` means the table is uncompressed.
    This is reported via ``uncompressed: True`` (with ``algorithm: ''``) so the
    advisor can treat the current ratio as 1.0 (raw size) and still recommend
    beneficial configurations instead of silently suppressing them.
    """
    raw_algo = cql_compression_map.get("sstable_compression", "")
    raw_algo_str = raw_algo if isinstance(raw_algo, str) else ""
    algo = _COMPRESSOR_ALIASES.get(raw_algo_str, raw_algo_str)

    uncompressed = algo == ""
    dict_aware = algo in _DICT_CAPABLE
    estimation_algo = _TO_DICT_AWARE.get(algo, algo)

    chunk_str = cql_compression_map.get("chunk_length_in_kb", "4")
    try:
        chunk_kb = int(chunk_str)
    except (ValueError, TypeError):
        chunk_kb = 4

    level_str = cql_compression_map.get("compression_level", "1")
    try:
        level = int(level_str)
    except (ValueError, TypeError):
        level = 1

    return {
        "algorithm": estimation_algo,
        "chunk_length_kb": chunk_kb,
        "level": level,
        "dict_aware": dict_aware,
        "uncompressed": uncompressed,
    }


def find_current_ratio(results: list, current_config: dict) -> float:
    """Find the compression ratio for the current config in *results*.

    For an uncompressed table, returns 1.0 (compressed size == raw size) so the
    advisor compares candidate configs against the raw on-disk footprint.

    For dict-aware compressors, looks for the ``past`` dict entry first
    (= the currently deployed dictionary) and falls back to ``none`` if
    no ``past`` match exists. For plain compressors, uses the ``none``
    bucket only.

    Returns 0.0 if the table is compressed but no matching entry is found.
    """
    if current_config.get("uncompressed"):
        return 1.0

    def _match(r, dict_mode):
        return (
            r["sstable_compression"] == current_config["algorithm"]
            and r["chunk_length_in_kb"] == current_config["chunk_length_kb"]
            and r["level"] == current_config["level"]
            and r["dict"] == dict_mode
        )

    current_uses_dict = current_config.get("dict_aware")
    if current_uses_dict is None:
        current_uses_dict = current_config.get("algorithm") in _DICT_CAPABLE

    if current_uses_dict:
        for r in results:
            if _match(r, "past"):
                return r["ratio"]
    for r in results:
        if _match(r, "none"):
            return r["ratio"]
    return 0.0


# ---------------------------------------------------------------------------
# Pure logic: heuristic selection
# ---------------------------------------------------------------------------


def _within_threshold(candidate_ratio: float, best_ratio: float) -> bool:
    """Return True if *candidate_ratio* is within IMPROVEMENT_THRESHOLD of *best_ratio*."""
    if best_ratio == 0:
        return True
    return candidate_ratio <= best_ratio * (1 + IMPROVEMENT_THRESHOLD)


def select_best_for_family(results: list, family_prefix: str) -> dict:
    """Select the best practical config for one algorithm family.

    *family_prefix* is ``"LZ4"`` or ``"Zstd"``.

    Applies the preference cascade (each override requires >5% improvement):

    1. Start from the absolute best ratio among ``future`` dict entries.
    2. Prefer 4 KB chunk size if within threshold of best chunk.
    3. Prefer ``none`` dict if within threshold of ``future`` dict.
    4. (ZSTD) Prefer the lowest level within threshold of the best level.

    Returns::

        {'config': {algorithm, chunk_length_kb, level, dict, ratio},
         'notes': [str, ...]}
    """
    algo_name = f"{family_prefix}WithDictsCompressor"

    notes: list[str] = []

    # --- Collect future-dict candidates for this family ---
    future = [
        r
        for r in results
        if r["sstable_compression"] == algo_name and r["dict"] == "future"
    ]
    none_entries = [
        r
        for r in results
        if r["sstable_compression"] == algo_name and r["dict"] == "none"
    ]

    if not future:
        # Fall back to none-dict if no future entries
        future = none_entries
    if not future:
        return {"config": None, "notes": ["No results for this algorithm family"]}

    # --- 1. Absolute best ratio ---
    best = min(future, key=lambda r: r["ratio"])
    best_ratio = best["ratio"]

    # --- 2. Chunk size preference: prefer 4K ---
    best_at_4k = [r for r in future if r["chunk_length_in_kb"] == 4]
    if best_at_4k:
        best_4k = min(best_at_4k, key=lambda r: r["ratio"])
        if _within_threshold(best_4k["ratio"], best_ratio):
            if best["chunk_length_in_kb"] != 4:
                pct = (
                    (best_4k["ratio"] - best_ratio) / best_ratio * 100
                    if best_ratio
                    else 0
                )
                notes.append(
                    f"4K chunk preferred ({best['chunk_length_in_kb']}K only {abs(pct):.1f}% better, below {IMPROVEMENT_THRESHOLD * 100:.0f}% threshold)"
                )
            chosen_chunk = 4
        else:
            chosen_chunk = best["chunk_length_in_kb"]
            pct = (
                (best_4k["ratio"] - best_ratio) / best_ratio * 100 if best_ratio else 0
            )
            notes.append(
                f"{chosen_chunk}K chunk chosen ({abs(pct):.1f}% better than 4K)"
            )
    else:
        chosen_chunk = best["chunk_length_in_kb"]

    # --- 3. Dict preference: prefer none ---
    future_at_chunk = [r for r in future if r["chunk_length_in_kb"] == chosen_chunk]
    none_at_chunk = [r for r in none_entries if r["chunk_length_in_kb"] == chosen_chunk]

    if future_at_chunk and none_at_chunk:
        best_future = min(future_at_chunk, key=lambda r: r["ratio"])
        best_none = min(none_at_chunk, key=lambda r: r["ratio"])
        if _within_threshold(best_none["ratio"], best_future["ratio"]):
            chosen_dict = "none"
            if best_future["ratio"] < best_none["ratio"]:
                pct = (
                    (best_none["ratio"] - best_future["ratio"])
                    / best_future["ratio"]
                    * 100
                    if best_future["ratio"]
                    else 0
                )
                notes.append(
                    f"No dictionary preferred (dict only {abs(pct):.1f}% better, below {IMPROVEMENT_THRESHOLD * 100:.0f}% threshold)"
                )
            else:
                notes.append("No dictionary preferred (no improvement from dictionary)")
        else:
            chosen_dict = "future"
            pct = (
                (best_none["ratio"] - best_future["ratio"]) / best_future["ratio"] * 100
                if best_future["ratio"]
                else 0
            )
            notes.append(
                f"Dictionary retrain recommended ({abs(pct):.1f}% improvement over no-dict)"
            )
    else:
        chosen_dict = "future" if future_at_chunk else "none"

    # --- 4. Level preference (ZSTD): prefer lowest adequate level ---
    if family_prefix == "Zstd":
        pool = future_at_chunk if chosen_dict == "future" else none_at_chunk
        if not pool:
            pool = future_at_chunk or none_at_chunk or future
        # Filter to chosen dict
        pool = [r for r in pool if r["dict"] == chosen_dict]
        if pool:
            best_level_r = min(pool, key=lambda r: r["ratio"])
            best_level_ratio = best_level_r["ratio"]
            # Walk from level 1 upward; pick lowest within threshold
            levels_sorted = sorted(pool, key=lambda r: r["level"])
            chosen_level = best_level_r["level"]
            for r in levels_sorted:
                if _within_threshold(r["ratio"], best_level_ratio):
                    chosen_level = r["level"]
                    break
            if chosen_level < best_level_r["level"]:
                pct = (
                    (
                        dict([(r["level"], r["ratio"]) for r in levels_sorted])[
                            chosen_level
                        ]
                        - best_level_ratio
                    )
                    / best_level_ratio
                    * 100
                    if best_level_ratio
                    else 0
                )
                notes.append(
                    f"Level {chosen_level} chosen (levels {chosen_level + 1}-{best_level_r['level']} add only {abs(pct):.1f}% improvement)"
                )
            elif chosen_level > 1:
                lvl1 = [r for r in levels_sorted if r["level"] == 1]
                if lvl1:
                    pct = (
                        (lvl1[0]["ratio"] - best_level_ratio) / best_level_ratio * 100
                        if best_level_ratio
                        else 0
                    )
                    notes.append(
                        f"Level {chosen_level} chosen ({abs(pct):.1f}% better than level 1)"
                    )
        else:
            chosen_level = 1
    else:
        chosen_level = 1

    # --- Find the exact matching result ---
    chosen_dict_entries = future_at_chunk if chosen_dict == "future" else none_at_chunk
    if not chosen_dict_entries:
        chosen_dict_entries = future
    match = [
        r
        for r in chosen_dict_entries
        if r["level"] == chosen_level and r["dict"] == chosen_dict
    ]
    if not match:
        # Broader fallback
        match = [
            r
            for r in results
            if r["sstable_compression"] == algo_name
            and r["chunk_length_in_kb"] == chosen_chunk
            and r["level"] == chosen_level
            and r["dict"] == chosen_dict
        ]
    if not match:
        # Last resort: pick the absolute best
        match = [best]

    chosen = match[0]
    return {
        "config": {
            "algorithm": chosen["sstable_compression"],
            "chunk_length_kb": chosen["chunk_length_in_kb"],
            "level": chosen["level"],
            "dict": chosen["dict"],
            "ratio": chosen["ratio"],
        },
        "notes": notes,
    }


def generate_summary(results: list, current_config: Optional[dict]) -> dict:
    """Produce the full summary: current config + two recommendations.

    Returns::

        {'current':   {'algorithm', 'chunk_length_kb', 'level', 'dict', 'ratio'},
         'best_lz4':  {'config': {..., 'ratio'}, 'improvement_pct': float, 'notes': [str]},
         'best_zstd': {'config': {..., 'ratio'}, 'improvement_pct': float, 'notes': [str]}}

    When *current_config* is ``None`` (e.g. the ScyllaDB driver is not
    installed) the current configuration is reported as unknown, improvement
    percentages cannot be computed (``improvement_pct`` is ``None``), and the
    recommendations fall back to the best absolute ratio per family.

    When the table is uncompressed the current ratio is 1.0 (raw size) so
    improvements are measured against the uncompressed footprint.
    """
    if current_config is None:
        current_ratio: Optional[float] = None
        current = {
            "algorithm": None,
            "chunk_length_kb": None,
            "level": None,
            "dict": None,
            "ratio": None,
        }
    elif current_config.get("uncompressed"):
        # Uncompressed table: ratio 1.0 (raw), no algorithm/level/dictionary.
        current_ratio = find_current_ratio(results, current_config)
        current = {
            "algorithm": "uncompressed",
            "chunk_length_kb": None,
            "level": None,
            "dict": None,
            "ratio": current_ratio,
        }
    else:
        current_ratio = find_current_ratio(results, current_config)
        current = {
            "algorithm": current_config["algorithm"],
            "chunk_length_kb": current_config["chunk_length_kb"],
            "level": current_config["level"],
            "dict": "current",
            "ratio": current_ratio,
        }

    def _make_recommendation(family_prefix):
        # ``improvement_pct`` is tri-state:
        #   float  -> percentage improvement vs the known current config
        #   None   -> current config is unknown, so it cannot be computed
        #   0.0    -> no candidate config exists for this family
        sel = select_best_for_family(results, family_prefix)
        if sel["config"] is None:
            return {"config": None, "improvement_pct": 0.0, "notes": sel["notes"]}
        cfg = sel["config"]
        if current_ratio is None:
            improvement = None
        elif current_ratio > 0:
            improvement = (current_ratio - cfg["ratio"]) / current_ratio * 100
        else:
            improvement = 0.0
        return {
            "config": cfg,
            "improvement_pct": improvement,
            "notes": sel["notes"],
        }

    return {
        "current": current,
        "best_lz4": _make_recommendation("LZ4"),
        "best_zstd": _make_recommendation("Zstd"),
    }


def _is_recommended(rec: dict) -> bool:
    """Whether a recommendation should be highlighted.

    When the current config is known, only highlight if it improves over the
    current ratio by more than the threshold.  When the current config is
    unknown (``improvement_pct is None``), always highlight the best absolute
    config for the family.
    """
    if not rec["config"]:
        return False
    imp = rec["improvement_pct"]
    if imp is None:
        return True
    return imp > IMPROVEMENT_THRESHOLD * 100


def _recommendation_sort_key(rec: dict) -> float:
    """Sort key for picking the single best recommendation.

    Uses the improvement percentage when the current config is known, and
    otherwise (unknown current) ranks by absolute compression quality so the
    lowest-ratio config wins.
    """
    imp = rec["improvement_pct"]
    if imp is not None:
        return imp
    return (1.0 - rec["config"]["ratio"]) * 100


# ---------------------------------------------------------------------------
# Formatting: heatmap grid
# ---------------------------------------------------------------------------


def _ratio_cell(
    ratio: float,
    current_ratio: float,
    is_current: bool,
    is_recommended: bool,
    C: _Colors,
) -> str:
    """Format a single ratio value with optional coloring and markers."""
    val_str = f"{ratio:5.3f}"

    marker = ""
    if is_current:
        marker = f"{C.YELLOW}*{C.RESET}"
    elif is_recommended:
        marker = f"{C.CYAN}\u25c4{C.RESET}"
    else:
        marker = " "

    if current_ratio > 0:
        rel = (ratio - current_ratio) / current_ratio
        if is_current:
            colored = f"{C.YELLOW}{val_str}{C.RESET}"
        elif rel < -IMPROVEMENT_THRESHOLD:
            colored = f"{C.GREEN}{val_str}{C.RESET}"
        elif rel > IMPROVEMENT_THRESHOLD:
            colored = f"{C.RED}{val_str}{C.RESET}"
        else:
            colored = f"{C.DIM}{val_str}{C.RESET}"
    else:
        colored = val_str

    return f"{colored}{marker}"


def format_heatmap_grid(
    results: list, current_config: Optional[dict], summary: dict, use_color: bool = True
) -> str:
    """Render the full heatmap grid as a string.

    Layout::

                               no dict              future dict
                         1K      4K      16K     1K      4K      16K
        LZ4    lvl1    0.82    0.78*   0.67    0.45    0.41<   0.38
        ZSTD   lvl1    ...
        ...
    """
    C = _Colors(use_color)
    lines: list[str] = []

    chunk_sizes = [1, 4, 16]
    dict_modes = ["none", "future"]

    # Build a lookup: (algo, chunk, level, dict) -> ratio
    lookup: dict[tuple, float] = {}
    for r in results:
        key = (r["sstable_compression"], r["chunk_length_in_kb"], r["level"], r["dict"])
        lookup[key] = r["ratio"]

    # Determine which entries are recommended (only if they improve over current)
    rec_lz4_key = None
    rec_zstd_key = None
    if _is_recommended(summary["best_lz4"]):
        c = summary["best_lz4"]["config"]
        rec_lz4_key = (c["algorithm"], c["chunk_length_kb"], c["level"], c["dict"])
    if _is_recommended(summary["best_zstd"]):
        c = summary["best_zstd"]["config"]
        rec_zstd_key = (c["algorithm"], c["chunk_length_kb"], c["level"], c["dict"])

    # A real ratio is always > 0, so ``or 0.0`` only ever substitutes for an
    # unknown (None) current config, which disables current-vs-others coloring.
    current_ratio = summary["current"]["ratio"] or 0.0

    # Header
    col_w = 8
    label_w = 15
    lines.append("")
    hdr1 = " " * label_w
    for dm in dict_modes:
        label = "no dict" if dm == "none" else "future dict"
        span = col_w * len(chunk_sizes)
        hdr1 += label.center(span)
    lines.append(hdr1)

    hdr2 = " " * label_w
    for _ in dict_modes:
        for ck in chunk_sizes:
            hdr2 += f"{ck}K".center(col_w)
    lines.append(f"{C.BOLD}{hdr2}{C.RESET}")
    lines.append("")

    # Rows: LZ4 level 1, then ZSTD levels 1-5
    rows = [("LZ4WithDictsCompressor", "LZ4", 1)]
    for lvl in range(1, 6):
        rows.append(("ZstdWithDictsCompressor", "ZSTD", lvl))

    for algo_full, algo_short, level in rows:
        row_label = f"{algo_short:5s}  level {level}".ljust(label_w)
        row = row_label
        for dm in dict_modes:
            for ck in chunk_sizes:
                key = (algo_full, ck, level, dm)
                ratio = lookup.get(key, -1.0)
                if ratio < 0:
                    row += "   —   ".center(col_w)
                    continue
                # Check current: match on algo/chunk/level, dict=past maps to none column
                is_cur = (
                    current_config is not None
                    and algo_full == current_config["algorithm"]
                    and ck == current_config["chunk_length_kb"]
                    and level == current_config["level"]
                    and dm == "none"
                )
                # Dict-capable current: show the active (``past``) ratio so the
                # starred cell matches the summary.
                display_ratio = ratio
                if (
                    is_cur
                    and current_config.get("dict_aware")
                    and summary["current"]["ratio"] is not None
                ):
                    display_ratio = summary["current"]["ratio"]
                is_rec = key == rec_lz4_key or key == rec_zstd_key
                cell = _ratio_cell(display_ratio, current_ratio, is_cur, is_rec, C)
                # Pad cell accounting for ANSI codes (they have zero display width)
                visible_len = len(f"{display_ratio:5.3f}") + 1  # value + marker
                padding = col_w - visible_len
                row += cell + " " * max(padding, 1)
        lines.append(row)

    lines.append("")
    lines.append(
        f"  {C.YELLOW}*{C.RESET} = current (shown in no-dict column; dictionary-aware "
        f"compressors show the active dictionary ratio)    "
        f"{C.CYAN}\u25c4{C.RESET} = recommended"
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Formatting: summary table
# ---------------------------------------------------------------------------


def format_summary_table(summary: dict, use_color: bool = True) -> str:
    """Render the recommendation summary as a 3-column table."""
    C = _Colors(use_color)
    lines: list[str] = []

    cur = summary["current"]
    lz4 = summary["best_lz4"]
    zstd = summary["best_zstd"]

    col_w = 18
    hdr = f"{'':20s}{C.BOLD}{'Current':>{col_w}s}{'Best LZ4':>{col_w}s}{'Best ZSTD':>{col_w}s}{C.RESET}"
    lines.append(hdr)
    lines.append("\u2500" * (20 + 3 * col_w))

    def _algo_short(name):
        if not name:
            return "—"
        if "LZ4" in name:
            return "LZ4WithDicts"
        if "Zstd" in name or "ZSTD" in name:
            return "ZstdWithDicts"
        return name

    def _dict_label(d):
        if not d:
            return "—"
        if d == "current":
            return "current"
        if d == "future":
            return "retrained"
        return d

    def _row(label, cur_val, lz4_val, zstd_val):
        return f"{label:20s}{str(cur_val):>{col_w}s}{str(lz4_val):>{col_w}s}{str(zstd_val):>{col_w}s}"

    lz4_cfg = lz4["config"] or {}
    zstd_cfg = zstd["config"] or {}

    lines.append(
        _row(
            "Algorithm",
            _algo_short(cur.get("algorithm", "—")),
            _algo_short(lz4_cfg.get("algorithm", "—")),
            _algo_short(zstd_cfg.get("algorithm", "—")),
        )
    )
    lines.append(
        _row(
            "Chunk Size (KB)",
            cur.get("chunk_length_kb") or "—",
            lz4_cfg.get("chunk_length_kb", "—"),
            zstd_cfg.get("chunk_length_kb", "—"),
        )
    )
    lines.append(
        _row(
            "Level",
            cur.get("level") or "—",
            lz4_cfg.get("level", "—"),
            zstd_cfg.get("level", "—"),
        )
    )
    lines.append(
        _row(
            "Dictionary",
            _dict_label(cur.get("dict", "—")),
            _dict_label(lz4_cfg.get("dict", "—")),
            _dict_label(zstd_cfg.get("dict", "—")),
        )
    )

    # Ratio row with color
    cur_ratio_s = f"{cur['ratio']:.3f}" if cur["ratio"] is not None else "—"
    lz4_ratio_s = f"{lz4_cfg['ratio']:.3f}" if lz4_cfg.get("ratio") is not None else "—"
    zstd_ratio_s = (
        f"{zstd_cfg['ratio']:.3f}" if zstd_cfg.get("ratio") is not None else "—"
    )
    lines.append(_row("Ratio", cur_ratio_s, lz4_ratio_s, zstd_ratio_s))

    # Improvement row — signs use "size change" convention:
    # positive improvement (better compression) → negative display (smaller size),
    # negative improvement (worse compression) → positive display (larger size).
    def _improvement_str(rec, C):
        if not rec["config"]:
            return "—"
        pct = rec["improvement_pct"]
        if pct is None:
            return f"{C.DIM}n/a{C.RESET}"
        if pct > IMPROVEMENT_THRESHOLD * 100:
            return f"{C.BOLD_GREEN}-{pct:.1f}%{C.RESET}"
        elif pct < -IMPROVEMENT_THRESHOLD * 100:
            return f"{C.BOLD_RED}+{abs(pct):.1f}%{C.RESET}"
        else:
            return f"{C.DIM}{-pct:+.1f}%{C.RESET}"

    imp_cur = "—"
    imp_lz4 = _improvement_str(lz4, C)
    imp_zstd = _improvement_str(zstd, C)
    # For improvement row, we need to handle ANSI codes in alignment
    # Use raw string building since colors mess up alignment
    # Pad colored strings manually
    lines.append(
        f"{'vs Current':20s}{imp_cur:>{col_w}s}{imp_lz4:>{col_w + len(imp_lz4) - _visible_len(imp_lz4)}s}{imp_zstd:>{col_w + len(imp_zstd) - _visible_len(imp_zstd)}s}"
    )

    # Notes
    all_notes = []
    if lz4.get("notes"):
        for n in lz4["notes"]:
            all_notes.append(f"  LZ4: {n}")
    if zstd.get("notes"):
        for n in zstd["notes"]:
            all_notes.append(f"  ZSTD: {n}")
    if all_notes:
        lines.append("")
        lines.append(f"{C.BOLD}Notes:{C.RESET}")
        lines.extend(all_notes)

    return "\n".join(lines)


def _visible_len(s: str) -> int:
    """Return the visible length of a string, ignoring ANSI escape codes."""
    return len(re.sub(r"\033\[[0-9;]*m", "", s))


# ---------------------------------------------------------------------------
# Full formatted output
# ---------------------------------------------------------------------------


def format_alter_table(
    summary: dict, keyspace: str, table: str, use_color: bool = True
) -> str:
    """Generate the ALTER TABLE CQL command for the best recommendation.

    Picks the recommendation with the largest improvement above the
    threshold, otherwise emits a no-change message.
    """
    C = _Colors(use_color)

    # Pick the best recommendation
    best = None
    zstd = summary["best_zstd"]
    lz4 = summary["best_lz4"]

    candidates = []
    if _is_recommended(zstd):
        candidates.append((_recommendation_sort_key(zstd), "ZSTD", zstd))
    if _is_recommended(lz4):
        candidates.append((_recommendation_sort_key(lz4), "LZ4", lz4))
    if candidates:
        _, _, best = max(candidates, key=lambda item: item[0])

    if not best:
        return f"{C.DIM}No change recommended — current config is already optimal.{C.RESET}"

    cfg = best["config"]
    algo = cfg["algorithm"]
    chunk = cfg["chunk_length_kb"]
    level = cfg["level"]
    needs_dict = cfg["dict"] == "future"

    # Build compression map entries
    opts = [f"'sstable_compression': '{algo}'", f"'chunk_length_in_kb': '{chunk}'"]
    if "Zstd" in algo:
        opts.append(f"'compression_level': '{level}'")

    compression_map = "{" + ", ".join(opts) + "}"

    lines = []
    lines.append(f"{C.BOLD}Apply with:{C.RESET}")
    lines.append("")
    lines.append(
        f'  {C.CYAN}ALTER TABLE "{keyspace}"."{table}" '
        f"WITH compression = {compression_map};{C.RESET}"
    )
    if needs_dict:
        retrain_params = urllib.parse.urlencode({"keyspace": keyspace, "cf": table})
        lines.append("")
        lines.append(
            "  -- Then retrain the dictionary (requires SSTABLE_COMPRESSION_DICTS feature):"
        )
        lines.append(f"  -- POST /storage_service/retrain_dict?{retrain_params}")

    return "\n".join(lines)


def format_output(
    results: list,
    current_config: Optional[dict],
    keyspace: str,
    table: str,
    use_color: bool = True,
) -> str:
    """Produce the complete formatted output string."""
    C = _Colors(use_color)
    summary = generate_summary(results, current_config)

    parts = []
    parts.append(
        f"{C.BOLD}Compression Estimation Results for {keyspace}.{table}{C.RESET}"
    )
    parts.append("(ratio = compressed/raw, lower is better)")
    parts.append(format_heatmap_grid(results, current_config, summary, use_color))
    parts.append("")
    parts.append(f"{C.BOLD}Recommendation Summary{C.RESET}")
    parts.append(format_summary_table(summary, use_color))
    parts.append("")
    parts.append(format_alter_table(summary, keyspace, table, use_color))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# I/O: CQL and REST
# ---------------------------------------------------------------------------


def get_current_config_via_cql(
    host: str, keyspace: str, table: str, port: int = 9042
) -> Optional[dict]:
    """Query ``system_schema.tables`` for the current compression config.

    Returns the raw CQL compression map, e.g.::

        {'chunk_length_in_kb': '4',
         'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}

    Returns ``None`` (and prints a warning) if the ScyllaDB Python driver is
    not installed.  In that case the caller can still produce the estimation
    report, just without highlighting the current configuration.
    """
    try:
        from cassandra.cluster import Cluster
    except ImportError:
        print(
            "Warning: ScyllaDB Python driver not found — continuing without the "
            "current compression settings.\n"
            "         Install it with: pip install scylla-driver",
            file=sys.stderr,
        )
        return None

    cluster = Cluster([host], port=port)
    try:
        session = cluster.connect()
        rows = session.execute(
            "SELECT compression FROM system_schema.tables "
            "WHERE keyspace_name = %s AND table_name = %s",
            (keyspace, table),
        )
        row = rows.one()
        if row is None:
            print(f"Error: table {keyspace}.{table} not found", file=sys.stderr)
            sys.exit(1)
        # A table may have an empty/absent compression map (uncompressed);
        # normalize to {} so the caller falls back to default settings rather
        # than crashing on dict(None).
        return dict(row.compression or {})
    except Exception as e:
        print(
            f"Error reading current config from CQL at {host}:{port}: {e}",
            file=sys.stderr,
        )
        sys.exit(1)
    finally:
        cluster.shutdown()


def call_estimate_api(host: str, keyspace: str, table: str, port: int = 10000) -> list:
    """Call ``GET /storage_service/estimate_compression_ratios``."""
    params = urllib.parse.urlencode({"keyspace": keyspace, "cf": table})
    url = f"http://{host}:{port}/storage_service/estimate_compression_ratios?{params}"
    try:
        with urllib.request.urlopen(url, timeout=300) as resp:
            raw = resp.read()
    except OSError as e:
        # OSError is the common base of urllib.error.URLError (and HTTPError),
        # socket.timeout/TimeoutError raised mid-read, and connection resets.
        print(f"Error calling estimation API: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        data: Any = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"Error: estimation API returned invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print(
            "Error: estimation API returned unexpected JSON structure",
            file=sys.stderr,
        )
        sys.exit(1)

    for index, entry in enumerate(data):
        if not isinstance(entry, dict) or not _RESULT_KEYS <= entry.keys():
            print(
                f"Error: estimation API returned a malformed entry at index "
                f"{index}: {entry!r}",
                file=sys.stderr,
            )
            sys.exit(1)
    return data


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="ScyllaDB Compression Advisor — analyze compression "
        "configurations and recommend improvements.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s --host 10.0.0.1 --keyspace myks --table mytbl\n"
            "  %(prog)s --host 10.0.0.1 --keyspace myks --table mytbl --no-color\n"
        ),
    )
    parser.add_argument("--host", required=True, help="ScyllaDB node IP address")
    parser.add_argument("--keyspace", required=True, help="Keyspace name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument(
        "--api-port", type=int, default=10000, help="REST API port (default: 10000)"
    )
    parser.add_argument(
        "--cql-port",
        type=int,
        default=9042,
        help="CQL native transport port (default: 9042)",
    )
    parser.add_argument(
        "--no-color", action="store_true", help="Disable ANSI color output"
    )
    args = parser.parse_args()

    use_color = not args.no_color and sys.stdout.isatty()

    cql_map = get_current_config_via_cql(
        args.host, args.keyspace, args.table, args.cql_port
    )
    current_config = parse_current_config(cql_map) if cql_map is not None else None
    results = call_estimate_api(args.host, args.keyspace, args.table, args.api_port)

    output = format_output(
        results, current_config, args.keyspace, args.table, use_color
    )
    print(output)


if __name__ == "__main__":
    main()

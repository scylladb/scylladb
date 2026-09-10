#!/usr/bin/env python3
"""
Reconstruct compaction start/end log entries from system.compaction_history
and merge them into an existing Scylla log file.

Usage:
    python3 inject_compaction_log.py --log scylla.log --history compaction_history.txt -o merged.log

The history input is the text output of `nodetool compactionhistory`.
The script produces a merged log containing original log lines plus
synthetic compaction start and finish entries, sorted by timestamp.

The synthetic entries mimic the format produced by compaction::setup()
(report_start_desc) and compaction::finish() (report_finish_desc) when
the compaction logger is at debug level:

  DEBUG ... compaction - [<type> <ks>.<cf> <uuid>] <Verb> [<sstables>]
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional


# Scylla log timestamp format: "INFO  2024-04-29 12:00:00,123 [shard ..."
# or sometimes: "2024-04-29T12:00:00.123+0000 ..."
LOG_TS_PATTERNS = [
    # Standard Scylla: "INFO  2024-04-29 12:00:00,123"
    re.compile(r'^[A-Z]+\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})'),
    # ISO-ish: "2024-04-29T12:00:00.123"
    re.compile(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[.,]\d{3})'),
]


@dataclass
class HistoryEntry:
    id: str
    shard_id: int
    ks: str
    cf: str
    compaction_type: str
    started_at: datetime
    compacted_at: datetime
    bytes_in: int
    bytes_out: int
    rows_merged: str
    sstables_in: str
    sstables_out: str


# Map compaction_type to the verbs used in log messages
COMPACTION_TYPE_START_VERB = {
    "Compaction": "Compacting",
    "Major":      "Compacting",
    "Cleanup":    "Cleaning",
    "Scrub":      "Scrubbing",
    "Reshape":    "Reshaping",
    "Reshard":    "Resharding",
    "Upgrade":    "Upgrading",
    "Split":      "Splitting",
}

COMPACTION_TYPE_FINISH_VERB = {
    "Compaction": "Compacted",
    "Major":      "Compacted",
    "Cleanup":    "Cleaned",
    "Scrub":      "Finished scrubbing",
    "Reshape":    "Reshaped",
    "Reshard":    "Resharded",
    "Upgrade":    "Upgraded",
    "Split":      "Split",
}


def pretty_size(nbytes: int) -> str:
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            if unit == "bytes":
                return f"{nbytes} {unit}"
            return f"{nbytes:.2f} {unit}"
        nbytes /= 1024.0
    return f"{nbytes:.2f} PB"


def pretty_throughput(nbytes: int, duration_ms: int) -> str:
    if duration_ms <= 0:
        return "N/A"
    bps = nbytes / (duration_ms / 1000.0)
    return f"{pretty_size(int(bps))}/s"


def parse_log_timestamp(line: str) -> Optional[datetime]:
    for pat in LOG_TS_PATTERNS:
        m = pat.match(line)
        if m:
            ts_str = m.group(1)
            ts_str = ts_str.replace(",", ".")
            ts_str = ts_str.replace("T", " ")
            try:
                return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                pass
    return None


def format_ts_for_log(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:23]


def parse_history_ts(ts_str: str) -> datetime:
    ts_str = ts_str.strip()
    # Handle variable fractional second precision
    try:
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")


def format_sstable_list(sstables: list) -> str:
    """Format a list of sstable dicts into the log representation."""
    if not sstables:
        return "N/A"
    parts = []
    for sst in sstables:
        if isinstance(sst, dict):
            gen = sst.get("generation", "?")
            origin = sst.get("origin", "?")
            size = sst.get("size", 0)
            parts.append(f"gen={gen}:origin={origin}:size={size}")
        else:
            parts.append(str(sst))
    return ",".join(parts)


def parse_compaction_history_json(history_file: str) -> list:
    """Parse JSON output of `nodetool compactionhistory -F json`."""
    with open(history_file, "r") as f:
        data = json.load(f)

    history_list = data.get("CompactionHistory", data) if isinstance(data, dict) else data

    entries = []
    for e in history_list:
        started_at_ms = e.get("started_at")
        compacted_at_ms = e.get("compacted_at")

        # JSON timestamps are either epoch millis (int) or ISO strings
        if isinstance(started_at_ms, int):
            started_at = datetime.fromtimestamp(started_at_ms / 1000.0)
        else:
            started_at = parse_history_ts(str(started_at_ms))

        if isinstance(compacted_at_ms, int):
            compacted_at = datetime.fromtimestamp(compacted_at_ms / 1000.0)
        else:
            compacted_at = parse_history_ts(str(compacted_at_ms))

        sstables_in = e.get("sstables_in", [])
        sstables_out = e.get("sstables_out", [])

        entries.append(HistoryEntry(
            id=e.get("id", ""),
            shard_id=int(e.get("shard_id", 0)),
            ks=e.get("keyspace_name", e.get("ks", "")),
            cf=e.get("columnfamily_name", e.get("cf", "")),
            compaction_type=e.get("compaction_type", ""),
            started_at=started_at,
            compacted_at=compacted_at,
            bytes_in=int(e.get("bytes_in", 0)),
            bytes_out=int(e.get("bytes_out", 0)),
            rows_merged=str(e.get("rows_merged", {})),
            sstables_in=format_sstable_list(sstables_in),
            sstables_out=format_sstable_list(sstables_out),
        ))
    return entries


def parse_compaction_history_text(history_file: str) -> list:
    """Parse the text output of `nodetool compactionhistory`."""
    entries = []

    with open(history_file, "r") as f:
        lines = f.readlines()

    # Find header line to get column positions
    header_idx = None
    for i, line in enumerate(lines):
        if "id" in line and "keyspace_name" in line and "compaction_type" in line:
            header_idx = i
            break

    if header_idx is None:
        print("Error: could not find header row in compaction history text output", file=sys.stderr)
        sys.exit(1)

    header = lines[header_idx]

    # Extract column names and their start positions
    col_names = header.split()
    col_starts = []
    pos = 0
    for name in col_names:
        idx = header.index(name, pos)
        col_starts.append(idx)
        pos = idx + len(name)

    def extract_columns(line: str) -> dict:
        parts = {}
        for i, name in enumerate(col_names):
            start = col_starts[i]
            end = col_starts[i + 1] if i + 1 < len(col_names) else len(line)
            parts[name] = line[start:end].strip()
        return parts

    for line in lines[header_idx + 1:]:
        stripped = line.strip()
        if not stripped:
            continue

        cols = extract_columns(line)

        try:
            entry = HistoryEntry(
                id=cols.get("id", ""),
                shard_id=int(cols.get("shard_id", "0")),
                ks=cols.get("keyspace_name", ""),
                cf=cols.get("columnfamily_name", ""),
                compaction_type=cols.get("compaction_type", ""),
                started_at=parse_history_ts(cols.get("started_at", "")),
                compacted_at=parse_history_ts(cols.get("compacted_at", "")),
                bytes_in=int(cols.get("bytes_in", "0")),
                bytes_out=int(cols.get("bytes_out", "0")),
                rows_merged=cols.get("rows_merged", "{}"),
                sstables_in=cols.get("sstables_in", "N/A"),
                sstables_out=cols.get("sstables_out", "N/A"),
            )
            entries.append(entry)
        except (ValueError, KeyError) as e:
            print(f"Warning: skipping malformed history line: {stripped!r} ({e})", file=sys.stderr)

    return entries


def parse_compaction_history(history_file: str) -> list:
    """Auto-detect format (JSON or text) and parse accordingly."""
    with open(history_file, "r") as f:
        first_chars = f.read(64).lstrip()

    if first_chars.startswith("{") or first_chars.startswith("["):
        print("Detected JSON format for compaction history input.", file=sys.stderr)
        return parse_compaction_history_json(history_file)
    else:
        print("Detected text format for compaction history input.", file=sys.stderr)
        print("Hint: for best results, use `nodetool compactionhistory -F json` to generate the input.",
              file=sys.stderr)
        return parse_compaction_history_text(history_file)


def make_start_line(entry: HistoryEntry) -> str:
    """Generate a synthetic log line for compaction start."""
    verb = COMPACTION_TYPE_START_VERB.get(entry.compaction_type, "Compacting")
    ts = format_ts_for_log(entry.started_at)

    return (
        f"DEBUG {ts} [shard {entry.shard_id}] compaction - "
        f"[{entry.compaction_type} {entry.ks}.{entry.cf} {entry.id}] "
        f"{verb} [{entry.sstables_in}]"
    )


def make_finish_line(entry: HistoryEntry) -> str:
    """Generate a synthetic log line for compaction finish."""
    verb = COMPACTION_TYPE_FINISH_VERB.get(entry.compaction_type, "Compacted")
    ts = format_ts_for_log(entry.compacted_at)

    duration_ms = int((entry.compacted_at - entry.started_at).total_seconds() * 1000)
    ratio = (entry.bytes_out / entry.bytes_in * 100) if entry.bytes_in > 0 else 0
    throughput = pretty_throughput(entry.bytes_in, duration_ms)

    # Count input sstables from the sstables_in field
    num_sstables_in = entry.sstables_in.count("gen=") if entry.sstables_in != "N/A" else 0

    return (
        f"DEBUG {ts} [shard {entry.shard_id}] compaction - "
        f"[{entry.compaction_type} {entry.ks}.{entry.cf} {entry.id}] "
        f"{verb} {num_sstables_in} sstables to [{entry.sstables_out}]. "
        f"{pretty_size(entry.bytes_in)} to {pretty_size(entry.bytes_out)} "
        f"(~{int(ratio)}% of original) in {duration_ms}ms = {throughput}."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Merge compaction history entries into a Scylla log as synthetic compaction start/end messages."
    )
    parser.add_argument("--log", required=True, help="Path to the original Scylla log file")
    parser.add_argument("--history", required=True,
                        help="Path to the nodetool compactionhistory text output")
    parser.add_argument("-o", "--output", default="-",
                        help="Output file (default: stdout)")
    args = parser.parse_args()

    entries = parse_compaction_history(args.history)
    if not entries:
        print("Warning: no compaction history entries found", file=sys.stderr)

    # Build list of (timestamp, line) for synthetic entries
    synthetic = []
    for e in entries:
        synthetic.append((e.started_at, make_start_line(e)))
        synthetic.append((e.compacted_at, make_finish_line(e)))

    synthetic.sort(key=lambda x: x[0])

    # Read original log
    with open(args.log, "r") as f:
        log_lines = f.readlines()

    # Merge: walk through log lines and synthetic entries together
    out_lines = []
    syn_idx = 0

    for line in log_lines:
        log_ts = parse_log_timestamp(line)

        # Insert any synthetic entries that come before this log line
        while syn_idx < len(synthetic) and log_ts is not None and synthetic[syn_idx][0] <= log_ts:
            out_lines.append(synthetic[syn_idx][1] + "\n")
            syn_idx += 1

        out_lines.append(line if line.endswith("\n") else line + "\n")

    # Append any remaining synthetic entries (after the last log line)
    while syn_idx < len(synthetic):
        out_lines.append(synthetic[syn_idx][1] + "\n")
        syn_idx += 1

    if args.output == "-":
        sys.stdout.writelines(out_lines)
    else:
        with open(args.output, "w") as f:
            f.writelines(out_lines)

    print(f"Injected {len(synthetic)} synthetic compaction log entries "
          f"({len(entries)} start + {len(entries)} finish) from {len(entries)} history records.",
          file=sys.stderr)


if __name__ == "__main__":
    main()

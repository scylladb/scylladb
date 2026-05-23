#!/usr/bin/env python3
"""Generate HTML report from W-TinyLFU real benchmark results.

Reads stats.txt files and Grafana PNG snapshots from <results_dir>/<label>/
and produces a self-contained HTML report with embedded images.

Usage: python3 tools/gen_bench_report.py [path/to/bench_config.yaml]
"""

import base64
import os
import sys
import glob

import yaml

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

config_path = sys.argv[1] if len(sys.argv) > 1 else "tools/bench_config.yaml"
with open(config_path) as f:
    cfg = yaml.safe_load(f)

RESULTS_DIR = cfg.get("results_dir", ".bench/results")
REPORT_PATH = cfg.get("report", "docs/dev/wtinylfu_real_benchmark_results.html")
SNAP_PANELS = cfg.get("snapshots", {}).get("panels", [])

configs = cfg.get("configs", [])
labels = [c["label"] for c in configs]

scylla_cfg = cfg.get("scylla", {})
workloads_cfg = cfg.get("workloads", {})
phases_cfg = cfg.get("phases", {})

# ---------------------------------------------------------------------------
# Load stats per config
# ---------------------------------------------------------------------------

data = {}
for label in labels:
    stats_path = os.path.join(RESULTS_DIR, label, "stats.txt")
    if not os.path.exists(stats_path):
        print(f"WARNING: {stats_path} not found, skipping {label}")
        continue
    d = {}
    with open(stats_path) as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                k, v = line.split("=", 1)
                try:
                    d[k] = float(v) if "." in v else int(v)
                except ValueError:
                    d[k] = v
    data[label] = d

if not data:
    print("ERROR: No stats files found. Run the benchmark first.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------

def embed_png(path):
    """Read a PNG file and return a base64 data URI, or None."""
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        raw = f.read()
    # Sanity: check PNG magic
    if raw[:4] != b"\x89PNG":
        return None
    return "data:image/png;base64," + base64.b64encode(raw).decode()


def snapshot_section(label, phase, panels):
    """Return HTML for all snapshot images of a given label + phase."""
    snap_dir = os.path.join(RESULTS_DIR, label, "snapshots")
    imgs = []
    for p in panels:
        name = p["name"]
        title = p.get("title", name)
        path = os.path.join(snap_dir, f"{name}_{phase}.png")
        uri = embed_png(path)
        if uri:
            imgs.append((title, uri))
    return imgs

# ---------------------------------------------------------------------------
# Build HTML
# ---------------------------------------------------------------------------

# Gather snapshot availability
has_snapshots = False
for label in data:
    snap_dir = os.path.join(RESULTS_DIR, label, "snapshots")
    if os.path.isdir(snap_dir) and glob.glob(os.path.join(snap_dir, "*.png")):
        has_snapshots = True
        break

html_parts = []

def w(s):
    html_parts.append(s)

# --- Header ---
w("""<!DOCTYPE html>
<html>
<head>
<title>W-TinyLFU Real Benchmark Results</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       margin: 20px; background: #f5f5f5; color: #333; }
.container { max-width: 1400px; margin: 0 auto; }
h1 { color: #333; border-bottom: 2px solid #1565c0; padding-bottom: 8px; }
h2 { color: #555; margin-top: 40px; }
h3 { color: #444; margin-top: 20px; }
.card { background: white; border-radius: 8px; padding: 20px; margin: 15px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
.kpi-row { display: flex; gap: 15px; flex-wrap: wrap; }
.kpi { flex: 1; min-width: 180px; text-align: center; padding: 15px; border-radius: 8px; }
.kpi .value { font-size: 1.8em; font-weight: bold; }
.kpi .label { color: #666; font-size: 0.85em; margin-top: 4px; }
.kpi-green { background: #e8f5e9; color: #2e7d32; }
.kpi-yellow { background: #fff3e0; color: #ef6c00; }
.kpi-red { background: #fbe9e7; color: #c62828; }
table { width: 100%; border-collapse: collapse; margin: 10px 0; }
th, td { padding: 10px 15px; text-align: right; border-bottom: 1px solid #eee; }
th { background: #f8f8f8; font-weight: 600; }
td:first-child, th:first-child { text-align: left; }
.highlight { font-weight: bold; color: #1565c0; }
.worse { color: #c62828; }

/* Grafana snapshot grid */
.snap-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(460px, 1fr));
             gap: 16px; margin: 12px 0; }
.snap-card { background: #fafafa; border: 1px solid #e0e0e0; border-radius: 6px;
             overflow: hidden; }
.snap-card img { width: 100%; display: block; }
.snap-card .snap-title { padding: 6px 12px; font-size: 0.85em; color: #555;
                          background: #f0f0f0; border-top: 1px solid #e0e0e0; }

/* Config badge */
.config-badge { display: inline-block; background: #1565c0; color: white;
                border-radius: 4px; padding: 2px 10px; font-size: 0.85em;
                font-weight: 600; margin-right: 8px; }
.config-badge.lru { background: #c62828; }
.config-badge.mid { background: #ef6c00; }
</style>
</head>
<body>
<div class="container">
""")

# --- Title ---
w(f"""<h1>W-TinyLFU Real Benchmark Results</h1>
<p>ScyllaDB {scylla_cfg.get('memory', '?')} memory, {scylla_cfg.get('smp', '?')} shard(s)</p>
<p>Hot: {workloads_cfg.get('hot', {}).get('row_count', '?')} rows
   ({workloads_cfg.get('hot', {}).get('access_pattern', '?')}),
   Scan: {workloads_cfg.get('scan', {}).get('row_count', '?')} rows
   ({workloads_cfg.get('scan', {}).get('access_pattern', '?')}),
   {workloads_cfg.get('hot', {}).get('payload_bytes', '?')}B payload</p>
<p>Warmup: {phases_cfg.get('warmup', {}).get('duration', '?')} hot-only @ {phases_cfg.get('warmup', {}).get('rate', '?')}/s,
   Attack: {phases_cfg.get('attack', {}).get('duration', '?')} hot+scan @ {phases_cfg.get('attack', {}).get('rate', '?')}/s each</p>
""")

# --- KPI cards ---
if len(data) >= 2:
    sorted_labels = sorted(data.keys(), key=lambda l: data[l].get("hot_p99", 999))
    best = sorted_labels[0]
    worst = sorted_labels[-1]
    best_d = data[best]
    worst_d = data[worst]

    w('<h2>Key Metrics</h2>\n<div class="kpi-row">\n')
    kpi_colors = {best: "kpi-green", worst: "kpi-red"}
    for label in sorted_labels:
        d = data[label]
        color = kpi_colors.get(label, "kpi-yellow")
        p99 = d.get("hot_p99", "?")
        mx = d.get("hot_pMax", "?")
        suffix = ""
        if label == worst and best_d.get("hot_p99"):
            try:
                pct = (float(worst_d["hot_p99"]) / float(best_d["hot_p99"]) - 1) * 100
                suffix = f" (+{pct:.0f}%)"
            except (ValueError, ZeroDivisionError):
                pass
        w(f'  <div class="kpi {color}">\n')
        w(f'    <div class="value">{label}: {p99}ms</div>\n')
        w(f'    <div class="label">Hot p99 Latency{suffix}</div>\n')
        w(f'  </div>\n')
    w('</div>\n')

    w('<div class="kpi-row" style="margin-top:10px">\n')
    for label in sorted_labels:
        d = data[label]
        color = kpi_colors.get(label, "kpi-yellow")
        mx = d.get("hot_pMax", "?")
        suffix = ""
        if label == worst and best_d.get("hot_pMax"):
            try:
                ratio = float(worst_d["hot_pMax"]) / float(best_d["hot_pMax"])
                suffix = f" ({ratio:.1f}x)"
            except (ValueError, ZeroDivisionError):
                pass
        w(f'  <div class="kpi {color}">\n')
        w(f'    <div class="value">{label}: {mx}ms</div>\n')
        w(f'    <div class="label">Hot Max Latency{suffix}</div>\n')
        w(f'  </div>\n')
    w('</div>\n')

# --- Plotly charts ---
w("""
<h2>Hit Rate Comparison</h2>
<div class="card"><div id="hr-chart"></div></div>

<h2>Hot Read Latency Percentiles</h2>
<div class="card"><div id="latency-chart"></div></div>
""")

# --- Detailed results table ---
w("""<h2>Detailed Results</h2>
<div class="card">
<table>
<tr><th>Config</th><th>Warmup HR</th><th>Attack HR</th>
    <th>Hot HR</th><th>Scan HR</th>
    <th>p50 (ms)</th><th>p90 (ms)</th><th>p99 (ms)</th><th>p99.9 (ms)</th><th>Max (ms)</th>
    <th>Cache Parts</th></tr>
""")

for label in labels:
    if label not in data:
        continue
    d = data[label]
    cfg_entry = next((c for c in configs if c["label"] == label), {})
    window = cfg_entry.get("window_percent", "?")
    w(f"""<tr>
    <td><b>{label} ({window}% window)</b></td>
    <td>{d.get('warmup_hr', '?')}%</td>
    <td>{d.get('attack_hr', '?')}%</td>
    <td class="highlight">{d.get('attack_hot_hr', '?')}%</td>
    <td>{d.get('attack_scan_hr', '?')}%</td>
    <td>{d.get('hot_p50', '?')}</td>
    <td>{d.get('hot_p90', '?')}</td>
    <td>{d.get('hot_p99', '?')}</td>
    <td>{d.get('hot_p99_9', '?')}</td>
    <td>{d.get('hot_pMax', '?')}</td>
    <td>{d.get('cache_partitions', '?'):,}</td>
</tr>
""")
w("</table>\n</div>\n")

# --- Grafana snapshots ---
if has_snapshots:
    w('<h2>Grafana Snapshots</h2>\n')
    w('<p>Captured automatically from the monitoring stack during each benchmark phase.</p>\n')

    for label in labels:
        if label not in data:
            continue
        cfg_entry = next((c for c in configs if c["label"] == label), {})
        window = cfg_entry.get("window_percent", "?")
        badge_cls = "lru" if window == 99 else ("mid" if window == 50 else "")

        for phase in ["warmup", "attack"]:
            imgs = snapshot_section(label, phase, SNAP_PANELS)
            if not imgs:
                continue

            phase_label = "Warmup (hot only)" if phase == "warmup" else "Attack (hot + scan)"
            w(f'<h3><span class="config-badge {badge_cls}">{label}</span> '
              f'{phase_label} &mdash; window={window}%</h3>\n')
            w('<div class="snap-grid">\n')
            for title, uri in imgs:
                w(f'  <div class="snap-card">\n')
                w(f'    <img src="{uri}" alt="{title}" loading="lazy">\n')
                w(f'    <div class="snap-title">{title}</div>\n')
                w(f'  </div>\n')
            w('</div>\n')
else:
    w("""<h2>Grafana Snapshots</h2>
<div class="card">
<p>No Grafana snapshots found. To capture snapshots, ensure the monitoring stack is
running (<code>docker compose -f .bench/monitoring/docker-compose.yml up -d</code>)
before running the benchmark. The <code>snapshots</code> section in
<code>tools/bench_config.yaml</code> controls which panels are captured.</p>
</div>
""")

# --- Analysis ---
w("""<h2>Analysis</h2>
<div class="card">
<h3>Hit Rate</h3>
<p>Per-workload cache hit rates reveal the true picture. With per-table metrics (separate keyspaces
for hot and scan workloads), we can see:</p>
<ul>
<li><b>Hot HR</b>: How well the cache protects frequently-accessed data under scan pressure</li>
<li><b>Scan HR</b>: Expected to be near-zero since the scan set (500k rows) far exceeds cache capacity</li>
<li><b>Combined HR</b>: The aggregate masks the per-workload difference</li>
</ul>

<h3>Tail Latency</h3>
<p>The most significant difference appears in <b>tail latency</b>:</p>
<ul>
<li><b>Large window (LRU-like)</b>: scan entries flood the window, causing occasional eviction
of hot entries that must then be re-read from disk &mdash; visible as p99+ spikes</li>
<li><b>Small window (frequency-dominant)</b>: the Count-Min Sketch frequency filter blocks most
scan entries from entering the SLRU cache, protecting hot data</li>
</ul>

<h3>Why differences are smaller than simulation</h3>
<ul>
<li><b>Uniform access pattern</b>: <code>hash(i) % N</code> distributes reads uniformly, unlike
Zipfian where a few keys are much hotter. Uniform means all keys have similar frequency, reducing
the advantage of frequency-based admission.</li>
<li><b>Cache overhead</b>: Real entries carry metadata; partition-level caching differs from
fixed-size slot simulation.</li>
<li><b>Background activity</b>: Compaction, memtable flushes, and other ScyllaDB internals
affect cache behaviour.</li>
</ul>

<h3>Conclusion</h3>
<p>Even with uniform access patterns and real-world overhead, <b>smaller W-TinyLFU window sizes
provide measurably better scan resistance</b>, particularly in tail latency. The default 1%
window is a good choice for most workloads.</p>
</div>
""")

# --- Plotly JS ---
hr_labels = [f"{l} ({next((c for c in configs if c['label']==l), {}).get('window_percent','?')}%)"
             for l in labels if l in data]

warmup_hrs = [data[l].get("warmup_hr", 0) for l in labels if l in data]
attack_hrs = [data[l].get("attack_hr", 0) for l in labels if l in data]
attack_hot_hrs = [data[l].get("attack_hot_hr", 0) for l in labels if l in data]
attack_scan_hrs = [data[l].get("attack_scan_hr", 0) for l in labels if l in data]

pct_names = ["p50", "p90", "p99", "p99.9", "Max"]
pct_keys = ["hot_p50", "hot_p90", "hot_p99", "hot_p99_9", "hot_pMax"]
colors = ["#2196F3", "#FF9800", "#F44336", "#9C27B0", "#4CAF50", "#795548"]

w("<script>\n")

# Hit rate chart
w(f"""Plotly.newPlot('hr-chart', [
  {{ x: {hr_labels}, y: {warmup_hrs}, name: 'Warmup HR', type: 'bar', marker: {{ color: '#4CAF50' }} }},
  {{ x: {hr_labels}, y: {attack_hot_hrs}, name: 'Attack Hot HR', type: 'bar', marker: {{ color: '#2196F3' }} }},
  {{ x: {hr_labels}, y: {attack_scan_hrs}, name: 'Attack Scan HR', type: 'bar', marker: {{ color: '#FF9800' }} }},
  {{ x: {hr_labels}, y: {attack_hrs}, name: 'Attack Combined HR', type: 'bar', marker: {{ color: '#F44336', opacity: 0.5 }} }}
], {{
  title: 'Cache Hit Rate by Window Size (Per-Workload)',
  yaxis: {{ title: 'Hit Rate (%)', range: [0, 100] }},
  barmode: 'group'
}});
""")

# Latency chart
w("Plotly.newPlot('latency-chart', [\n")
for i, label in enumerate([l for l in labels if l in data]):
    d = data[label]
    vals = [d.get(k, 0) for k in pct_keys]
    cfg_entry = next((c for c in configs if c["label"] == label), {})
    window = cfg_entry.get("window_percent", "?")
    color = colors[i % len(colors)]
    w(f"  {{ x: {pct_names}, y: {vals}, name: '{label} ({window}%)', type: 'bar', "
      f"marker: {{ color: '{color}' }} }},\n")
w("""], {
  title: 'Hot Read Latency During Scan Attack',
  yaxis: { title: 'Latency (ms)', type: 'log' },
  barmode: 'group'
});
""")

w("</script>\n")
w("</div>\n</body>\n</html>\n")

# ---------------------------------------------------------------------------
# Write report
# ---------------------------------------------------------------------------

os.makedirs(os.path.dirname(REPORT_PATH) or ".", exist_ok=True)
with open(REPORT_PATH, "w") as f:
    f.write("".join(html_parts))

snap_count = sum(1 for _ in glob.glob(os.path.join(RESULTS_DIR, "*/snapshots/*.png")))
print(f"Report saved to {REPORT_PATH}")
print(f"  {len(data)} configs, {snap_count} Grafana snapshots embedded")

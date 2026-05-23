#!/usr/bin/env python3
"""
W-TinyLFU Window Size Comparison

Experiment 1: Hot + Scan workload at window sizes 1%, 50%, 99%
Experiment 2: Queue workload (2x cache) comparing 1% vs 99% window

Output: self-contained HTML with interactive Plotly.js charts.
"""

import random
import collections
import time
import json
import os

import numpy as np

# ---------------------------------------------------------------------------
# Count-Min Sketch (4-bit counters, 4 rows)
# ---------------------------------------------------------------------------

class CountMinSketch:
    DEPTH = 4
    MAX_COUNT = 15

    def __init__(self, width=1 << 16):
        self.width = width
        self._table = np.zeros((self.DEPTH, width), dtype=np.uint8)
        self._sample_count = 0
        self._reset_threshold = width * 10
        self._seeds = [0x9E3779B97F4A7C15, 0x6A09E667F3BCC908,
                       0xBB67AE8584CAA73B, 0x3C6EF372FE94F82B]

    def _hash(self, key, row):
        x = key ^ self._seeds[row]
        x = ((x >> 30) ^ x) * 0xBF58476D1CE4E5B9 & 0xFFFFFFFFFFFFFFFF
        x = ((x >> 27) ^ x) * 0x94D049BB133111EB & 0xFFFFFFFFFFFFFFFF
        x = (x >> 31) ^ x
        return x & (self.width - 1)

    def increment(self, key):
        for row in range(self.DEPTH):
            col = self._hash(key, row)
            if self._table[row, col] < self.MAX_COUNT:
                self._table[row, col] += 1
        self._sample_count += 1
        if self._sample_count >= self._reset_threshold:
            self._table >>= 1
            self._sample_count = 0

    def estimate(self, key):
        return min(int(self._table[row, self._hash(key, row)])
                   for row in range(self.DEPTH))


# ---------------------------------------------------------------------------
# LRU Cache
# ---------------------------------------------------------------------------

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self._cache = collections.OrderedDict()

    def access(self, key):
        if key in self._cache:
            self._cache.move_to_end(key)
            return True
        if len(self._cache) >= self.capacity:
            self._cache.popitem(last=False)
        self._cache[key] = True
        return False

    def __len__(self):
        return len(self._cache)


# ---------------------------------------------------------------------------
# W-TinyLFU Cache (parameterized window)
# ---------------------------------------------------------------------------

class WTinyLFUCache:
    def __init__(self, capacity, window_pct=0.01, protected_pct=0.80):
        self.capacity = capacity
        self.window_pct = window_pct
        self._window_cap = max(1, int(capacity * window_pct))
        self._protected_cap = max(1, int(capacity * protected_pct))

        self._window = collections.OrderedDict()
        self._probation = collections.OrderedDict()
        self._protected = collections.OrderedDict()
        self._key_to_seg = {}
        self._sketch = CountMinSketch(width=1 << 16)
        self._sketch._reset_threshold = max(1000, capacity * 10)
        self._jitter_state = 0x12345678

    def _jitter(self):
        s = self._jitter_state & 0xFFFFFFFF
        s ^= (s << 13) & 0xFFFFFFFF
        s ^= (s >> 17)
        s ^= (s << 5) & 0xFFFFFFFF
        self._jitter_state = s
        return s

    def _total_size(self):
        return len(self._window) + len(self._probation) + len(self._protected)

    def access(self, key):
        self._sketch.increment(key)
        seg = self._key_to_seg.get(key)
        if seg is None:
            self._on_miss(key)
            return False
        if seg == "window":
            self._window.move_to_end(key)
        elif seg == "probation":
            del self._probation[key]
            self._protected[key] = True
            self._key_to_seg[key] = "protected"
            self._balance_protected()
        else:
            self._protected.move_to_end(key)
        return True

    def _on_miss(self, key):
        self._window[key] = True
        self._key_to_seg[key] = "window"
        while len(self._window) > self._window_cap:
            w_evictee, _ = self._window.popitem(last=False)
            del self._key_to_seg[w_evictee]
            self._try_admit(w_evictee)
        while self._total_size() > self.capacity:
            if self._probation:
                evicted, _ = self._probation.popitem(last=False)
                del self._key_to_seg[evicted]
            elif self._window:
                evicted, _ = self._window.popitem(last=False)
                del self._key_to_seg[evicted]
            else:
                break

    def _try_admit(self, candidate):
        if self._total_size() < self.capacity:
            self._probation[candidate] = True
            self._key_to_seg[candidate] = "probation"
            return
        if not self._probation:
            self._probation[candidate] = True
            self._key_to_seg[candidate] = "probation"
            return
        p_victim_key = next(iter(self._probation))
        c_freq = self._sketch.estimate(candidate)
        p_freq = self._sketch.estimate(p_victim_key)
        if c_freq > p_freq:
            admit = True
        elif c_freq >= 6:
            admit = (self._jitter() & 127) == 0
        else:
            admit = False
        if admit:
            self._probation.popitem(last=False)
            del self._key_to_seg[p_victim_key]
            self._probation[candidate] = True
            self._key_to_seg[candidate] = "probation"

    def _balance_protected(self):
        while len(self._protected) > self._protected_cap:
            demoted, _ = self._protected.popitem(last=False)
            self._probation[demoted] = True
            self._key_to_seg[demoted] = "probation"

    def __len__(self):
        return self._total_size()


# ---------------------------------------------------------------------------
# Zipfian key generator
# ---------------------------------------------------------------------------

class ZipfianGenerator:
    def __init__(self, n, s=1.0, rng=None):
        self.n = n
        self._rng = rng or random.Random(42)
        weights = np.array([1.0 / (k ** s) for k in range(1, n + 1)])
        self._cdf = np.cumsum(weights / weights.sum())

    def next(self):
        return int(np.searchsorted(self._cdf, self._rng.random()))


# ---------------------------------------------------------------------------
# Experiment 1: Hot + Scan simulation
# ---------------------------------------------------------------------------

def run_hot_scan(cache, hot_gen, n_hot, n_scan, warmup_ops, attack_seconds,
                 sample_interval):
    rng_lat = np.random.RandomState(12345)
    disk_ms, cache_ms = 5.5, 0.4

    ts = {"time": [], "hot_hits": [], "hot_total": [],
          "scan_hits": [], "scan_total": [],
          "hot_lats": [], "scan_lats": []}

    # Warmup
    for _ in range(warmup_ops):
        cache.access(hot_gen.next())

    scan_cursor = 0
    hot_rate = scan_rate = 5000

    for sec in range(attack_seconds):
        hot_keys = [hot_gen.next() for _ in range(hot_rate)]
        scan_keys = []
        for _ in range(scan_rate):
            scan_keys.append(n_hot + (scan_cursor % n_scan))
            scan_cursor += 1

        hh = ht = sh = st = 0
        h_lats = []
        s_lats = []

        batch = 50
        hi = si = 0
        while hi < hot_rate or si < scan_rate:
            end_h = min(hi + batch, hot_rate)
            for i in range(hi, end_h):
                hit = cache.access(hot_keys[i])
                lat = max(0.1, rng_lat.normal(cache_ms if hit else disk_ms,
                                               0.08 if hit else 0.6))
                hh += int(hit); ht += 1; h_lats.append(lat)
            hi = end_h
            end_s = min(si + batch, scan_rate)
            for i in range(si, end_s):
                hit = cache.access(scan_keys[i])
                lat = max(0.1, rng_lat.normal(cache_ms if hit else disk_ms,
                                               0.08 if hit else 0.6))
                sh += int(hit); st += 1; s_lats.append(lat)
            si = end_s

        if (sec + 1) % sample_interval == 0:
            ts["time"].append(sec + 1)
            ts["hot_hits"].append(hh)
            ts["hot_total"].append(ht * sample_interval)
            ts["scan_hits"].append(sh)
            ts["scan_total"].append(st * sample_interval)
            ts["hot_lats"].append(h_lats)
            ts["scan_lats"].append(s_lats)
            # reset for this interval
        # Note: we accumulate per-second but only sample every interval
        # Fix: accumulate properly per interval
    return ts


def run_hot_scan_proper(cache, hot_gen, n_hot, n_scan, warmup_ops,
                        attack_seconds, sample_interval):
    """Hot + Scan with proper interval accumulation."""
    rng_lat = np.random.RandomState(12345)
    disk_ms, cache_ms = 5.5, 0.4

    ts = {"time": [], "hot_hits": [], "hot_total": [],
          "scan_hits": [], "scan_total": [],
          "hot_lats": [], "scan_lats": []}

    for _ in range(warmup_ops):
        cache.access(hot_gen.next())

    scan_cursor = 0
    hot_rate = scan_rate = 5000

    ihh = iht = ish = ist = 0
    ih_lats = []
    is_lats = []

    for sec in range(attack_seconds):
        hot_keys = [hot_gen.next() for _ in range(hot_rate)]
        scan_keys = []
        for _ in range(scan_rate):
            scan_keys.append(n_hot + (scan_cursor % n_scan))
            scan_cursor += 1

        batch = 50
        hi = si = 0
        while hi < hot_rate or si < scan_rate:
            end_h = min(hi + batch, hot_rate)
            for i in range(hi, end_h):
                hit = cache.access(hot_keys[i])
                lat = max(0.1, rng_lat.normal(cache_ms if hit else disk_ms,
                                               0.08 if hit else 0.6))
                ihh += int(hit); iht += 1; ih_lats.append(lat)
            hi = end_h
            end_s = min(si + batch, scan_rate)
            for i in range(si, end_s):
                hit = cache.access(scan_keys[i])
                lat = max(0.1, rng_lat.normal(cache_ms if hit else disk_ms,
                                               0.08 if hit else 0.6))
                ish += int(hit); ist += 1; is_lats.append(lat)
            si = end_s

        if (sec + 1) % sample_interval == 0:
            ts["time"].append(sec + 1)
            ts["hot_hits"].append(ihh)
            ts["hot_total"].append(iht)
            ts["scan_hits"].append(ish)
            ts["scan_total"].append(ist)
            ts["hot_lats"].append(ih_lats)
            ts["scan_lats"].append(is_lats)
            ihh = iht = ish = ist = 0
            ih_lats = []
            is_lats = []

    return ts


# ---------------------------------------------------------------------------
# Experiment 2: Queue workload
# ---------------------------------------------------------------------------

def run_queue(cache, queue_size, total_ops, sample_interval_ops):
    """
    Queue (looping FIFO) workload: items 0..queue_size-1 are accessed
    sequentially, one access per item, wrapping around. queue_size = 2×cache
    means the working set exceeds cache capacity.

    On each loop through the queue, an item is re-seen after queue_size other
    accesses.  With cache < queue_size, every re-access is a miss under pure
    LRU (classic LRU thrashing on a cyclic scan larger than the cache).

    Under W-TinyLFU with 1% window, the frequency gate further blocks new
    arrivals (they have freq ≤ 1 and can't beat incumbents whose counters
    haven't fully decayed).  With 99% window (≈LRU), items at least get
    admitted to the large window for their brief lifetime.

    Neither policy achieves great hit rates here, but the comparison shows
    how the admission filter can hurt on sequential/queue patterns where
    there is no frequency signal to exploit.
    """
    rng_lat = np.random.RandomState(54321)
    disk_ms, cache_ms = 5.5, 0.4

    ts = {"time": [], "hits": [], "total": [], "lats": []}

    ihits = itotal = 0
    ilats = []
    ops_done = 0
    sample_num = 0
    cursor = 0

    while ops_done < total_ops:
        key = cursor % queue_size
        hit = cache.access(key)
        lat = max(0.1, rng_lat.normal(cache_ms if hit else disk_ms,
                                       0.08 if hit else 0.6))
        ihits += int(hit)
        itotal += 1
        ilats.append(lat)
        ops_done += 1
        cursor += 1

        if itotal >= sample_interval_ops:
            sample_num += 1
            ts["time"].append(sample_num)
            ts["hits"].append(ihits)
            ts["total"].append(itotal)
            ts["lats"].append(ilats)
            ihits = itotal = 0
            ilats = []

    if itotal > 0:
        sample_num += 1
        ts["time"].append(sample_num)
        ts["hits"].append(ihits)
        ts["total"].append(itotal)
        ts["lats"].append(ilats)

    return ts


# ---------------------------------------------------------------------------
# Percentile helper
# ---------------------------------------------------------------------------

def pctiles(lat_lists, ps):
    result = {p: [] for p in ps}
    for lats in lat_lists:
        if not lats:
            for p in ps:
                result[p].append(0)
            continue
        arr = np.array(lats)
        for p in ps:
            result[p].append(float(np.percentile(arr, p)))
    return result


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def generate_html(exp1_data, exp2_data):
    """Generate comparison HTML with all experiments."""

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>W-TinyLFU Window Size Comparison</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  :root {
    --bg: #0a0e17; --surface: #111827; --surface2: #1a2332;
    --border: #1e3a5f; --text: #e2e8f0; --muted: #8899aa;
    --accent: #00b4d8; --red: #ef476f; --green: #06d6a0;
    --yellow: #ffd166; --purple: #b388ff;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: var(--bg); color: var(--text);
    font-family: 'Inter', -apple-system, sans-serif; }

  .header { text-align: center; padding: 40px 20px 10px; }
  .header h1 { font-size: 2.2rem; color: var(--accent); font-weight: 700; }
  .header .sub { color: var(--muted); font-size: 1rem; margin-top: 6px; }

  .section { max-width: 1600px; margin: 0 auto; padding: 10px 40px; }
  .section h2 { color: var(--accent); font-size: 1.5rem; margin: 30px 0 10px;
    border-bottom: 2px solid var(--border); padding-bottom: 8px; }
  .section h3 { color: var(--yellow); font-size: 1.1rem; margin: 16px 0 8px; }
  .section p { color: var(--muted); font-size: 0.95rem; line-height: 1.6; margin-bottom: 12px; }

  .kpi-row { display: flex; justify-content: center; gap: 16px;
    padding: 16px 0; flex-wrap: wrap; }
  .kpi { background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px 24px; min-width: 180px; text-align: center; }
  .kpi .label { font-size: 0.7rem; text-transform: uppercase;
    letter-spacing: 0.08em; color: var(--muted); margin-bottom: 6px; }
  .kpi .val { font-size: 1.6rem; font-weight: 700; }
  .kpi .delta { font-size: 0.8rem; font-weight: 600; margin-top: 4px; }
  .good { color: var(--green); } .bad { color: var(--red); } .neutral { color: var(--accent); }
  .c-w1 { color: var(--green); }   /* 1% window */
  .c-w50 { color: var(--yellow); } /* 50% window */
  .c-w99 { color: var(--red); }    /* 99% window */
  .c-lru { color: var(--purple); } /* pure LRU */

  .chart-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin: 12px 0; }
  .chart-card { background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 14px; min-height: 370px; }
  .chart-card.full { grid-column: 1 / -1; }

  table { width: 100%; border-collapse: collapse; background: var(--surface);
    border-radius: 10px; overflow: hidden; margin: 12px 0; }
  th, td { padding: 10px 16px; text-align: right; }
  th { background: var(--surface2); color: var(--muted); font-size: 0.7rem;
    text-transform: uppercase; letter-spacing: 0.08em; font-weight: 600; }
  th:first-child, td:first-child { text-align: left; }
  td { border-top: 1px solid var(--border); font-variant-numeric: tabular-nums; }

  .insight { max-width: 1000px; margin: 16px auto; background: linear-gradient(135deg, #0d2137, #112240);
    border: 1px solid var(--border); border-left: 4px solid var(--green);
    border-radius: 8px; padding: 16px 24px; font-size: 0.9rem; line-height: 1.6; }
  .insight strong { color: var(--green); }
  .insight.warn { border-left-color: var(--yellow); }
  .insight.warn strong { color: var(--yellow); }

  .params { text-align: center; padding: 30px 40px; color: var(--muted); font-size: 0.75rem; }
</style>
</head>
<body>

<div class="header">
  <h1>W-TinyLFU Window Size Comparison</h1>
  <div class="sub">How window percentage affects cache behavior under different workloads</div>
  <div class="sub" style="margin-top:4px">March 27, 2026 — Branch: copilot/use-w-tinylfu-for-cache</div>
</div>
"""

    html += '<div class="section">\n'

    # =========================================================================
    # EXPERIMENT 1: Hot + Scan
    # =========================================================================
    html += '<h2>Experiment 1: Hot + Scan Attack (Window = 1%, 50%, 99%)</h2>\n'
    html += """<p>The classic W-TinyLFU benchmark: 80k hot keys (Zipfian s=1.0) + 500k sequential scan keys.
    Cache holds 80k entries. We compare three window sizes to show how the frequency gate
    protects hot data from scan pollution.</p>\n"""

    # KPI cards for experiment 1
    e1 = exp1_data
    html += '<div class="kpi-row">\n'
    for cfg in e1["configs"]:
        name = cfg["name"]
        color_cls = cfg["color_cls"]
        html += f'''<div class="kpi">
  <div class="label">{name} — Hot Hit Rate</div>
  <div class="val {color_cls}">{cfg["hot_hr_total"]:.1f}%</div>
</div>
<div class="kpi">
  <div class="label">{name} — Hot p90</div>
  <div class="val {color_cls}">{cfg["hot_p90"]:.2f}ms</div>
</div>\n'''
    html += '</div>\n'

    # Insight
    html += f'''<div class="insight">
<strong>Key finding:</strong> Window=1% achieves {e1["configs"][0]["hot_hr_total"]:.1f}% hot hit rate,
protecting the working set from scan pollution. Window=99% (≈pure LRU) drops to
{e1["configs"][2]["hot_hr_total"]:.1f}% as scan entries evict hot data. The hot p90 latency
tells the story: {e1["configs"][0]["hot_p90"]:.2f}ms (1%) vs {e1["configs"][2]["hot_p90"]:.2f}ms (99%).
</div>\n'''

    # Charts
    html += '<div class="chart-grid">\n'
    html += '  <div class="chart-card"><div id="e1-hot-hr" style="height:350px"></div></div>\n'
    html += '  <div class="chart-card"><div id="e1-scan-hr" style="height:350px"></div></div>\n'
    html += '  <div class="chart-card"><div id="e1-hot-p90" style="height:350px"></div></div>\n'
    html += '  <div class="chart-card"><div id="e1-hot-p50" style="height:350px"></div></div>\n'
    html += '  <div class="chart-card full"><div id="e1-bar" style="height:370px"></div></div>\n'
    html += '</div>\n'

    # Summary table
    html += '<h3>Hot Read Latency Summary (ms, averaged over attack period)</h3>\n'
    html += '<table><thead><tr><th>Percentile</th>'
    for cfg in e1["configs"]:
        html += f'<th style="color:{cfg["color"]}">{cfg["name"]}</th>'
    html += '</tr></thead><tbody>\n'
    for p in [50, 75, 90, 95, 99]:
        html += f'<tr><td>p{p}</td>'
        for cfg in e1["configs"]:
            val = cfg["hot_pcts_mean"][p]
            html += f'<td style="color:{cfg["color"]}">{val:.3f}</td>'
        html += '</tr>\n'
    html += '</tbody></table>\n'

    html += '</div>\n'  # end section

    # =========================================================================
    # EXPERIMENT 2: Queue workload
    # =========================================================================
    html += '<div class="section">\n'
    html += '<h2>Experiment 2: Queue Workload (Window = 1% vs 99%)</h2>\n'
    html += """<p>A looping FIFO queue: items 0–159,999 accessed sequentially, one access per item,
    wrapping around. Queue size = 2× cache (160k items through an 80k-slot cache).
    This is a <strong>sequential scan pattern with no frequency signal</strong> —
    every item has the same access frequency. Neither LRU nor W-TinyLFU can achieve
    high hit rates when the working set exceeds the cache, but the comparison reveals
    how the frequency admission filter behaves when there's nothing useful to filter.</p>\n"""

    e2 = exp2_data
    html += '<div class="kpi-row">\n'
    for cfg in e2["configs"]:
        html += f'''<div class="kpi">
  <div class="label">{cfg["name"]} — Hit Rate</div>
  <div class="val {cfg["color_cls"]}">{cfg["hr"]:.1f}%</div>
</div>
<div class="kpi">
  <div class="label">{cfg["name"]} — p90 Latency</div>
  <div class="val {cfg["color_cls"]}">{cfg["p90"]:.2f}ms</div>
</div>\n'''
    html += '</div>\n'

    # Insight
    html += f'''<div class="insight warn">
<strong>Queue workload — classic cache thrashing:</strong>
With a queue of 2× cache size, items are re-seen only after 160k other accesses — far beyond
what any cache of 80k slots can retain. Both window sizes achieve low hit rates:
1% window → {e2["configs"][0]["hr"]:.1f}%, 99% window → {e2["configs"][1]["hr"]:.1f}%.
The comparison shows how the admission filter and SLRU structure interact under a
workload with zero useful frequency signal. A future adaptive algorithm could
detect this low-hit-rate regime and adjust the window size accordingly.
</div>\n'''

    html += '<div class="chart-grid">\n'
    html += '  <div class="chart-card"><div id="e2-hr" style="height:350px"></div></div>\n'
    html += '  <div class="chart-card"><div id="e2-p90" style="height:350px"></div></div>\n'
    html += '</div>\n'

    # Queue summary table
    html += '<h3>Queue Workload Latency Summary (ms)</h3>\n'
    html += '<table><thead><tr><th>Percentile</th>'
    for cfg in e2["configs"]:
        html += f'<th style="color:{cfg["color"]}">{cfg["name"]}</th>'
    html += '</tr></thead><tbody>\n'
    for p in [50, 75, 90, 95, 99]:
        html += f'<tr><td>p{p}</td>'
        for cfg in e2["configs"]:
            val = cfg["pcts_mean"][p]
            html += f'<td style="color:{cfg["color"]}">{val:.3f}</td>'
        html += '</tr>\n'
    html += '</tbody></table>\n'

    html += '</div>\n'  # end section

    # =========================================================================
    # Conclusion
    # =========================================================================
    html += '<div class="section">\n'
    html += '<h2>Conclusions</h2>\n'
    html += f'''<div class="insight">
<strong>No single window size is optimal for all workloads.</strong>
Under scan attack, a 1% window (frequency-dominant) delivers
{e1["configs"][0]["hot_hr_total"]:.0f}% hot hit rate vs {e1["configs"][2]["hot_hr_total"]:.0f}% for 99% window.
Under a sequential queue (no frequency signal), both policies struggle when the working set
exceeds cache size. Caffeine includes a hill-climbing algorithm that adaptively
adjusts the window size; our implementation currently uses a static window fraction.
The <code>tinylfu_initial_window_fraction</code>
config knob lets operators set the window size for their workload profile.
</div>\n'''
    html += '</div>\n'

    html += '<div class="params">Cache: 80,000 slots · Hot: 80k keys (Zipfian s=1.0) · Scan: 500k keys · Queue: 160k items (looping FIFO) · Warmup: 120s · Attack: 120s · 10k ops/s</div>\n'

    # =========================================================================
    # JavaScript
    # =========================================================================
    html += '<script>\n'
    html += f'const E1 = {json.dumps(exp1_data)};\n'
    html += f'const E2 = {json.dumps(exp2_data)};\n'

    html += r"""
const plotBg = '#111827', paperBg = '#111827', gridColor = '#1e3a5f', textColor = '#8899aa';
const W_COLORS = {'1%': '#06d6a0', '50%': '#ffd166', '99%': '#ef476f'};
const Q_COLORS = {'W-TinyLFU 1%': '#06d6a0', 'W-TinyLFU 99%': '#ef476f'};

const plotCfg = {responsive:true, displaylogo:false,
  modeBarButtonsToRemove:['lasso2d','select2d']};

function layout(title, yTitle, yRange) {
  return {
    title:{text:title, font:{size:14, color:'#00b4d8'}},
    paper_bgcolor:paperBg, plot_bgcolor:plotBg,
    font:{color:textColor, size:11},
    margin:{t:50,b:50,l:60,r:20},
    xaxis:{title:'Time into attack (s)', gridcolor:gridColor, zerolinecolor:gridColor},
    yaxis:{title:yTitle, gridcolor:gridColor, zerolinecolor:gridColor, range:yRange||undefined},
    legend:{bgcolor:'rgba(0,0,0,0)', font:{color:'#e2e8f0'}, x:0.01, y:0.99},
    hovermode:'x unified',
  };
}

// -- Experiment 1 charts --
const e1Traces = (field) => E1.configs.map(c => ({
  x: c.time, y: c[field], name: c.name,
  line: {color: c.color, width: 2.5}
}));

Plotly.newPlot('e1-hot-hr', e1Traces('hot_hr'),
  layout('Hot Workload Hit Rate', 'Hit rate (%)', [0, 105]), plotCfg);
Plotly.newPlot('e1-scan-hr', e1Traces('scan_hr'),
  layout('Scan Workload Hit Rate', 'Hit rate (%)', [0, 105]), plotCfg);
Plotly.newPlot('e1-hot-p90', e1Traces('hot_p90_ts'),
  layout('Hot Read p90 Latency', 'Latency (ms)'), plotCfg);
Plotly.newPlot('e1-hot-p50', e1Traces('hot_p50_ts'),
  layout('Hot Read p50 Latency', 'Latency (ms)'), plotCfg);

// Bar chart
const barPcts = ['p50','p75','p90','p95','p99'];
const barTraces = E1.configs.map(c => ({
  x: barPcts,
  y: barPcts.map(p => c.hot_pcts_mean[parseInt(p.slice(1))]),
  name: c.name, type: 'bar', marker: {color: c.color},
}));
Plotly.newPlot('e1-bar', barTraces, {
  ...layout('Hot Read Latency by Percentile', 'Avg latency (ms)'),
  barmode: 'group', margin:{t:50,b:50,l:60,r:40},
}, plotCfg);

// -- Experiment 2 charts --
const e2Traces = (field) => E2.configs.map(c => ({
  x: c.time, y: c[field], name: c.name,
  line: {color: c.color, width: 2.5}
}));

Plotly.newPlot('e2-hr', e2Traces('hr_ts'),
  layout('Queue Hit Rate Over Time', 'Hit rate (%)', [0, 105]), plotCfg);
Plotly.newPlot('e2-p90', e2Traces('p90_ts'),
  layout('Queue p90 Latency Over Time', 'Latency (ms)'), plotCfg);
"""

    html += '</script>\n</body>\n</html>\n'
    return html


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    CACHE_SLOTS = 80_000
    HOT_KEYS = 80_000
    SCAN_KEYS = 500_000
    WARMUP_S = 120
    ATTACK_S = 120
    SAMPLE_S = 5
    ZIPF_EXP = 1.0
    SEED = 42
    QUEUE_SIZE = CACHE_SLOTS * 2  # 160k
    QUEUE_OPS = 120 * 10_000      # match attack duration in ops

    warmup_ops = WARMUP_S * 5000
    pcts_list = [50, 75, 90, 95, 99]

    window_configs = [
        ("1%",  0.01, "#06d6a0", "c-w1"),
        ("50%", 0.50, "#ffd166", "c-w50"),
        ("99%", 0.99, "#ef476f", "c-w99"),
    ]

    # =====================================================================
    # Experiment 1: Hot + Scan
    # =====================================================================
    print("=" * 60)
    print("EXPERIMENT 1: Hot + Scan Attack")
    print("=" * 60)

    exp1_configs = []
    for name, wpct, color, color_cls in window_configs:
        label = f"Window={name}"
        print(f"\nRunning W-TinyLFU ({label})...", end=" ", flush=True)
        t0 = time.time()
        rng = random.Random(SEED)
        gen = ZipfianGenerator(HOT_KEYS, s=ZIPF_EXP, rng=rng)
        cache = WTinyLFUCache(CACHE_SLOTS, window_pct=wpct)
        res = run_hot_scan_proper(cache, gen, HOT_KEYS, SCAN_KEYS,
                                  warmup_ops, ATTACK_S, SAMPLE_S)
        elapsed = time.time() - t0
        print(f"done ({elapsed:.1f}s)")

        # Compute metrics
        hot_hr_ts = [h / t * 100 if t else 0
                     for h, t in zip(res["hot_hits"], res["hot_total"])]
        scan_hr_ts = [h / t * 100 if t else 0
                      for h, t in zip(res["scan_hits"], res["scan_total"])]

        hp = pctiles(res["hot_lats"], pcts_list)
        sp = pctiles(res["scan_lats"], pcts_list)

        total_hh = sum(res["hot_hits"])
        total_ht = sum(res["hot_total"])
        total_sh = sum(res["scan_hits"])
        total_st = sum(res["scan_total"])
        hot_hr = total_hh / total_ht * 100 if total_ht else 0
        scan_hr = total_sh / total_st * 100 if total_st else 0
        hot_p90 = float(np.mean(hp[90]))

        hot_pcts_mean = {p: round(float(np.mean(hp[p])), 3) for p in pcts_list}

        print(f"  Hot HR: {hot_hr:.1f}%  Scan HR: {scan_hr:.1f}%  Hot p90: {hot_p90:.2f}ms")

        exp1_configs.append({
            "name": label,
            "color": color,
            "color_cls": color_cls,
            "time": res["time"],
            "hot_hr": [round(v, 2) for v in hot_hr_ts],
            "scan_hr": [round(v, 2) for v in scan_hr_ts],
            "hot_p90_ts": [round(v, 3) for v in hp[90]],
            "hot_p50_ts": [round(v, 3) for v in hp[50]],
            "hot_hr_total": round(hot_hr, 1),
            "scan_hr_total": round(scan_hr, 1),
            "hot_p90": round(hot_p90, 2),
            "hot_pcts_mean": hot_pcts_mean,
        })

    exp1_data = {"configs": exp1_configs}

    # =====================================================================
    # Experiment 2: Queue workload
    # =====================================================================
    print("\n" + "=" * 60)
    print("EXPERIMENT 2: Queue Workload (2× cache)")
    print("=" * 60)

    queue_configs = [
        ("W-TinyLFU 1%",  0.01, "#06d6a0", "c-w1"),
        ("W-TinyLFU 99%", 0.99, "#ef476f", "c-w99"),
    ]

    sample_ops = 50_000  # sample every 50k ops

    exp2_configs = []
    for name, wpct, color, color_cls in queue_configs:
        print(f"\nRunning {name} on queue...", end=" ", flush=True)
        t0 = time.time()
        cache = WTinyLFUCache(CACHE_SLOTS, window_pct=wpct)
        res = run_queue(cache, QUEUE_SIZE, QUEUE_OPS, sample_ops)
        elapsed = time.time() - t0
        print(f"done ({elapsed:.1f}s)")

        hr_ts = [h / t * 100 if t else 0
                 for h, t in zip(res["hits"], res["total"])]
        lp = pctiles(res["lats"], pcts_list)

        total_h = sum(res["hits"])
        total_t = sum(res["total"])
        hr = total_h / total_t * 100 if total_t else 0
        p90 = float(np.mean(lp[90]))

        pcts_mean = {p: round(float(np.mean(lp[p])), 3) for p in pcts_list}

        print(f"  Hit Rate: {hr:.1f}%  p90: {p90:.2f}ms")

        exp2_configs.append({
            "name": name,
            "color": color,
            "color_cls": color_cls,
            "time": res["time"],
            "hr": round(hr, 1),
            "p90": round(p90, 2),
            "hr_ts": [round(v, 2) for v in hr_ts],
            "p90_ts": [round(v, 3) for v in lp[90]],
            "pcts_mean": pcts_mean,
        })

    exp2_data = {"configs": exp2_configs}

    # =====================================================================
    # Generate HTML
    # =====================================================================
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "wtinylfu_window_comparison.html")
    html = generate_html(exp1_data, exp2_data)
    with open(out_path, "w") as f:
        f.write(html)
    print(f"\nHTML report saved to {out_path}")
    print("Open in a browser to view interactive charts.")


if __name__ == "__main__":
    main()

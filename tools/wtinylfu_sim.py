#!/usr/bin/env python3
"""
W-TinyLFU cache simulator with live web dashboard.

Simulates the exact same admission logic as utils/lru.hh:
  - Window / Probation / Protected segments
  - Count-Min Sketch with 4-bit counters
  - Admission: window victim freq vs probation victim freq
  - Protected promotion on re-access in probation

Run:  python3 tools/wtinylfu_sim.py [--port 8765]
Open: http://localhost:8765

Adjust parameters via the web UI or query string.
"""

import argparse
import collections
import hashlib
import http.server
import json
import math
import os
import random
import struct
import threading
import time
from io import BytesIO

# =============================================================================
# Count-Min Sketch (conceptually similar to utils/count_min_sketch.hh;
# uses a simpler per-row layout instead of the cache-line-optimized
# block layout used in production)
# =============================================================================

class CountMinSketch:
    """4-row Count-Min Sketch with 4-bit counters (max 15)."""

    def __init__(self, width_log2=10):
        self.width_log2 = width_log2
        self.width = 1 << width_log2
        self.mask = self.width - 1
        # 4 rows of counters
        self.rows = [bytearray(self.width) for _ in range(4)]
        # Different hash seeds per row
        self.seeds = [0x9e3779b9, 0x517cc1b7, 0x6a09e667, 0xbb67ae85]

    def _hash(self, key, row):
        h = key * self.seeds[row]
        h ^= h >> 16
        h &= 0xFFFFFFFF
        return h & self.mask

    def increment(self, key):
        for row in range(4):
            idx = self._hash(key, row)
            if self.rows[row][idx] < 15:
                self.rows[row][idx] += 1

    def estimate(self, key):
        return min(self.rows[row][self._hash(key, row)] for row in range(4))

    def reset(self):
        """Halve all counters (aging)."""
        for row in self.rows:
            for i in range(len(row)):
                row[i] >>= 1


# =============================================================================
# Cache Entry
# =============================================================================

class CacheEntry:
    __slots__ = ['key', 'source', 'segment', 'insert_time', 'last_access', 'addr']

    def __init__(self, key, source, segment, now):
        self.key = key
        self.source = source  # 'hot' or 'scan'
        self.segment = segment  # 'window', 'probation', 'protected'
        self.insert_time = now
        self.last_access = now
        self.addr = 0


# =============================================================================
# W-TinyLFU Cache Simulator
# =============================================================================

class WTinyLFUCache:
    def __init__(self, capacity, window_pct, address_keying=False):
        self.capacity = capacity
        self.window_pct = window_pct

        self.window_max = max(1, int(capacity * window_pct / 100))
        slru = capacity - self.window_max
        self.probation_max = max(1, int(slru * 0.20))
        self.protected_max = slru - self.probation_max

        # Segments as ordered dicts (key → CacheEntry), insertion order = LRU order
        self.window = collections.OrderedDict()
        self.probation = collections.OrderedDict()
        self.protected = collections.OrderedDict()

        # Lookup: key → segment name (for O(1) hit check)
        self.lookup = {}

        self.sketch = CountMinSketch(width_log2=max(8, int(math.log2(capacity * 4))))

        self.sample_count = 0
        self.sample_threshold = max(100, capacity * 10)

        self.address_keying = address_keying
        self._next_addr = 0

        self.jitter_state = 0xDEADBEEF

        # Stats
        self.hot_hits = 0
        self.hot_misses = 0
        self.scan_hits = 0
        self.scan_misses = 0
        self.admissions = 0
        self.rejections = 0
        self.tick = 0

        # Per-batch stats (reset each batch)
        self.batch_hot_hits = 0
        self.batch_hot_misses = 0
        self.batch_scan_hits = 0
        self.batch_scan_misses = 0

        # History for charts (ring buffer)
        self.history_len = 600
        self.history = collections.deque(maxlen=self.history_len)

    def _get_sketch_key(self, key):
        """Get the sketch key for an entry. With address_keying, uses stored address."""
        if not self.address_keying:
            return key
        seg = self.lookup.get(key)
        if seg is not None:
            entry = self._segment_dict(seg)[key]
            return entry.addr
        return key  # new entry not yet in cache

    def _entry_sketch_key(self, key, seg_name):
        """Get sketch key for a specific entry in a specific segment."""
        if not self.address_keying:
            return key
        seg = self._segment_dict(seg_name)
        if key in seg:
            return seg[key].addr
        return key

    def _jitter(self):
        s = self.jitter_state & 0xFFFFFFFF
        s ^= (s << 13) & 0xFFFFFFFF
        s ^= (s >> 17)
        s ^= (s << 5) & 0xFFFFFFFF
        self.jitter_state = s
        return s

    def _record_access(self, key):
        self.sketch.increment(self._get_sketch_key(key))
        self.sample_count += 1
        if self.sample_count >= self.sample_threshold:
            self.sketch.reset()
            self.sample_count = 0

    def _segment_dict(self, seg_name):
        if seg_name == 'window':
            return self.window
        elif seg_name == 'probation':
            return self.probation
        elif seg_name == 'protected':
            return self.protected

    def _rebalance_protected(self):
        """Demote oldest protected entries to probation if protected exceeds max."""
        while len(self.protected) > self.protected_max and self.protected:
            key, entry = self.protected.popitem(last=False)  # pop oldest
            entry.segment = 'probation'
            self.probation[key] = entry
            self.lookup[key] = 'probation'

    def _drain_window(self):
        """Drain window to target size using admission filter."""
        self._rebalance_protected()
        while len(self.window) > self.window_max and self.window:
            w_key, w_entry = next(iter(self.window.items()))

            if self.probation:
                p_key, p_entry = next(iter(self.probation.items()))
                w_freq = self.sketch.estimate(self._entry_sketch_key(w_key, 'window'))
                p_freq = self.sketch.estimate(self._entry_sketch_key(p_key, 'probation'))

                if w_freq > p_freq:
                    admit = True
                elif w_freq >= 6:
                    admit = (self._jitter() & 127) == 0
                else:
                    admit = False

                if admit:
                    del self.window[w_key]
                    w_entry.segment = 'probation'
                    self.probation[w_key] = w_entry
                    self.lookup[w_key] = 'probation'
                    del self.probation[p_key]
                    del self.lookup[p_key]
                    self.admissions += 1
                else:
                    del self.window[w_key]
                    del self.lookup[w_key]
                    self.rejections += 1
                # continue draining until window reaches target size
            else:
                del self.window[w_key]
                w_entry.segment = 'probation'
                self.probation[w_key] = w_entry
                self.lookup[w_key] = 'probation'

    def _evict_for_capacity(self):
        """Evict one entry when over capacity."""
        for seg_name in ['probation', 'window', 'protected']:
            seg = self._segment_dict(seg_name)
            if seg:
                key, entry = seg.popitem(last=False)
                del self.lookup[key]
                return

    def access(self, key, source):
        """Access a key. Returns True if hit, False if miss."""
        self.tick += 1
        now = self.tick

        seg_name = self.lookup.get(key)
        if seg_name is not None:
            # HIT
            self._record_access(key)
            seg = self._segment_dict(seg_name)
            entry = seg[key]
            entry.last_access = now

            if seg_name == 'window':
                seg.move_to_end(key)
            elif seg_name == 'probation':
                del seg[key]
                entry.segment = 'protected'
                self.protected[key] = entry
                self.lookup[key] = 'protected'
            elif seg_name == 'protected':
                seg.move_to_end(key)

            if source == 'hot':
                self.hot_hits += 1
                self.batch_hot_hits += 1
            else:
                self.scan_hits += 1
                self.batch_scan_hits += 1
            return True
        else:
            # MISS — insert into window
            self._record_access(key)
            entry = CacheEntry(key, source, 'window', now)
            if self.address_keying:
                entry.addr = self._next_addr
                self._next_addr += 1
            self.window[key] = entry
            self.lookup[key] = 'window'

            # Evict if over total capacity (this triggers window drain too,
            # matching real ScyllaDB where LSA reclaimer calls do_evict)
            total = len(self.window) + len(self.probation) + len(self.protected)
            while total > self.capacity:
                self._drain_window()
                total = len(self.window) + len(self.probation) + len(self.protected)
                if total > self.capacity:
                    self._evict_for_capacity()
                    total = len(self.window) + len(self.probation) + len(self.protected)

            if source == 'hot':
                self.hot_misses += 1
                self.batch_hot_misses += 1
            else:
                self.scan_misses += 1
                self.batch_scan_misses += 1
            return False

    def snapshot(self):
        """Return current state for the dashboard."""
        now = self.tick

        def seg_info(seg, name):
            hot_keys = []
            scan_keys = []
            ages = []
            for k, e in seg.items():
                age = now - e.insert_time
                ages.append(age)
                if e.source == 'hot':
                    hot_keys.append(k)
                else:
                    scan_keys.append(k)
            return {
                'name': name,
                'size': len(seg),
                'hot_count': len(hot_keys),
                'scan_count': len(scan_keys),
                'hot_keys': hot_keys,
                'scan_keys': scan_keys,
                'avg_age': sum(ages) / len(ages) if ages else 0,
                'max_age': max(ages) if ages else 0,
                'min_age': min(ages) if ages else 0,
                'ages': ages,
            }

        # Sketch frequency for all cached entries
        freq_hot = []
        freq_scan = []
        for k, seg_name in self.lookup.items():
            f = self.sketch.estimate(k)
            seg = self._segment_dict(seg_name)
            entry = seg[k]
            if entry.source == 'hot':
                freq_hot.append(f)
            else:
                freq_scan.append(f)

        hot_total = self.hot_hits + self.hot_misses
        scan_total = self.scan_hits + self.scan_misses

        return {
            'tick': now,
            'capacity': self.capacity,
            'window_pct': self.window_pct,
            'segments': {
                'window': seg_info(self.window, 'window'),
                'probation': seg_info(self.probation, 'probation'),
                'protected': seg_info(self.protected, 'protected'),
            },
            'hot_hr': self.hot_hits / hot_total * 100 if hot_total else 0,
            'scan_hr': self.scan_hits / scan_total * 100 if scan_total else 0,
            'hot_hits': self.hot_hits,
            'hot_misses': self.hot_misses,
            'scan_hits': self.scan_hits,
            'scan_misses': self.scan_misses,
            'admissions': self.admissions,
            'rejections': self.rejections,
            'freq_hot': freq_hot,
            'freq_scan': freq_scan,
        }

    def record_history(self):
        """Record a history point for time-series charts."""
        hot_total = self.hot_hits + self.hot_misses
        scan_total = self.scan_hits + self.scan_misses
        batch_hot = self.batch_hot_hits + self.batch_hot_misses
        batch_scan = self.batch_scan_hits + self.batch_scan_misses
        self.history.append({
            'tick': self.tick,
            'hot_hr': self.hot_hits / hot_total * 100 if hot_total else 0,
            'scan_hr': self.scan_hits / scan_total * 100 if scan_total else 0,
            'batch_hot_hr': self.batch_hot_hits / batch_hot * 100 if batch_hot else 0,
            'batch_scan_hr': self.batch_scan_hits / batch_scan * 100 if batch_scan else 0,
            'window_hot': sum(1 for e in self.window.values() if e.source == 'hot'),
            'window_scan': sum(1 for e in self.window.values() if e.source == 'scan'),
            'probation_hot': sum(1 for e in self.probation.values() if e.source == 'hot'),
            'probation_scan': sum(1 for e in self.probation.values() if e.source == 'scan'),
            'protected_hot': sum(1 for e in self.protected.values() if e.source == 'hot'),
            'protected_scan': sum(1 for e in self.protected.values() if e.source == 'scan'),
        })
        self.batch_hot_hits = 0
        self.batch_hot_misses = 0
        self.batch_scan_hits = 0
        self.batch_scan_misses = 0


# =============================================================================
# Simulation Runner
# =============================================================================

class SimRunner:
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.cache = None
        self.address_keying = False
        self.params = {
            'cache_capacity': 1000,
            'hot_keys': 500,
            'scan_keys': 10000,
            'zipf_s': 0.8,
            'window_pct': 1,
            'hot_rate': 500,   # ops per batch
            'scan_rate': 1500,
            'phase': 'warmup',  # warmup or attack
        }
        self.configs = []  # list of window_pct values to compare
        self.caches = {}   # window_pct → WTinyLFUCache
        self.scan_cursor = 0
        self.batch_count = 0

    def reset(self, params=None):
        with self.lock:
            if params:
                self.params.update(params)
            p = self.params
            self.caches = {}
            configs = [1, 50, 99]
            for wpct in configs:
                self.caches[wpct] = WTinyLFUCache(p['cache_capacity'], wpct, address_keying=self.address_keying)
            self.configs = configs
            self.scan_cursor = 0
            self.batch_count = 0
            self.params['phase'] = 'warmup'

    def _zipf_key(self, i):
        """Generate a Zipfian hot key."""
        p = self.params
        N = p['hot_keys']
        s = p['zipf_s']
        alpha = 1.0 / (1.0 - s)
        # Use a hash of i to get a pseudo-random uniform value
        h = hash((i, 0x12345)) & 0xFFFFFFFF
        u = (h % 1000000 + 1) / 1000001.0
        pk = int(N * (u ** alpha)) % N
        return pk

    def run_batch(self):
        """Run one batch of operations on all caches."""
        with self.lock:
            p = self.params
            phase = p['phase']

            for cache in self.caches.values():
                if phase == 'warmup':
                    # Hot only
                    for i in range(p['hot_rate']):
                        key = self._zipf_key(self.batch_count * p['hot_rate'] + i)
                        cache.access(key, 'hot')
                else:
                    # Hot + scan
                    for i in range(p['hot_rate']):
                        key = self._zipf_key(self.batch_count * p['hot_rate'] + i)
                        cache.access(key, 'hot')
                    for i in range(p['scan_rate']):
                        key = p['hot_keys'] + (self.scan_cursor % p['scan_keys'])
                        self.scan_cursor += 1
                        cache.access(key, 'scan')

                cache.record_history()

            self.batch_count += 1

    def get_state(self):
        """Get current state of all caches."""
        with self.lock:
            result = {
                'params': dict(self.params),
                'batch': self.batch_count,
                'configs': {},
            }
            for wpct, cache in self.caches.items():
                snap = cache.snapshot()
                snap['history'] = list(cache.history)
                result['configs'][str(wpct)] = snap
            return result


# =============================================================================
# Web Server
# =============================================================================

SIM = SimRunner()
STATIC_DIR = os.path.dirname(os.path.abspath(__file__))


class SimHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # suppress request logs

    def do_GET(self):
        if self.path == '/' or self.path.startswith('/?'):
            self._serve_html()
        elif self.path == '/api/state':
            self._json_response(SIM.get_state())
        elif self.path.startswith('/api/reset'):
            self._handle_reset()
        elif self.path == '/api/step':
            SIM.run_batch()
            self._json_response({'ok': True})
        elif self.path.startswith('/api/phase'):
            phase = 'attack' if 'attack' in self.path else 'warmup'
            SIM.params['phase'] = phase
            self._json_response({'phase': phase})
        else:
            self.send_error(404)

    def _json_response(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def _handle_reset(self):
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        params = {}
        for k in ['cache_capacity', 'hot_keys', 'scan_keys', 'hot_rate', 'scan_rate']:
            if k in qs:
                params[k] = int(qs[k][0])
        for k in ['zipf_s']:
            if k in qs:
                params[k] = float(qs[k][0])
        SIM.reset(params)
        self._json_response({'ok': True, 'params': SIM.params})

    def _serve_html(self):
        html = HTML_PAGE.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.send_header('Content-Length', len(html))
        self.end_headers()
        self.wfile.write(html)


def sim_loop():
    """Background simulation loop."""
    while True:
        if SIM.caches:
            SIM.run_batch()
        time.sleep(0.05)  # 20 batches/s


# =============================================================================
# HTML Dashboard
# =============================================================================

HTML_PAGE = r"""<!DOCTYPE html>
<html>
<head>
<title>W-TinyLFU Cache Simulator</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
       background: #0a0a0f; color: #c0c0c0; font-size: 13px; }

.header { background: #12121a; padding: 12px 20px; border-bottom: 1px solid #222;
          display: flex; align-items: center; gap: 20px; }
.header h1 { font-size: 15px; color: #7aa2f7; font-weight: 600; }
.header .phase { padding: 3px 10px; border-radius: 3px; font-size: 12px; font-weight: 600; }
.phase-warmup { background: #1a3a1a; color: #4ade80; }
.phase-attack { background: #3a1a1a; color: #f87171; }

.controls { background: #12121a; padding: 10px 20px; border-bottom: 1px solid #222;
            display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
.controls label { color: #888; font-size: 11px; }
.controls input { background: #1a1a24; border: 1px solid #333; color: #c0c0c0;
                  padding: 3px 6px; width: 70px; border-radius: 3px; font-family: inherit; }
.controls button { background: #1a2a4a; border: 1px solid #334; color: #7aa2f7;
                   padding: 4px 12px; border-radius: 3px; cursor: pointer;
                   font-family: inherit; font-size: 12px; }
.controls button:hover { background: #2a3a5a; }
.controls button.danger { background: #3a1a1a; color: #f87171; border-color: #533; }
.controls button.active { background: #2a4a2a; color: #4ade80; border-color: #353; }

.grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 1px;
        background: #222; padding: 1px; }
.panel { background: #12121a; padding: 12px; min-height: 200px; }
.panel h2 { font-size: 12px; color: #7aa2f7; margin-bottom: 8px;
            text-transform: uppercase; letter-spacing: 1px; }
.panel h3 { font-size: 11px; color: #888; margin: 6px 0 4px; }

.kpi-row { display: flex; gap: 8px; margin-bottom: 8px; }
.kpi { text-align: center; flex: 1; padding: 6px; border-radius: 4px; background: #1a1a24; }
.kpi .val { font-size: 18px; font-weight: 700; }
.kpi .lbl { font-size: 10px; color: #666; margin-top: 2px; }
.hot-color { color: #f7768e; }
.scan-color { color: #7dcfff; }
.win-color { color: #e0af68; }
.prob-color { color: #bb9af7; }
.prot-color { color: #9ece6a; }

/* Segment bars */
.seg-bar { display: flex; height: 24px; border-radius: 3px; overflow: hidden;
           margin: 4px 0; background: #1a1a24; }
.seg-bar .chunk { display: flex; align-items: center; justify-content: center;
                  font-size: 10px; font-weight: 600; color: #000; min-width: 2px; }
.chunk-hot-win { background: #e0af68; }
.chunk-scan-win { background: #544a30; }
.chunk-hot-prob { background: #bb9af7; }
.chunk-scan-prob { background: #443a5a; }
.chunk-hot-prot { background: #9ece6a; }
.chunk-scan-prot { background: #3a4a2a; }

/* Key grid */
.key-grid { display: flex; flex-wrap: wrap; gap: 1px; margin: 4px 0; }
.key-cell { width: 5px; height: 5px; border-radius: 1px; }
.key-none { background: #1a1a24; }
.key-window { background: #e0af68; }
.key-probation { background: #bb9af7; }
.key-protected { background: #9ece6a; }

/* Age histogram */
.age-hist { display: flex; align-items: flex-end; gap: 1px; height: 40px; margin: 4px 0; }
.age-bar { flex: 1; min-width: 2px; border-radius: 1px 1px 0 0; }

/* Frequency distribution */
.freq-dist { display: flex; gap: 2px; align-items: flex-end; height: 50px; margin: 4px 0; }
.freq-bar { flex: 1; border-radius: 2px 2px 0 0; position: relative; }
.freq-bar .freq-lbl { position: absolute; bottom: -14px; font-size: 9px; color: #555;
                       text-align: center; width: 100%; }

canvas { image-rendering: pixelated; }

.config-tabs { display: flex; gap: 1px; margin-bottom: 8px; }
.config-tab { padding: 4px 12px; background: #1a1a24; cursor: pointer;
              border-radius: 3px 3px 0 0; font-size: 11px; }
.config-tab.active { background: #1a2a4a; color: #7aa2f7; }

.wide { grid-column: span 3; }
.span2 { grid-column: span 2; }

.legend { display: flex; gap: 12px; font-size: 10px; margin: 6px 0; flex-wrap: wrap; }
.legend span { display: flex; align-items: center; gap: 4px; }
.legend .dot { width: 8px; height: 8px; border-radius: 2px; display: inline-block; }
</style>
</head>
<body>

<div class="header">
  <h1>W-TinyLFU Cache Simulator</h1>
  <span id="phase-badge" class="phase phase-warmup">WARMUP</span>
  <span style="color:#555">Batch: <span id="batch-num">0</span></span>
  <span style="color:#555">Speed: <span id="speed-val">20</span> batches/s</span>
</div>

<div class="controls">
  <label>Cache <input id="p-capacity" type="number" value="1000"></label>
  <label>Hot keys <input id="p-hot" type="number" value="500"></label>
  <label>Scan keys <input id="p-scan" type="number" value="10000"></label>
  <label>Zipf s <input id="p-zipf" type="number" step="0.01" value="0.80"></label>
  <label>Hot/batch <input id="p-hotrate" type="number" value="500"></label>
  <label>Scan/batch <input id="p-scanrate" type="number" value="1500"></label>
  <button onclick="doReset()">Reset & Start</button>
  <button onclick="setPhase('warmup')" id="btn-warmup" class="active">Warmup</button>
  <button onclick="setPhase('attack')" id="btn-attack" class="danger">Attack!</button>
  <button onclick="togglePause()" id="btn-pause">Pause</button>
</div>

<div class="grid" id="main-grid">
  <!-- Populated by JS -->
</div>

<script>
const POLL_MS = 200;
let paused = false;
let state = null;

function doReset() {
  const p = {
    cache_capacity: +document.getElementById('p-capacity').value,
    hot_keys: +document.getElementById('p-hot').value,
    scan_keys: +document.getElementById('p-scan').value,
    zipf_s: +document.getElementById('p-zipf').value,
    hot_rate: +document.getElementById('p-hotrate').value,
    scan_rate: +document.getElementById('p-scanrate').value,
  };
  const qs = Object.entries(p).map(([k,v]) => `${k}=${v}`).join('&');
  fetch(`/api/reset?${qs}`).then(() => fetch('/api/state').then(r => r.json()).then(render));
}

function setPhase(ph) {
  fetch(`/api/phase?p=${ph}`);
  document.getElementById('btn-warmup').classList.toggle('active', ph === 'warmup');
  document.getElementById('btn-attack').classList.toggle('active', ph === 'attack');
}

function togglePause() {
  paused = !paused;
  document.getElementById('btn-pause').textContent = paused ? 'Resume' : 'Pause';
  document.getElementById('btn-pause').classList.toggle('active', paused);
}

function makeSegBar(seg, maxCap) {
  const total = seg.window.hot + seg.window.scan +
                seg.probation.hot + seg.probation.scan +
                seg.protected.hot + seg.protected.scan;
  const w = (v) => Math.max(0, v / maxCap * 100);
  return `<div class="seg-bar">
    <div class="chunk chunk-hot-win" style="width:${w(seg.window.hot)}%">${seg.window.hot || ''}</div>
    <div class="chunk chunk-scan-win" style="width:${w(seg.window.scan)}%">${seg.window.scan || ''}</div>
    <div class="chunk chunk-hot-prob" style="width:${w(seg.probation.hot)}%">${seg.probation.hot || ''}</div>
    <div class="chunk chunk-scan-prob" style="width:${w(seg.probation.scan)}%">${seg.probation.scan || ''}</div>
    <div class="chunk chunk-hot-prot" style="width:${w(seg.protected.hot)}%">${seg.protected.hot || ''}</div>
    <div class="chunk chunk-scan-prot" style="width:${w(seg.protected.scan)}%">${seg.protected.scan || ''}</div>
  </div>`;
}

function makeKeyGrid(cfg, hotKeys) {
  // Show hot key space: each cell = one hot key, colored by segment
  const segs = cfg.segments;
  const keyMap = {};
  for (const k of segs.window.hot_keys) keyMap[k] = 'window';
  for (const k of segs.probation.hot_keys) keyMap[k] = 'probation';
  for (const k of segs.protected.hot_keys) keyMap[k] = 'protected';

  let html = '<div class="key-grid">';
  // Show first 2000 hot keys (or all if less)
  const show = Math.min(hotKeys, 2000);
  for (let i = 0; i < show; i++) {
    const seg = keyMap[i] || 'none';
    html += `<div class="key-cell key-${seg}" title="key ${i}: ${seg}"></div>`;
  }
  html += '</div>';
  return html;
}

function makeAgeHist(ages, color, maxAge) {
  if (!ages || ages.length === 0) return '<div style="color:#444;font-size:10px">empty</div>';
  const buckets = 20;
  const step = Math.max(1, Math.ceil((maxAge || Math.max(...ages)) / buckets));
  const counts = new Array(buckets).fill(0);
  for (const a of ages) counts[Math.min(buckets - 1, Math.floor(a / step))]++;
  const maxC = Math.max(1, ...counts);
  let html = '<div class="age-hist">';
  for (let i = 0; i < buckets; i++) {
    const h = counts[i] / maxC * 100;
    html += `<div class="age-bar" style="height:${h}%;background:${color}" title="${i*step}-${(i+1)*step}: ${counts[i]}"></div>`;
  }
  html += '</div>';
  html += `<div style="font-size:9px;color:#555;display:flex;justify-content:space-between"><span>0</span><span>${maxAge} ticks</span></div>`;
  return html;
}

function makeFreqDist(freqHot, freqScan) {
  const buckets = 16; // 0-15
  const hotC = new Array(buckets).fill(0);
  const scanC = new Array(buckets).fill(0);
  for (const f of freqHot) hotC[Math.min(15, f)]++;
  for (const f of freqScan) scanC[Math.min(15, f)]++;
  const maxC = Math.max(1, ...hotC, ...scanC);

  let html = '<div class="freq-dist">';
  for (let i = 0; i < buckets; i++) {
    const hh = hotC[i] / maxC * 100;
    const sh = scanC[i] / maxC * 100;
    html += `<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:1px;height:100%;justify-content:flex-end">`;
    html += `<div style="width:100%;height:${hh}%;background:#f7768e;border-radius:2px 2px 0 0;min-height:${hotC[i]?1:0}px" title="hot freq=${i}: ${hotC[i]}"></div>`;
    html += `<div style="width:100%;height:${sh}%;background:#7dcfff;border-radius:2px 2px 0 0;min-height:${scanC[i]?1:0}px" title="scan freq=${i}: ${scanC[i]}"></div>`;
    html += `</div>`;
  }
  html += '</div>';
  html += '<div style="font-size:9px;color:#555;display:flex;justify-content:space-between"><span>freq 0</span><span>15</span></div>';
  return html;
}

function render(data) {
  if (!data || !data.configs) return;
  state = data;
  const p = data.params;

  document.getElementById('batch-num').textContent = data.batch;
  const phaseBadge = document.getElementById('phase-badge');
  phaseBadge.textContent = p.phase.toUpperCase();
  phaseBadge.className = `phase phase-${p.phase}`;

  const grid = document.getElementById('main-grid');
  let html = '';

  const configs = Object.keys(data.configs).sort((a,b) => +a - +b);

  // Time-series hit rate chart (wide, spans all columns)
  html += `<div class="panel wide">
    <h2>Hot Hit Rate Over Time (per batch)</h2>
    <canvas id="hr-canvas" width="1200" height="150" style="width:100%;height:150px;background:#0d0d14;border-radius:4px;"></canvas>
  </div>`;

  for (const wpct of configs) {
    const cfg = data.configs[wpct];
    const segs = cfg.segments;
    const maxAge = Math.max(
      segs.window.max_age || 1,
      segs.probation.max_age || 1,
      segs.protected.max_age || 1
    );

    const segData = {
      window: { hot: segs.window.hot_count, scan: segs.window.scan_count },
      probation: { hot: segs.probation.hot_count, scan: segs.probation.scan_count },
      protected: { hot: segs.protected.hot_count, scan: segs.protected.scan_count },
    };

    html += `<div class="panel">
      <h2>w${wpct} (${wpct}% window)</h2>

      <div class="kpi-row">
        <div class="kpi"><div class="val hot-color">${cfg.hot_hr.toFixed(1)}%</div><div class="lbl">Hot HR</div></div>
        <div class="kpi"><div class="val scan-color">${cfg.scan_hr.toFixed(1)}%</div><div class="lbl">Scan HR</div></div>
        <div class="kpi"><div class="val" style="color:#73daca">${cfg.admissions}</div><div class="lbl">Admits</div></div>
        <div class="kpi"><div class="val" style="color:#555">${cfg.rejections}</div><div class="lbl">Rejects</div></div>
      </div>

      <h3>Segment Composition (hot vs scan)</h3>
      <div class="legend">
        <span><span class="dot" style="background:#e0af68"></span> Win/hot</span>
        <span><span class="dot" style="background:#544a30"></span> Win/scan</span>
        <span><span class="dot" style="background:#bb9af7"></span> Prob/hot</span>
        <span><span class="dot" style="background:#443a5a"></span> Prob/scan</span>
        <span><span class="dot" style="background:#9ece6a"></span> Prot/hot</span>
        <span><span class="dot" style="background:#3a4a2a"></span> Prot/scan</span>
      </div>
      ${makeSegBar(segData, cfg.capacity)}

      <h3>Window (${segs.window.size} entries, avg age ${segs.window.avg_age.toFixed(0)})</h3>
      ${makeAgeHist(segs.window.ages, '#e0af68', maxAge)}

      <h3>Probation (${segs.probation.size} entries, avg age ${segs.probation.avg_age.toFixed(0)})</h3>
      ${makeAgeHist(segs.probation.ages, '#bb9af7', maxAge)}

      <h3>Protected (${segs.protected.size} entries, avg age ${segs.protected.avg_age.toFixed(0)})</h3>
      ${makeAgeHist(segs.protected.ages, '#9ece6a', maxAge)}

      <h3>Sketch Frequency (hot=red, scan=blue)</h3>
      ${makeFreqDist(cfg.freq_hot, cfg.freq_scan)}

      <h3>Hot Key Map (${p.hot_keys} keys)</h3>
      ${makeKeyGrid(cfg, p.hot_keys)}
    </div>`;
  }

  grid.innerHTML = html;

  // Draw time-series canvas
  const canvas = document.getElementById('hr-canvas');
  if (canvas && configs.length > 0) {
    const ctx = canvas.getContext('2d');
    const W = canvas.width, H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    // Grid lines
    ctx.strokeStyle = '#1a1a24';
    ctx.lineWidth = 1;
    for (let y = 0; y <= 100; y += 25) {
      const py = H - (y / 100) * H;
      ctx.beginPath(); ctx.moveTo(0, py); ctx.lineTo(W, py); ctx.stroke();
      ctx.fillStyle = '#444'; ctx.font = '10px monospace';
      ctx.fillText(y + '%', 2, py - 2);
    }

    const colors = {'1': '#f7768e', '50': '#e0af68', '99': '#7dcfff'};
    const labels = {'1': 'w1', '50': 'w50', '99': 'w99'};

    for (const wpct of configs) {
      const hist = data.configs[wpct].history || [];
      if (hist.length < 2) continue;
      const maxPts = Math.min(hist.length, 300);
      const startIdx = hist.length - maxPts;

      ctx.strokeStyle = colors[wpct] || '#888';
      ctx.lineWidth = 2;
      ctx.beginPath();
      for (let i = 0; i < maxPts; i++) {
        const x = (i / maxPts) * W;
        const y = H - (hist[startIdx + i].batch_hot_hr / 100) * H;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      }
      ctx.stroke();

      // Label at end
      const lastHr = hist[hist.length - 1].batch_hot_hr;
      ctx.fillStyle = colors[wpct] || '#888';
      ctx.font = 'bold 11px monospace';
      ctx.fillText(`${labels[wpct]}: ${lastHr.toFixed(1)}%`, W - 100, H - (lastHr / 100) * H - 4);
    }
  }
}

async function poll() {
  if (paused) { setTimeout(poll, POLL_MS); return; }
  try {
    const r = await fetch('/api/state');
    const data = await r.json();
    render(data);
  } catch(e) {}
  setTimeout(poll, POLL_MS);
}

// Init
fetch('/api/reset?' + new URLSearchParams({
  cache_capacity: 1000, hot_keys: 500, scan_keys: 10000,
  zipf_s: 0.80, hot_rate: 500, scan_rate: 1500
})).then(() => poll());

</script>
</body>
</html>
"""

# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='W-TinyLFU Cache Simulator')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--address-keying', action='store_true',
                        help='Simulate broken address-based sketch keying (reproduces pre-fix C++ bug)')
    args = parser.parse_args()

    SIM.address_keying = args.address_keying

    # Start simulation thread
    t = threading.Thread(target=sim_loop, daemon=True)
    t.start()

    server = http.server.HTTPServer(('0.0.0.0', args.port), SimHandler)
    print(f"W-TinyLFU Simulator running at http://localhost:{args.port}")
    print(f"Open in browser to see live dashboard.")
    print(f"Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == '__main__':
    main()

---
name: perf-bench
description: perf-simple-query micro-benchmarks with CPU isolation, baseline comparison
license: LicenseRef-ScyllaDB-Source-Available-1.1
metadata:
  audience: developers
  workflow: performance-testing
---

<!--
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-->

Locks single CPU core (freq=base, turbo=off, C1-only) → build + run via `./tools/toolchain/dbuild` → runs perf-simple-query N× in selected modes → optionally compares vs baseline → saves JSON + terminal table.

Requires: `sudo` for `cpupower`, Docker, `gh` CLI (baseline only). CWD = scylladb repo root.

## Pick the right benchmark

perf-simple-query exercises the CQL transport + coordinator + replica single-partition read/write path. It's the right tool for changes in `transport/` or anything that path touches. A change to collections, or anything not exercised by a simple single-partition query, is invisible to it — use a different benchmark (e.g. alternator for collections). A flat result from a benchmark that can't observe your change means nothing; it is not evidence of "no regression". Decide this before running Phase 1.

## Phase 1 — Params (`question` tool)

| Question | Options | Default |
|---|---|---|
| Build mode | `dev`, `release` | `release` |
| PGO | `yes`, `no` | `no` |
| Baseline branch | name, commit, or `no` | `$(git merge-base origin/master HEAD)` |
| Baseline first? | `yes` (1 shot), `no` | `no` |
| Modes | multi: read, write, delete, counters, collection | `read` |
| Iterations | int | `5` |
| Duration | seconds | `30` |
| CPU core | int or `auto` | `0` |

The merge base is the default baseline, not `master`'s tip: master keeps moving after you branch, so comparing against its tip measures your change plus every unrelated commit that landed since. The merge base isolates just your change.

## Phase 2 — Build

**Current-first (no baseline):** `./tools/toolchain/dbuild ./configure.py --mode <mode>` + `./tools/toolchain/dbuild ninja build/<mode>/scylla`

**Baseline-first (build both):**
```
./tools/toolchain/dbuild ./configure.py --mode <mode>
./tools/toolchain/dbuild ninja build/<mode>/scylla
MAIN_DIR=$PWD
git clone --shared . ../scylla-baseline
cd ../scylla-baseline
BASELINE=$(git -C "$MAIN_DIR" merge-base origin/master HEAD)
git checkout $BASELINE
git submodule init
for k in abseil seastar swagger-ui scylla-python3 tools/cqlsh; do
  url=$(git -C "$MAIN_DIR" config --get submodule.$k.url)
  git config submodule.$k.url "$url"
done
git submodule update --init --recursive
./tools/toolchain/dbuild ./configure.py --mode <mode>
./tools/toolchain/dbuild ninja build/<mode>/scylla
cd -
```

PGO: `--pgo-gen` → training → `--pgo-use`.

## Phase 3 — Lock → Run → Restore (current <branch>, N×)

**Save + lock (turbo, vendor-agnostic):**
```
mkdir -p ~/.cache/opencode/perf-bench/cpu-state
for f in scaling_governor scaling_min_freq scaling_max_freq; do
  cat /sys/devices/system/cpu/cpu<core>/cpufreq/$f > ~/.cache/opencode/perf-bench/cpu-state/$f
done

TURBO_KNOB=""
if [ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]; then
  TURBO_KNOB=/sys/devices/system/cpu/intel_pstate/no_turbo; TURBO_OFF=1   # 1 = turbo off
elif [ -f /sys/devices/system/cpu/cpufreq/boost ]; then
  TURBO_KNOB=/sys/devices/system/cpu/cpufreq/boost; TURBO_OFF=0          # 0 = boost off (inverted!)
else
  echo "FATAL: no turbo/boost control found (not intel_pstate or amd_pstate/acpi-cpufreq) — aborting, numbers would be unpinned" >&2
  exit 1
fi
cat "$TURBO_KNOB" > ~/.cache/opencode/perf-bench/cpu-state/turbo_knob_value
echo "$TURBO_KNOB" > ~/.cache/opencode/perf-bench/cpu-state/turbo_knob_path
cat /sys/devices/system/cpu/cpu<core>/cpuidle/state2/disable 2>/dev/null > ~/.cache/opencode/perf-bench/cpu-state/cstate 2>/dev/null
```

An unpinnable CPU must fail the run, not silently produce unpinned (worthless) numbers.

**Hyperthread sibling:** pin core N contaminates the measurement if its SMT sibling is running other work.
```
SIBLINGS=$(cat /sys/devices/system/cpu/cpu<core>/topology/thread_siblings_list)  # e.g. "0,4"
for s in ${SIBLINGS//,/ }; do
  [ "$s" = "<core>" ] && continue
  echo "$s" >> ~/.cache/opencode/perf-bench/cpu-state/offlined_siblings
  echo 0 | sudo tee /sys/devices/system/cpu/cpu$s/online
done
```
Restore them (`echo 1 > .../cpu$s/online`) alongside the other CPU state below. If the host has SMT disabled the list is a single entry and this is a no-op.

**Lock:** `BF=$(cat /sys/devices/system/cpu/cpu<core>/cpufreq/base_frequency)` — then:
```
sudo cpupower -c <core> frequency-set -g performance -d $BF -u $BF
echo $TURBO_OFF | sudo tee "$TURBO_KNOB"
sudo cpupower -c <core> idle-set -d 2
```

**Verify the lock held:**
```
CUR=$(cat /sys/devices/system/cpu/cpu<core>/cpufreq/scaling_cur_freq)
# cur_freq jitters by a few MHz even when locked - compare within 2%, not exactly
awk -v c="$CUR" -v b="$BF" 'BEGIN{exit !(c > b*0.98 && c < b*1.02)}' || {
  echo "FATAL: core <core> at ${CUR}kHz, expected ~${BF}kHz - abort, numbers would not be reliable" >&2; exit 1; }
```

**Run (× iterations):**
```
mkdir -p ~/.cache/opencode/perf-bench/results
./tools/toolchain/dbuild build/<mode>/scylla perf-simple-query --smp 1 --cpuset <core> \
  --duration <dur> --partitions 10000 --stop-on-error false <mode_flags> \
  --json-result ~/.cache/opencode/perf-bench/results/run-<mode>-<iter>.json \
  2>&1 | grep -E 'tps|median|mean|instructions'
```
Mode flags: `--write`, `--delete`, `--counters`, `--collection 10`.

**Restore:**
```
sudo cpupower -c <core> frequency-set \
  -g $(cat ~/.../cpu-state/scaling_governor) \
  -d $(cat ~/.../cpu-state/scaling_min_freq) \
  -u $(cat ~/.../cpu-state/scaling_max_freq)
cat ~/.../cpu-state/turbo_knob_value | sudo tee $(cat ~/.../cpu-state/turbo_knob_path)
sudo cpupower -c <core> idle-set -e $(cat ~/.../cpu-state/cstate)
for s in $(cat ~/.../cpu-state/offlined_siblings 2>/dev/null); do
  echo 1 | sudo tee /sys/devices/system/cpu/cpu$s/online
done
```
*(where `~/...` = `~/.cache/opencode/perf-bench`)*

**Sanity check (current-first):** if median tps == 0 → abort. This is a liveness check (the run failed to produce output at all), not a regression metric.

## Phase 4 — Build baseline (current-first, after sanity check)

Same as Phase 2 "Baseline-first" steps (clone + submodule init at `$(git merge-base origin/master HEAD)` + dbuild).

## Phase 5 — Lock → Run → Restore (baseline <branch>, 1×)

Same as Phase 3 but 1 iteration, output `run-baseline-<mode>.json`.

## Phase 6 — Parse & aggregate

`instructions_per_op` and `allocs_per_op` are the primary, gating metrics — these are what Argus actually gates on (see SCYLLADB-2794). tps is informational context only; it is known to be unstable run-to-run.

µ, σ, median, min, max per metric. Flag `|x - µ| > 2σ` as outliers.

From `--json-result` `stats` object (keys with spaces: `"median tps"`, `"mad tps"`, `"max tps"`, `"min tps"`; others: `allocs_per_op`, `logallocs_per_op`, `tasks_per_op`, `instructions_per_op`, `cpu_cycles_per_op`, `errors`).

## Phase 7 — Compare (if baseline)

`∆% = (current - baseline) / baseline × 100`. Gate on `instructions_per_op` (>5% increase) and `allocs_per_op` (>5% increase) regressions. Report tps delta as context only — a tps delta alone is NOT a regression signal, tps is not stable enough run-to-run to gate on. See SCYLLADB-2794 for the metrics Argus tracks.

## Phase 8 — Save & display

JSON to `~/.cache/opencode/perf-bench/results-<ts>.json` — commits, CPU pre/post, build mode, PGO, per-mode metrics/aggregates/outliers/baseline delta. Then `rm -rf ../scylla-baseline`.

Terminal table (insns/op and allocs/op first — they're the gating metrics; outlier flag keyed off insns/op):
```
mode   | run | insns/op  | allocs/op | tps      | outlier
read   | 1   | 1234567   | 12.3      | 450123.4 |
read   | 2   | 1228901   | 12.1      | 448901.2 |
read   | 3   | 1987654   | 15.8      | 321000.1 | YES (>2σ insns/op)
---    | avg | 1483713   | 13.4      | 406675   |
---    | ∆%  | +0.5%     | +0.8%     | -1.2%    | (vs baseline (merge-base))
```

## Constraints

1. Lock wraps measurement only — restore immediately after last run.
2. Results under `$HOME/.cache/opencode/perf-bench/`.
3. ASCII only, `<<'PYEOF'` for heredocs.
4. Baseline via `git clone --shared` (works with dbuild, preserves main repo cache).

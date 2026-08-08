---
name: perf-bench
description: perf-simple-query micro-benchmarks with CPU isolation, baseline comparison
license: MIT
compatibility: opencode
metadata:
  audience: developers
  workflow: performance-testing
---

Locks single CPU core (freq=base, turbo=off, C1-only) → build + run via `./tools/toolchain/dbuild` → runs perf-simple-query N× in selected modes → optionally compares vs baseline → saves JSON + terminal table.

Requires: `sudo` for `cpupower`, Docker, `gh` CLI (baseline only). CWD = scylladb repo root.

## Phase 1 — Params (`question` tool)

| Question | Options | Default |
|---|---|---|
| Build mode | `dev`, `release` | `release` |
| PGO | `yes`, `no` | `no` |
| Baseline branch | name or `no` | `master` |
| Baseline first? | `yes` (1 shot), `no` | `no` |
| Modes | multi: read, write, delete, counters, collection | `read` |
| Iterations | int | `5` |
| Duration | seconds | `30` |
| CPU core | int or `auto` | `0` |

## Phase 2 — Build

**Current-first (no baseline):** `./tools/toolchain/dbuild ./configure.py --mode <mode>` + `./tools/toolchain/dbuild ninja build/<mode>/scylla`

**Baseline-first (build both):**
```
./tools/toolchain/dbuild ./configure.py --mode <mode>
./tools/toolchain/dbuild ninja build/<mode>/scylla
MAIN_DIR=$PWD
git clone --shared . ../scylla-baseline
cd ../scylla-baseline
git checkout <branch>
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

**Save:**
```
mkdir -p ~/.cache/opencode/perf-bench/cpu-state
for f in scaling_governor scaling_min_freq scaling_max_freq; do
  cat /sys/devices/system/cpu/cpu<core>/cpufreq/$f > ~/.cache/opencode/perf-bench/cpu-state/$f
done
cat /sys/devices/system/cpu/intel_pstate/no_turbo > ~/.cache/opencode/perf-bench/cpu-state/no_turbo
cat /sys/devices/system/cpu/cpu<core>/cpuidle/state2/disable 2>/dev/null > ~/.cache/opencode/perf-bench/cpu-state/cstate 2>/dev/null
```

**Lock:** `BF=$(cat /sys/devices/system/cpu/cpu<core>/cpufreq/base_frequency)` — then:
```
sudo cpupower -c <core> frequency-set -g performance -d $BF -u $BF
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
sudo cpupower -c <core> idle-set -d 2
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
sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo < ~/.../cpu-state/no_turbo
sudo cpupower -c <core> idle-set -e $(cat ~/.../cpu-state/cstate)
  *(where `~/...` = `~/.cache/opencode/perf-bench`)*

**Sanity check (current-first):** if median tps == 0 → abort.

## Phase 4 — Build baseline (current-first, after sanity check)

Same as Phase 2 "Baseline-first" steps (clone + submodule init + dbuild).

## Phase 5 — Lock → Run → Restore (baseline <branch>, 1×)

Same as Phase 3 but 1 iteration, output `run-baseline-<mode>.json`.

## Phase 6 — Parse & aggregate

µ, σ, median, min, max per metric. Flag `|x - µ| > 2σ` as outliers.

From `--json-result` `stats` object (keys with spaces: `"median tps"`, `"mad tps"`, `"max tps"`, `"min tps"`; others: `allocs_per_op`, `logallocs_per_op`, `tasks_per_op`, `instructions_per_op`, `cpu_cycles_per_op`, `errors`).

## Phase 7 — Compare (if baseline)

`∆% = (current - baseline) / baseline × 100`. Flag: tps >3% drop, insns >5% increase.

## Phase 8 — Save & display

JSON to `~/.cache/opencode/perf-bench/results-<ts>.json` — commits, CPU pre/post, build mode, PGO, per-mode metrics/aggregates/outliers/baseline delta. Then `rm -rf ../scylla-baseline`.

Terminal table:
```
mode   | run | tps      | allocs/op | insns/op  | outlier
read   | 1   | 450123.4 | 12.3      | 1234567   |
read   | 2   | 448901.2 | 12.1      | 1228901   |
read   | 3   | 321000.1 | 15.8      | 1987654   | YES (>2σ tps)
---    | avg | 406675   | 13.4      | 1483713   |
---    | ∆%  | -1.2%    | +0.8%     | +0.5%     | (vs baseline (master))
```

## Constraints

1. Lock wraps measurement only — restore immediately after last run.
2. Results under `$HOME/.cache/opencode/perf-bench/`.
3. ASCII only, `<<'PYEOF'` for heredocs.
4. Baseline via `git clone --shared` (works with dbuild, preserves main repo cache).

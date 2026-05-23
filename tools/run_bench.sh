#!/bin/bash
set -euo pipefail

# W-TinyLFU Real Benchmark
# Config: tools/bench_config.yaml
# Usage: bash tools/run_bench.sh [path/to/config.yaml]

CONFIG="${1:-tools/bench_config.yaml}"

if [ ! -f "$CONFIG" ]; then
    echo "ERROR: Config file not found: $CONFIG"
    exit 1
fi

# --- Parse config (simple yaml key extraction) ---
yaml_val() { grep -E "^\s*${1}:" "$CONFIG" | head -1 | sed "s/.*${1}:\s*//" | sed 's/\s*#.*//' | tr -d "'\""; }

SCYLLA_BIN=$(yaml_val "binary")
MEMORY=$(yaml_val "memory")
SMP=$(yaml_val "smp")
API_PORT=$(yaml_val "api_port")
CQL_PORT=$(yaml_val "cql_port")
PROM_PORT=$(yaml_val "prometheus_port")
# Per-workload keyspaces (each workload has its own keyspace for metric isolation)
HOT_KEYSPACE=$(python3 -c "import yaml; cfg=yaml.safe_load(open('$CONFIG')); print(cfg['workloads']['hot']['keyspace'])" 2>/dev/null || echo "bench_hot")
SCAN_KEYSPACE=$(python3 -c "import yaml; cfg=yaml.safe_load(open('$CONFIG')); print(cfg['workloads']['scan']['keyspace'])" 2>/dev/null || echo "bench_scan")

# Use python for nested YAML values to avoid grep context issues
# Usage: py_val key1 key2 key3 ... (dot-separated path into YAML)
py_val() {
    python3 -c "
import yaml, sys
with open('$CONFIG') as f:
    cfg = yaml.safe_load(f)
keys = sys.argv[1:]
v = cfg
for k in keys:
    v = v[k]
print(v)
" "$@"
}

WARMUP_DURATION=$(py_val phases warmup duration)
WARMUP_RATE=$(py_val phases warmup rate)
ATTACK_DURATION=$(py_val phases attack duration)
# Support separate hot/scan rates, fall back to single rate
ATTACK_HOT_RATE=$(python3 -c "
import yaml
with open('$CONFIG') as f:
    cfg = yaml.safe_load(f)
a = cfg['phases']['attack']
print(a.get('hot_rate', a.get('rate', 15000)))
")
ATTACK_SCAN_RATE=$(python3 -c "
import yaml
with open('$CONFIG') as f:
    cfg = yaml.safe_load(f)
a = cfg['phases']['attack']
print(a.get('scan_rate', a.get('rate', 15000)))
")

HOT_SCRIPT=$(py_val workloads hot script)
SCAN_SCRIPT=$(py_val workloads scan script)

RESULTS_DIR=$(yaml_val "results_dir")

GRAFANA_URL=$(yaml_val "url" | head -1)            # first url: match = grafana
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
DASHBOARD_UID=$(yaml_val "dashboard_uid")
DASHBOARD_UID="${DASHBOARD_UID:-scylla-bench-wtinylfu}"
SNAP_SETTLE=$(yaml_val "settle_seconds")
SNAP_SETTLE="${SNAP_SETTLE:-15}"
SNAP_WIDTH=$(yaml_val "width")
SNAP_WIDTH="${SNAP_WIDTH:-1000}"
SNAP_HEIGHT=$(yaml_val "height")
SNAP_HEIGHT="${SNAP_HEIGHT:-500}"
DOCKER_COMPOSE=$(yaml_val "docker_compose")
DOCKER_COMPOSE="${DOCKER_COMPOSE:-.bench/monitoring/docker-compose.yml}"

API="http://localhost:$API_PORT"
PROM="http://localhost:$PROM_PORT/metrics"

mkdir -p "$RESULTS_DIR"

# --- Parse snapshot panel list from config ---
# Produces lines like: "2:cache_hit_rate:Cache Hit Rate"
SNAP_PANELS=()
while IFS= read -r line; do
    SNAP_PANELS+=("$line")
done < <(python3 -c "
import yaml, sys
with open('$CONFIG') as f:
    cfg = yaml.safe_load(f)
for p in cfg.get('snapshots', {}).get('panels', []):
    print(f\"{p['panel_id']}:{p['name']}:{p.get('title', p['name'])}\")
" 2>/dev/null || true)

# --- Extract configs array ---
CONFIGS=()
while IFS= read -r line; do
    [ -n "$line" ] && CONFIGS+=("$line")
done < <(python3 -c "
import yaml
with open('$CONFIG') as f:
    cfg = yaml.safe_load(f)
for c in cfg.get('configs', []):
    hill = str(c.get('hill_climbing', False)).lower()
    frac = c.get('window_fraction', c.get('window_percent', 1) / 100.0)
    print(f\"{c['label']}:{frac}:{hill}\")
" 2>/dev/null)

if [ ${#CONFIGS[@]} -eq 0 ]; then
    echo "WARNING: No configs parsed from $CONFIG, using defaults"
    CONFIGS=("w1:0.01:false" "w50:0.50:false" "w99:0.99:false")
fi

log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "Config: $CONFIG"
log "ScyllaDB: $SCYLLA_BIN, memory=$MEMORY, smp=$SMP"
log "Hot: $HOT_SCRIPT, Scan: $SCAN_SCRIPT"
log "Warmup: ${WARMUP_DURATION} @ ${WARMUP_RATE}/s, Attack: ${ATTACK_DURATION} hot@${ATTACK_HOT_RATE}/s scan@${ATTACK_SCAN_RATE}/s"
log "Configs: ${CONFIGS[*]}"
log "Grafana snapshots: ${#SNAP_PANELS[@]} panels, settle=${SNAP_SETTLE}s"
log ""

# =========================================================================
# Monitoring stack
# =========================================================================

start_monitoring() {
    if [ ! -f "$DOCKER_COMPOSE" ]; then
        log "WARNING: docker-compose file not found at $DOCKER_COMPOSE — skipping monitoring"
        return 1
    fi
    log "Starting monitoring stack (Prometheus + Grafana + Renderer)..."
    docker compose -f "$DOCKER_COMPOSE" up -d 2>&1 | tail -3

    log "Waiting for Grafana..."
    for i in $(seq 1 30); do
        if curl -s "$GRAFANA_URL/api/health" 2>/dev/null | grep -q "ok"; then
            log "Grafana ready after ${i}s"
            return 0
        fi
        sleep 2
    done
    log "WARNING: Grafana did not become ready — snapshots will be skipped"
    return 1
}

stop_monitoring() {
    if [ -f "$DOCKER_COMPOSE" ]; then
        log "Stopping monitoring stack..."
        docker compose -f "$DOCKER_COMPOSE" down 2>/dev/null || true
    fi
}

MONITORING_UP=false

# Capture a Grafana panel as PNG.
# Usage: grafana_snapshot <output_path> <panel_id> <from_epoch_ms> <to_epoch_ms>
grafana_snapshot() {
    local out="$1" panel_id="$2" from_ms="$3" to_ms="$4"

    if [ "$MONITORING_UP" != "true" ]; then return; fi

    local render_url="${GRAFANA_URL}/render/d-solo/${DASHBOARD_UID}"
    render_url+="?orgId=1&panelId=${panel_id}"
    render_url+="&from=${from_ms}&to=${to_ms}"
    render_url+="&width=${SNAP_WIDTH}&height=${SNAP_HEIGHT}"

    curl -s -o "$out" \
        -H "Authorization: Basic $(echo -n 'admin:admin' | base64)" \
        "$render_url" 2>/dev/null

    # Verify we got an image, not an error page
    if file "$out" 2>/dev/null | grep -q "PNG"; then
        log "  snapshot: $(basename $out) ($(du -h "$out" | cut -f1))"
    else
        log "  snapshot FAILED: $(basename $out)"
        rm -f "$out"
    fi
}

# Capture all configured panels for a given phase.
# Usage: capture_phase_snapshots <label> <phase_name> <from_epoch_ms> <to_epoch_ms>
capture_phase_snapshots() {
    local label="$1" phase="$2" from_ms="$3" to_ms="$4"
    local snap_dir="$RESULTS_DIR/$label/snapshots"
    mkdir -p "$snap_dir"

    if [ "$MONITORING_UP" != "true" ]; then return; fi
    if [ ${#SNAP_PANELS[@]} -eq 0 ]; then return; fi

    log "Capturing Grafana snapshots for $label/$phase (waiting ${SNAP_SETTLE}s for metrics settle)..."
    sleep "$SNAP_SETTLE"

    for entry in "${SNAP_PANELS[@]}"; do
        IFS=: read -r panel_id panel_name _title <<< "$entry"
        grafana_snapshot "$snap_dir/${panel_name}_${phase}.png" "$panel_id" "$from_ms" "$to_ms"
    done
}

# =========================================================================
# ScyllaDB lifecycle
# =========================================================================

stop_scylla() {
    log "Stopping ScyllaDB..."
    docker kill scylla-bench 2>/dev/null || true
    docker rm -f scylla-bench 2>/dev/null || true
    pkill -f "scylla.*--developer-mode" 2>/dev/null || true
    sleep 3
}

start_scylla() {
    local window_pct="$1"
    local label="$3"
    local datadir=".bench/run-$label"

    # Data dirs may be owned by root (created inside Docker), so use sudo to clean
    sudo rm -rf "$datadir" 2>/dev/null || rm -rf "$datadir" 2>/dev/null || true
    mkdir -p "$datadir/data" "$datadir/commitlog" "$datadir/hints" "$datadir/view-hints"


    local abs_datadir
    abs_datadir="$(realpath "$datadir")"
    local abs_toplevel
    abs_toplevel="$(realpath .)"
    docker run --rm --name scylla-bench \
        --network host \
        -v "$abs_toplevel:$abs_toplevel" \
        -v "$abs_datadir:$abs_datadir" \
        -w "$abs_toplevel" \
        docker.io/scylladb/scylla-toolchain:fedora-43-20260304 \
        "$SCYLLA_BIN" \
        --workdir "$abs_datadir/data" \
        --commitlog-directory "$abs_datadir/commitlog" \
        --hints-directory "$abs_datadir/hints" \
        --view-hints-directory "$abs_datadir/view-hints" \
        --memory $MEMORY --smp $SMP \
        --developer-mode true --overprovisioned \
        --default-log-level warn \
        --api-address 0.0.0.0 --api-port $API_PORT \
        --tinylfu-initial-window-fraction "$window_pct" \
        &> "$datadir/scylla.log" &

    log "Waiting for API..."
    for i in $(seq 1 120); do
        if curl -s "$API/cache_service/tinylfu_initial_window_fraction" 2>/dev/null | grep -q "[0-9]"; then
            log "API ready after ${i}s"; break
        fi
        if [ $i -eq 120 ]; then log "ERROR: API timeout"; exit 1; fi
        sleep 2
    done

    log "Waiting for CQL..."
    for i in $(seq 1 120); do
        if ~/.local/bin/cqlsh localhost $CQL_PORT -e "SELECT now() FROM system.local" &>/dev/null; then
            log "CQL ready after ${i}s"; break
        fi
        if [ $i -eq 120 ]; then log "ERROR: CQL timeout"; exit 1; fi
        sleep 2
    done

    log "Window: $(curl -s $API/cache_service/tinylfu_initial_window_fraction)"
}

load_data() {
    log "Creating schema and loading data..."
    ~/.local/bin/cqlsh localhost $CQL_PORT -e "CREATE KEYSPACE IF NOT EXISTS $HOT_KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND tablets = {'enabled': false};" 2>/dev/null
    ~/.local/bin/cqlsh localhost $CQL_PORT -e "CREATE KEYSPACE IF NOT EXISTS $SCAN_KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND tablets = {'enabled': false};" 2>/dev/null
    latte schema "$HOT_SCRIPT" 2>&1 | tail -1
    latte schema "$SCAN_SCRIPT" 2>&1 | tail -1
    latte load "$HOT_SCRIPT" --request-timeout 30s 2>&1 | tail -1
    latte load "$SCAN_SCRIPT" --request-timeout 30s 2>&1 | tail -1
    log "Data loaded."
}

get_stats() {
    local metrics
    metrics=$(curl -s "$PROM" 2>/dev/null)

    # Global metrics (sum across shards for multi-shard safety)
    local hits misses cache_bytes cache_partitions
    hits=$(echo "$metrics" | grep "^scylla_cache_row_hits" | awk '{s+=$2} END{printf "%d", s}')
    misses=$(echo "$metrics" | grep "^scylla_cache_row_misses" | awk '{s+=$2} END{printf "%d", s}')
    cache_bytes=$(echo "$metrics" | grep "^scylla_cache_bytes_used" | awk '{s+=$2} END{printf "%d", s}')
    cache_partitions=$(echo "$metrics" | grep "^scylla_cache_partitions{" | awk '{s+=$2} END{printf "%d", s}')

    # Per-keyspace metrics (from per-table cache counters)
    local hot_hits hot_misses scan_hits scan_misses
    hot_hits=$(echo "$metrics" | grep "scylla_column_family_cache_row_hits.*ks=\"$HOT_KEYSPACE\"" | awk '{s+=$2} END{printf "%d", s}')
    hot_misses=$(echo "$metrics" | grep "scylla_column_family_cache_row_misses.*ks=\"$HOT_KEYSPACE\"" | awk '{s+=$2} END{printf "%d", s}')
    scan_hits=$(echo "$metrics" | grep "scylla_column_family_cache_row_hits.*ks=\"$SCAN_KEYSPACE\"" | awk '{s+=$2} END{printf "%d", s}')
    scan_misses=$(echo "$metrics" | grep "scylla_column_family_cache_row_misses.*ks=\"$SCAN_KEYSPACE\"" | awk '{s+=$2} END{printf "%d", s}')

    echo "$hits $misses $cache_bytes $cache_partitions $hot_hits $hot_misses $scan_hits $scan_misses"
}

epoch_ms() { python3 -c "import time; print(int(time.time() * 1000))"; }

# =========================================================================
# Test runner
# =========================================================================

run_test() {
    local label="$1"
    local outdir="$RESULTS_DIR/$label"
    mkdir -p "$outdir"

    log "=== Benchmark: $label ==="

    # --- Warmup phase ---
    local pre_hits pre_misses pre_bytes pre_parts pre_hot_hits pre_hot_misses _pre_scan_hits _pre_scan_misses
    read pre_hits pre_misses pre_bytes pre_parts pre_hot_hits pre_hot_misses _pre_scan_hits _pre_scan_misses <<< "$(get_stats)"
    log "Pre-warmup: hits=$pre_hits misses=$pre_misses cache=${pre_bytes}B parts=$pre_parts"

    local warmup_start_ms
    warmup_start_ms=$(epoch_ms)

    log "Warmup: hot reads for ${WARMUP_DURATION} at ${WARMUP_RATE}/s..."
    latte run "$HOT_SCRIPT" -r $WARMUP_RATE -d "$WARMUP_DURATION" \
        --output "$outdir/warmup.json" 2>&1 | tail -3

    local warmup_end_ms
    warmup_end_ms=$(epoch_ms)

    local pw_hits pw_misses pw_bytes pw_parts pw_hot_hits pw_hot_misses _pw_scan_hits _pw_scan_misses
    read pw_hits pw_misses pw_bytes pw_parts pw_hot_hits pw_hot_misses _pw_scan_hits _pw_scan_misses <<< "$(get_stats)"
    local warmup_hits=$((pw_hits - pre_hits))
    local warmup_misses=$((pw_misses - pre_misses))
    local warmup_total=$((warmup_hits + warmup_misses))
    local warmup_hot_hits=$((pw_hot_hits - pre_hot_hits))
    local warmup_hot_misses=$((pw_hot_misses - pre_hot_misses))
    local warmup_hot_total=$((warmup_hot_hits + warmup_hot_misses))
    log "Post-warmup: hits=+$warmup_hits misses=+$warmup_misses (HR=$(python3 -c "print(f'{$warmup_hits/max($warmup_total,1)*100:.1f}%')" 2>/dev/null)) cache=${pw_bytes}B parts=$pw_parts"
    log "  hot-only HR: $(python3 -c "print(f'{$warmup_hot_hits/max($warmup_hot_total,1)*100:.1f}%')" 2>/dev/null)"

    # Capture warmup snapshots
    capture_phase_snapshots "$label" "warmup" "$warmup_start_ms" "$warmup_end_ms"

    # --- Attack phase ---
    log "Attack: hot@${ATTACK_HOT_RATE}/s + scan@${ATTACK_SCAN_RATE}/s for ${ATTACK_DURATION}..."

    local pa_hits pa_misses pa_hot_hits pa_hot_misses pa_scan_hits pa_scan_misses
    read pa_hits pa_misses _ _ pa_hot_hits pa_hot_misses pa_scan_hits pa_scan_misses <<< "$(get_stats)"
    local attack_start_ms
    attack_start_ms=$(epoch_ms)

    latte run "$HOT_SCRIPT" -r $ATTACK_HOT_RATE -d "$ATTACK_DURATION" \
        --output "$outdir/hot_attack.json" 2>&1 | tee "$outdir/hot_summary.txt" &
    local HOT_PID=$!

    latte run "$SCAN_SCRIPT" -r $ATTACK_SCAN_RATE -d "$ATTACK_DURATION" \
        --output "$outdir/scan_attack.json" 2>&1 | tee "$outdir/scan_summary.txt" &
    local SCAN_PID=$!

    wait $HOT_PID || true
    wait $SCAN_PID || true

    local attack_end_ms
    attack_end_ms=$(epoch_ms)

    local final_hits final_misses final_bytes final_parts final_hot_hits final_hot_misses final_scan_hits final_scan_misses
    read final_hits final_misses final_bytes final_parts final_hot_hits final_hot_misses final_scan_hits final_scan_misses <<< "$(get_stats)"
    local attack_hits=$((final_hits - pa_hits))
    local attack_misses=$((final_misses - pa_misses))
    local attack_total=$((attack_hits + attack_misses))
    local attack_hot_hits=$((final_hot_hits - pa_hot_hits))
    local attack_hot_misses=$((final_hot_misses - pa_hot_misses))
    local attack_hot_total=$((attack_hot_hits + attack_hot_misses))
    local attack_scan_hits=$((final_scan_hits - pa_scan_hits))
    local attack_scan_misses=$((final_scan_misses - pa_scan_misses))
    local attack_scan_total=$((attack_scan_hits + attack_scan_misses))
    log "Post-attack: hits=+$attack_hits misses=+$attack_misses (combined HR=$(python3 -c "print(f'{$attack_hits/max($attack_total,1)*100:.1f}%')" 2>/dev/null)) cache=${final_bytes}B parts=$final_parts"
    log "  hot HR: $(python3 -c "print(f'{$attack_hot_hits/max($attack_hot_total,1)*100:.1f}%')" 2>/dev/null)  scan HR: $(python3 -c "print(f'{$attack_scan_hits/max($attack_scan_total,1)*100:.1f}%')" 2>/dev/null)"

    # Capture attack snapshots
    capture_phase_snapshots "$label" "attack" "$attack_start_ms" "$attack_end_ms"

    # --- Save stats ---
    cat > "$outdir/stats.txt" << EOF
warmup_hits=$warmup_hits
warmup_misses=$warmup_misses
warmup_hr=$(python3 -c "print(f'{$warmup_hits/max($warmup_total,1)*100:.1f}')" 2>/dev/null)
warmup_hot_hits=$warmup_hot_hits
warmup_hot_misses=$warmup_hot_misses
warmup_hot_hr=$(python3 -c "print(f'{$warmup_hot_hits/max($warmup_hot_total,1)*100:.1f}')" 2>/dev/null)
attack_hits=$attack_hits
attack_misses=$attack_misses
attack_hr=$(python3 -c "print(f'{$attack_hits/max($attack_total,1)*100:.1f}')" 2>/dev/null)
attack_hot_hits=$attack_hot_hits
attack_hot_misses=$attack_hot_misses
attack_hot_hr=$(python3 -c "print(f'{$attack_hot_hits/max($attack_hot_total,1)*100:.1f}')" 2>/dev/null)
attack_scan_hits=$attack_scan_hits
attack_scan_misses=$attack_scan_misses
attack_scan_hr=$(python3 -c "print(f'{$attack_scan_hits/max($attack_scan_total,1)*100:.1f}')" 2>/dev/null)
cache_bytes=$final_bytes
cache_partitions=$final_parts
warmup_start_ms=$warmup_start_ms
warmup_end_ms=$warmup_end_ms
attack_start_ms=$attack_start_ms
attack_end_ms=$attack_end_ms
EOF

    # Extract latency percentiles from hot attack (from CYCLE LATENCY section)
    local in_latency=false
    while IFS= read -r line; do
        if echo "$line" | grep -q "CYCLE LATENCY for run"; then in_latency=true; continue; fi
        if [ "$in_latency" = true ]; then
            for pct_label in "50:p50" "90:p90" "99 :p99" "99.9:p99_9" "Max:pMax"; do
                IFS=: read -r pattern key <<< "$pct_label"
                if echo "$line" | grep -qE "^\s+${pattern}\s"; then
                    local val
                    val=$(echo "$line" | awk '{print $2}')
                    echo "hot_${key}=$val" >> "$outdir/stats.txt"
                fi
            done
        fi
    done < "$outdir/hot_summary.txt"

    log "Test $label complete."
    echo ""
}

# =========================================================================
# Main
# =========================================================================

log "W-TinyLFU Real Benchmark (with restart per config)"
log "==================================================="

# Start monitoring stack (non-fatal if it fails)
if start_monitoring; then
    MONITORING_UP=true
    log "Monitoring stack is up — Grafana snapshots enabled"
else
    log "Monitoring stack not available — running without snapshots"
fi

for config in "${CONFIGS[@]}"; do
    IFS=: read -r label window hill <<< "$config"
    stop_scylla
    start_scylla "$window" "$hill" "$label"
    load_data
    run_test "$label"
done

stop_scylla

log "All tests complete!"
log ""

# =========================================================================
# Summary
# =========================================================================

log "=== Summary ==="
printf "%-8s %10s %10s %10s %10s %10s %10s %10s\n" "Config" "Warmup HR" "Hot HR" "Scan HR" "Hot p50" "Hot p99" "Hot Max" "Parts"
printf "%-8s %10s %10s %10s %10s %10s %10s %10s\n" "--------" "---------" "------" "-------" "-------" "-------" "-------" "-----"
for config in "${CONFIGS[@]}"; do
    IFS=: read -r label _ _ <<< "$config"
    if [ -f "$RESULTS_DIR/$label/stats.txt" ]; then
        # shellcheck source=/dev/null  # stats.txt is generated by this script
        source "$RESULTS_DIR/$label/stats.txt"
        printf "%-8s %9s%% %9s%% %9s%% %8sms %8sms %8sms %10s\n" \
            "$label" "$warmup_hr" "${attack_hot_hr:-?}" "${attack_scan_hr:-?}" \
            "${hot_p50:-?}" "${hot_p99:-?}" "${hot_pMax:-?}" "$cache_partitions"
    fi
done

# Count snapshots
snap_count=$(find "$RESULTS_DIR" -name "*.png" 2>/dev/null | wc -l)
if [ "$snap_count" -gt 0 ]; then
    log ""
    log "Grafana snapshots: $snap_count images captured"
    find "$RESULTS_DIR" -name "*.png" -printf "  %p (%s bytes)\n" 2>/dev/null | sort
fi

log ""
log "Results in: $RESULTS_DIR"
log "Generate report: python3 tools/gen_bench_report.py"

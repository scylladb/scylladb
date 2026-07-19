#!/bin/bash
set -euo pipefail

# W-TinyLFU vs LRU Benchmark
# Compares 1% window (scan-resistant W-TinyLFU) vs 99% window (pure LRU)
# using hot+scan workload via latte.

SCYLLA_BIN="./build/release/scylla"
BASE_DIR=".bench/wtinylfu-bench"
SCYLLA_PORT=9042
API_PORT=10000
SCYLLA_MEM="512M"
SCYLLA_SMP=1

HOT_ROWS=80000
SCAN_ROWS=500000
PAYLOAD_SIZE=1024

WARMUP_DURATION=60
ATTACK_DURATION=90
RATE=5000

RESULTS_DIR="$BASE_DIR/results"
mkdir -p "$RESULTS_DIR"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

wait_for_cql() {
    log "Waiting for CQL on port $SCYLLA_PORT..."
    for i in $(seq 1 120); do
        if cqlsh localhost $SCYLLA_PORT -e "SELECT now() FROM system.local" &>/dev/null; then
            log "CQL is ready."
            return 0
        fi
        sleep 1
    done
    log "ERROR: CQL did not become ready in 120s"
    return 1
}

start_scylla() {
    local conf_dir="$1"
    local window_frac="$2"
    local label="$3"

    log "Starting ScyllaDB [$label] (window_fraction=$window_frac)..."

    rm -rf "$conf_dir/commitlog" "$conf_dir/data" "$conf_dir/hints" "$conf_dir/view-hints"
    mkdir -p "$conf_dir/commitlog" "$conf_dir/data" "$conf_dir/hints" "$conf_dir/view-hints"

    cat > "$conf_dir/scylla.yaml" << YAML
cluster_name: 'WTinyLFU Bench $label'
num_tokens: 256
commitlog_directory: $(realpath $conf_dir)/commitlog
data_file_directories:
  - $(realpath $conf_dir)/data
hints_directory: $(realpath $conf_dir)/hints
view_hints_directory: $(realpath $conf_dir)/view-hints
tinylfu_initial_window_fraction: $window_frac
YAML

    DBUILD_TOOL=docker ./tools/toolchain/dbuild --image scylladb/scylla-toolchain:fedora-43-20260304 -- \
        "$SCYLLA_BIN" \
        --options-file "$(realpath $conf_dir)/scylla.yaml" \
        --smp $SCYLLA_SMP \
        --memory $SCYLLA_MEM \
        --developer-mode 1 \
        --overprovisioned \
        --max-io-requests 128 \
        --api-address 0.0.0.0 \
        --api-port $API_PORT \
        &> "$conf_dir/scylla.log" &

    SCYLLA_PID=$!
    log "ScyllaDB PID: $SCYLLA_PID"
    wait_for_cql
}

stop_scylla() {
    log "Stopping ScyllaDB..."
    if [ -n "${SCYLLA_PID:-}" ]; then
        kill $SCYLLA_PID 2>/dev/null || true
        wait $SCYLLA_PID 2>/dev/null || true
    fi
    pkill -f "scylla.*--developer-mode" 2>/dev/null || true
    sleep 3
}

setup_schema() {
    log "Creating schema..."
    cqlsh localhost $SCYLLA_PORT -e "
        CREATE KEYSPACE IF NOT EXISTS latte
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        CREATE TABLE IF NOT EXISTS latte.hot_test (
            pk bigint PRIMARY KEY,
            payload blob
        );
        CREATE TABLE IF NOT EXISTS latte.scan_test (
            pk bigint PRIMARY KEY,
            payload blob
        );
    "
}

create_latte_workloads() {
    mkdir -p tools
    cat > tools/latte_hot.rn << 'RUNE'
pub async fn schema(db) {
    db.execute("CREATE TABLE IF NOT EXISTS latte.hot_test (pk bigint PRIMARY KEY, payload blob)").await?;
}
pub async fn erase(db) {
    db.execute("TRUNCATE TABLE latte.hot_test").await?;
}
pub async fn prepare(db) {
    db.prepare("INSERT INTO latte.hot_test (pk, payload) VALUES (:pk, :payload)").await?;
    db.prepare("SELECT * FROM latte.hot_test WHERE pk = :pk").await?;
}
pub async fn load(db, i) {
    let payload = blob::new(1024);
    db.execute_prepared(0, [i, payload]).await?;
}
pub async fn run(db, i) {
    let pk = hash2(i, 0) % ROW_COUNT;
    db.execute_prepared(1, [pk]).await?;
}
RUNE

    cat > tools/latte_scan.rn << 'RUNE'
pub async fn schema(db) {
    db.execute("CREATE TABLE IF NOT EXISTS latte.scan_test (pk bigint PRIMARY KEY, payload blob)").await?;
}
pub async fn erase(db) {
    db.execute("TRUNCATE TABLE latte.scan_test").await?;
}
pub async fn prepare(db) {
    db.prepare("INSERT INTO latte.scan_test (pk, payload) VALUES (:pk, :payload)").await?;
    db.prepare("SELECT * FROM latte.scan_test WHERE pk = :pk").await?;
}
pub async fn load(db, i) {
    let payload = blob::new(1024);
    db.execute_prepared(0, [i, payload]).await?;
}
pub async fn run(db, i) {
    let pk = i % ROW_COUNT;
    db.execute_prepared(1, [pk]).await?;
}
RUNE
}

get_cache_stats() {
    local label="$1"
    local hits=$(curl -s "http://localhost:$API_PORT/cache_service/metrics/partition_hits" 2>/dev/null || echo "?")
    local misses=$(curl -s "http://localhost:$API_PORT/cache_service/metrics/partition_misses" 2>/dev/null || echo "?")
    echo "  $label: hits=$hits misses=$misses"
}

run_benchmark() {
    local label="$1"
    local outdir="$RESULTS_DIR/$label"
    mkdir -p "$outdir"

    log "=== Benchmark: $label ==="

    # Warmup: hot reads only
    log "Warmup: hot reads for ${WARMUP_DURATION}s at ${RATE}/s..."
    latte run "tools/latte_hot.rn" -d $SCYLLA_PORT \
        -n $HOT_ROWS -r $RATE --duration "${WARMUP_DURATION}s" \
        2>&1 | tail -3

    get_cache_stats "post-warmup" | tee "$outdir/stats.txt"

    # Attack: hot + scan in parallel
    log "Attack: hot + scan for ${ATTACK_DURATION}s at ${RATE}/s each..."

    latte run "tools/latte_hot.rn" -d $SCYLLA_PORT \
        -n $HOT_ROWS -r $RATE --duration "${ATTACK_DURATION}s" \
        2>&1 | tee "$outdir/hot_result.txt" &
    HOT_PID=$!

    latte run "tools/latte_scan.rn" -d $SCYLLA_PORT \
        -n $SCAN_ROWS -r $RATE --duration "${ATTACK_DURATION}s" \
        2>&1 | tee "$outdir/scan_result.txt" &
    SCAN_PID=$!

    wait $HOT_PID || true
    wait $SCAN_PID || true

    get_cache_stats "post-attack" | tee -a "$outdir/stats.txt"

    log "Benchmark $label complete."
    echo ""
    echo "=== HOT reads ($label) ==="
    cat "$outdir/hot_result.txt" | tail -10
    echo ""
    echo "=== SCAN reads ($label) ==="
    cat "$outdir/scan_result.txt" | tail -10
    echo ""
}

# =========================================================================
# Main
# =========================================================================

log "W-TinyLFU vs LRU Benchmark"
log "==========================="
log "Hot rows: $HOT_ROWS, Scan rows: $SCAN_ROWS"
log "Cache: $SCYLLA_MEM, Rate: $RATE/s per workload"
log ""

create_latte_workloads

# Configuration: label, window_fraction
configs=(
    "wtinylfu-1pct:0.01"
    "lru-99pct:0.99"
)

for config in "${configs[@]}"; do
    IFS=: read -r label window_frac <<< "$config"
    conf_dir="$BASE_DIR/conf-$label"
    mkdir -p "$conf_dir"

    stop_scylla
    start_scylla "$conf_dir" "$window_frac" "$label"
    setup_schema

    log "Loading hot data ($HOT_ROWS rows)..."
    latte load "tools/latte_hot.rn" -d $SCYLLA_PORT -n $HOT_ROWS 2>&1 | tail -1

    log "Loading scan data ($SCAN_ROWS rows)..."
    latte load "tools/latte_scan.rn" -d $SCYLLA_PORT -n $SCAN_ROWS 2>&1 | tail -1

    # Flush to force data to SSTables
    log "Flushing memtables..."
    curl -s -X POST "http://localhost:$API_PORT/storage_service/keyspace_flush/latte" > /dev/null 2>&1 || true

    run_benchmark "$label"
    stop_scylla
done

log ""
log "==========================================="
log "All benchmarks complete!"
log "Results in: $RESULTS_DIR"
log "==========================================="

echo ""
echo "=== COMPARISON ==="
echo ""
echo "--- W-TinyLFU (1% window) HOT reads ---"
cat "$RESULTS_DIR/wtinylfu-1pct/hot_result.txt" 2>/dev/null | tail -5
echo ""
echo "--- LRU (99% window) HOT reads ---"
cat "$RESULTS_DIR/lru-99pct/hot_result.txt" 2>/dev/null | tail -5
echo ""
echo "W-TinyLFU should show lower p99 latency for HOT reads"
echo "because the scan workload doesn't evict hot data."

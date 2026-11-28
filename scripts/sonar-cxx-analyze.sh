#!/usr/bin/env bash
#
# Copyright (C) 2024-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Whole-repo C++ static analysis for the SonarQube POC: runs clang-tidy and
# cppcheck over the compile database and emits reports that sonar-cxx imports
# via sonar.cxx.clangtidy.reportPaths / sonar.cxx.cppcheck.reportPaths.
#
# Key constraints handled:
#  * Memory: meant to run inside a memory-capped systemd user unit; clang-tidy
#    is parallel and heavy, so concurrency is the OOM lever.
#  * Resumable: clang-tidy is chunked per top-level source dir with a done-marker
#    so a crash/restart skips completed dirs.
#  * Path portability: sonar-cxx resolves report paths against the scanner's
#    base dir (/usr/src in the container). compile_commands uses absolute host
#    paths, so we strip the repo-root prefix to make report paths repo-relative
#    (otherwise issues don't map -> 0 issues, same trap as the coverage report).
#
# Usage (inside a capped unit):
#   systemd-run --user --unit=sonar-cxx -p MemoryMax=26G -p MemorySwapMax=4G \
#     -p WorkingDirectory="$PWD" -E JOBS=12 \
#     bash -c 'exec scripts/sonar-cxx-analyze.sh >> testlog/coverage/sonar/cxx-analyze.log 2>&1'

set -euo pipefail
shopt -s nullglob

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BUILD="${BUILD:-build/coverage}"                 # compile DB + generated headers (build output, stays in build/)
DB="$BUILD/compile_commands.json"
OUT_DIR="${OUT_DIR:-testlog/coverage/sonar}"      # generated reports: a test-run artifact, not build output
WORK="$OUT_DIR/cxx"                               # per-dir clang-tidy logs + markers
JOBS="${JOBS:-8}"                                # clang-tidy / cppcheck parallelism
PHASE="${PHASE:-all}"                            # all | clangtidy | cppcheck
EXCLUDE_TOP_CSV="${EXCLUDE_TOP_CSV:-abseil,seastar,build,test,testlog,kmip,kmipc,swagger-ui,docs}"

CLANG_TIDY_LOG="$OUT_DIR/clang-tidy.txt"
CPPCHECK_XML="$OUT_DIR/cppcheck.xml"

# Curated broad check set aligned with the CXX quality profile: focus on
# bug-prone / analyzer / performance findings and avoid the very noisy INFO-only
# groups (`misc-*`, broad `cppcoreguidelines-*`, `modernize-*`).
CHECKS='clang-diagnostic-*,clang-analyzer-*,bugprone-*,performance-*,-bugprone-easily-swappable-parameters'
# Report diagnostics only for files under the repo (skip /usr system headers).
HEADER_FILTER="^${REPO_ROOT}/"

mkdir -p "$WORK"

log() { printf '%s [sonar-cxx] %s\n' "$(date '+%H:%M:%S')" "$*"; }
run() { nice -n 10 ionice -c2 -n4 "$@"; }

[[ -f "$DB" ]] || { log "FATAL: compile DB $DB not found"; exit 1; }

if [[ -z "${RUN_CLANG_TIDY:-}" ]]; then
    for c in run-clang-tidy run-clang-tidy-19 run-clang-tidy-18 run-clang-tidy-17 run-clang-tidy-16; do
        if command -v "$c" >/dev/null 2>&1; then
            RUN_CLANG_TIDY="$c"
            break
        fi
    done
fi
[[ -n "${RUN_CLANG_TIDY:-}" ]] || { log "FATAL: run-clang-tidy not found"; exit 1; }

# Top-level source directories present in the compile DB (chunks for resumability).
mapfile -t DIRS < <(python3 -c "
import json,sys
exclude={x for x in '$EXCLUDE_TOP_CSV'.split(',') if x}
seen=set()
for e in json.load(open('$DB')):
    f=e['file']
    # normalize to repo-relative
    if f.startswith('$REPO_ROOT/'): f=f[len('$REPO_ROOT/'):]
    top=f.split('/',1)[0] if '/' in f else '.'
    if top in exclude:
        continue
    seen.add(top)
for d in sorted(seen): print(d)
")

run_clangtidy() {
    log "clang-tidy: ${#DIRS[@]} source dirs, jobs=$JOBS, checks=$CHECKS"
    for d in "${DIRS[@]}"; do
        local marker="$WORK/.done.$d" out="$WORK/clang-tidy.$d.txt"
        if [[ -f "$marker" ]]; then
            log "clang-tidy: $d already done, skipping"; continue
        fi
        log "clang-tidy: analyzing '$d/' ..."
        # run-clang-tidy filters DB entries by regex on the file path.
        run "$RUN_CLANG_TIDY" -p "$BUILD" -j "$JOBS" \
            -header-filter="$HEADER_FILTER" -checks="$CHECKS" \
            "/${d}/" > "$out" 2> "$WORK/clang-tidy.$d.err" || true
        touch "$marker"
        log "clang-tidy: '$d/' done ($(wc -l < "$out") log lines)"
    done
    # Concatenate + strip repo-root prefix so paths are repo-relative.
    log "clang-tidy: assembling $CLANG_TIDY_LOG (relative paths)"
    local files=()
    for d in "${DIRS[@]}"; do
        [[ -f "$WORK/clang-tidy.$d.txt" ]] && files+=("$WORK/clang-tidy.$d.txt")
    done
    if [[ ${#files[@]} -gt 0 ]]; then
        cat "${files[@]}" | sed "s#${REPO_ROOT}/##g" > "$CLANG_TIDY_LOG"
    else
        : > "$CLANG_TIDY_LOG"
    fi
    log "clang-tidy: $CLANG_TIDY_LOG ready ($(wc -l < "$CLANG_TIDY_LOG") lines)"
}

run_cppcheck() {
    if ! command -v cppcheck >/dev/null 2>&1; then
        log "cppcheck not installed -> skipping (install it, then run with PHASE=cppcheck)"
        return 0
    fi
    log "cppcheck: analyzing project (jobs=$JOBS)"
    # --relative-paths makes <location file=...> relative to the repo root so
    # sonar-cxx can resolve them against the scanner base dir.
    run cppcheck \
        --project="$DB" \
        --enable=warning,performance,portability \
        --inconclusive \
        --inline-suppr \
        --suppress=*:*/seastar/* --suppress=*:*/abseil/* --suppress=*:*/kmip/* --suppress=*:*/kmipc/* \
        --suppress=*:*/build/* --suppress=*:*/test/* --suppress=*:*/testlog/* --suppress=*:*/docs/* \
        --suppress=*:*/swagger-ui/* \
        -i seastar -i abseil -i kmip -i kmipc -i swagger-ui -i build -i test -i testlog -i docs \
        --relative-paths="$REPO_ROOT" \
        -j "$JOBS" \
        --xml --output-file="$CPPCHECK_XML" 2> "$OUT_DIR/cppcheck.err" || true
    # Belt-and-suspenders: strip any remaining absolute prefixes.
    sed -i "s#${REPO_ROOT}/##g" "$CPPCHECK_XML" 2>/dev/null || true
    log "cppcheck: $CPPCHECK_XML ready"
}

case "$PHASE" in
    clangtidy) run_clangtidy ;;
    cppcheck)  run_cppcheck ;;
    all)       run_clangtidy; run_cppcheck ;;
    *) log "unknown PHASE=$PHASE"; exit 1 ;;
esac

log "DONE."

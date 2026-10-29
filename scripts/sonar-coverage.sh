#!/usr/bin/env bash
#
# Copyright (C) 2024-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Memory-safe, resumable LLVM-coverage -> Cobertura pipeline for the SonarQube POC.
#
# Pipeline (per suite, then global):
#   *.profraw --(llvm-profdata merge)--> *.profdata
#   *.profdata --(llvm-cov export)-----> *.info (lcov)
#   per-suite lcov union --------------> sonar/<suite>.info
#   global lcov union -----------------> sonar/test_coverage.info
#   lcov_cobertura --------------------> sonar/coverage.cobertura.xml
#
# Safety: launch inside a memory-capped systemd user unit if you want OOM kills
# confined to the unit instead of the desktop, e.g.:
#   systemd-run --user -p MemoryMax=10G -p MemorySwapMax=2G \
#     --unit=sonar-cov scripts/sonar-coverage.sh --conc=3
# Not required though -- run it directly if you'd rather not bother.
#
# It is resumable: existing *.profdata / *.info / suite unions are not redone.
#
# Usage: scripts/sonar-coverage.sh [-c|--conc N] [-i|--integ-chunk N]
#   -c, --conc N          concurrency for the heavy llvm-cov/llvm-profdata steps
#                         (memory lever, not a CPU one -- each concurrent worker
#                         can load a full instrumented binary; default: 3)
#   -i, --integ-chunk N   profraw files per llvm-profdata merge pass when
#                         merging integration coverage (default: 64)

set -euo pipefail
shopt -s nullglob

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Fixed by this repo's layout, not meaningfully tunable per invocation.
COV_ROOT="testlog/coverage/coverage"   # where the *.profraw / *.profdata / *.info live
BUILD_ROOT="build/coverage/test"       # where the test binaries live (for -b binary-search-path)
OUT_DIR="testlog/coverage/sonar"       # where merged reports are written (test-run artifact, not build output)
SUITES="ldap raft unit boost"          # small-to-large: validate before the largest suite
# Integration coverage: the Python integration tests run the instrumented scylla
# server binary and dump one profile per test worker under testlog/coverage/.
# Despite living in testlog, this is *real* coverage of exercised server code, so
# it is folded into the same unified report.
INTEGRATION=1
INTEG_PROFRAW_ROOT="$COV_ROOT"
INTEG_BINARY="build/coverage/scylla"
COV_UTILS="test/pylib/coverage_utils.py"
MERGER="scripts/lcov_merge.py"
VENV="$OUT_DIR/venv"

# The two knobs that actually vary per run: how much RAM is free on this host
# right now (CONC), and how many profiles to hold in flight per merge pass
# during the integration merge (INTEG_CHUNK).
CONC=3
INTEG_CHUNK=64
while (( $# )); do
    case "$1" in
        -c|--conc) CONC="$2"; shift 2 ;;
        --conc=*) CONC="${1#*=}"; shift ;;
        -i|--integ-chunk) INTEG_CHUNK="$2"; shift 2 ;;
        --integ-chunk=*) INTEG_CHUNK="${1#*=}"; shift ;;
        -h|--help)
            sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done
[[ "$CONC" =~ ^[0-9]+$ ]] || { echo "Error: --conc must be a positive integer, got '$CONC'" >&2; exit 1; }
[[ "$INTEG_CHUNK" =~ ^[0-9]+$ ]] || { echo "Error: --integ-chunk must be a positive integer, got '$INTEG_CHUNK'" >&2; exit 1; }

mkdir -p "$OUT_DIR"

log() { printf '%s [sonar-cov] %s\n' "$(date '+%H:%M:%S')" "$*"; }

run() {  # keep the box responsive
    nice -n 19 ionice -c3 "$@"
}

ensure_lcov_cobertura() {  # bootstrap lcov_cobertura into a local venv if missing
    if [[ -x "$VENV/bin/lcov_cobertura" ]]; then
        return 0
    fi
    log "lcov_cobertura not found; provisioning venv at $VENV"
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install --quiet --disable-pip-version-check --upgrade pip >/dev/null
    "$VENV/bin/pip" install --quiet --disable-pip-version-check lcov_cobertura
    log "lcov_cobertura provisioned"
}

merge_suite() {  # *.profraw -> *.profdata
    local suite="$1" dir="$COV_ROOT/$suite"
    local all_profraw=("$dir"/*.profraw)
    if (( ${#all_profraw[@]} == 0 )); then
        log "$suite: no *.profraw, skipping"; return 0
    fi
    if [[ -f "$dir/.merge.done" ]]; then
        log "$suite: merge already done ($(ls "$dir"/*.profdata 2>/dev/null | wc -l) profdata), skipping"
        return 0
    fi
    # Drop profiles with no embedded binary ID (corrupt / killed-before-dump);
    # otherwise the repo merge aborts the whole suite on the first bad one.
    local profraw=() skipped=0 f
    for f in "${all_profraw[@]}"; do
        if llvm-profdata show --binary-ids "$f" 2>/dev/null | grep -qE '^[0-9a-fA-F]{16,}$'; then
            profraw+=("$f")
        else
            skipped=$((skipped+1)); log "$suite: skipping profile without binary id: $(basename "$f")"
        fi
    done
    if (( ${#profraw[@]} == 0 )); then
        log "$suite: no valid profiles after filtering, skipping"; return 0
    fi
    (( skipped > 0 )) && log "$suite: skipped $skipped invalid profile(s)"
    log "$suite: merging ${#profraw[@]} raw profiles -> profdata (conc=$CONC)"
    run python3 "$COV_UTILS" --concurrency "$CONC" \
        llvm-profiles merge "${profraw[@]}" "$dir"
    touch "$dir/.merge.done"
    log "$suite: merge produced $(ls "$dir"/*.profdata 2>/dev/null | wc -l) profdata"
}

tolcov_suite() {  # *.profdata -> *.info (only the ones still missing)
    local suite="$1" dir="$COV_ROOT/$suite"
    local pending=()
    local pd
    for pd in "$dir"/*.profdata; do
        [[ -f "${pd%.profdata}.info" ]] || pending+=("$pd")
    done
    if (( ${#pending[@]} == 0 )); then
        log "$suite: all profdata already converted to lcov, skipping"; return 0
    fi
    log "$suite: converting ${#pending[@]} profdata -> lcov (conc=$CONC)"
    run python3 "$COV_UTILS" --concurrency "$CONC" \
        llvm-profiles to-lcov "${pending[@]}" -b "$BUILD_ROOT/$suite"
    log "$suite: lcov conversion done"
}

union_suite() {  # per-suite *.info -> sonar/<suite>.info (genuine lcov, then reclaim disk)
    local suite="$1" dir="$COV_ROOT/$suite" out="$OUT_DIR/$suite.info"
    local infos=("$dir"/*.info)
    if [[ -f "$out" ]]; then
        log "$suite: union already exists ($out), skipping"
    elif (( ${#infos[@]} == 0 )); then
        log "$suite: no *.info to union, skipping"; return 0
    else
        log "$suite: merging ${#infos[@]} lcov files -> $out"
        run python3 "$MERGER" -o "$out" "${infos[@]}"
    fi
    # reclaim the large per-binary .info now that the suite union exists
    if [[ -s "$out" ]]; then
        rm -f "$dir"/*.info
    fi
}

process_integration() {  # testlog/coverage/**/*.profraw + scylla binary -> sonar/integration.info
    local out="$OUT_DIR/integration.info"
    if [[ -s "$out" ]]; then
        log "integration: $out already exists, skipping"; return 0
    fi
    if [[ ! -x "$INTEG_BINARY" ]]; then
        log "integration: binary $INTEG_BINARY not found, skipping integration coverage"; return 0
    fi
    # $INTEG_PROFRAW_ROOT now also holds the unit-test suites' own raw profiles
    # (moved there from build/coverage/test/<suite>/, see test/pylib/cpp/base.py),
    # which merge_suite()/tolcov_suite() above already process per-suite; skip
    # those directories here so the same profiles aren't double-counted into
    # both a per-suite .info and integration.info.
    local all=() f skip suite
    while IFS= read -r -d '' f; do
        skip=0
        for suite in $SUITES; do
            [[ "$f" == "$INTEG_PROFRAW_ROOT/$suite/"* ]] && { skip=1; break; }
        done
        (( skip )) || all+=("$f")
    done < <(find "$INTEG_PROFRAW_ROOT" -name '*.profraw' -print0 2>/dev/null)
    if (( ${#all[@]} == 0 )); then
        log "integration: no *.profraw under $INTEG_PROFRAW_ROOT (outside unit suites), skipping"; return 0
    fi

    local profdata="$OUT_DIR/integration.profdata"
    if [[ -s "$profdata" ]]; then
        log "integration: $profdata already exists, skipping merge"
    else
        # prefilter: drop empty / no-binary-id profiles
        local valid=() skipped=0 f
        for f in "${all[@]}"; do
            [[ -s "$f" ]] || { skipped=$((skipped+1)); continue; }
            if llvm-profdata show --binary-ids "$f" 2>/dev/null | grep -qE '^[0-9a-fA-F]{16,}$'; then
                valid+=("$f")
            else
                skipped=$((skipped+1))
            fi
        done
        log "integration: ${#valid[@]} valid profiles ($skipped skipped) -> merging in chunks of $INTEG_CHUNK"
        if (( ${#valid[@]} == 0 )); then
            log "integration: no valid profiles, skipping"; return 0
        fi
        # chunked merge: partials then a final merge (bounds working set, resumable-ish)
        local tmpd="$OUT_DIR/.integ_merge"; mkdir -p "$tmpd"; rm -f "$tmpd"/part_*.profdata
        local i=0 part=0 partials=()
        while (( i < ${#valid[@]} )); do
            local chunk=("${valid[@]:i:INTEG_CHUNK}")
            local p="$tmpd/part_$part.profdata"
            run llvm-profdata merge --sparse "${chunk[@]}" -o "$p"
            partials+=("$p"); part=$((part+1)); i=$((i+INTEG_CHUNK))
            log "  integration: merged chunk $part ($i/${#valid[@]} profiles)"
        done
        run llvm-profdata merge --sparse "${partials[@]}" -o "$profdata.tmp"
        mv -f "$profdata.tmp" "$profdata"
        rm -rf "$tmpd"
        log "integration: produced $profdata"
    fi

    log "integration: exporting lcov from $INTEG_BINARY (single large export)"
    run llvm-cov export --format=lcov -instr-profile "$profdata" -object "$INTEG_BINARY" \
        > "$out.tmp"
    mv -f "$out.tmp" "$out"
    log "integration: produced $out"
}

# ---- per-suite processing -------------------------------------------------
for suite in $SUITES; do
    log "=== suite: $suite ==="
    if [[ -s "$OUT_DIR/$suite.info" ]]; then
        log "$suite: consolidated lcov already exists, skipping suite"
        continue
    fi
    merge_suite  "$suite"
    tolcov_suite "$suite"
    union_suite  "$suite"
done

# ---- integration coverage (scylla server binary) --------------------------
if [[ "$INTEGRATION" == "1" ]]; then
    log "=== integration coverage ==="
    process_integration
fi

# ---- global union ---------------------------------------------------------
ALL=("$OUT_DIR"/*.info)
# exclude the consolidated target itself if a previous run left it
FINAL="$OUT_DIR/test_coverage.info"
GLOBAL_INPUTS=()
for f in "${ALL[@]}"; do
    [[ "$f" == "$FINAL" ]] && continue
    GLOBAL_INPUTS+=("$f")
done
if (( ${#GLOBAL_INPUTS[@]} == 0 )); then
    log "FATAL: no per-suite lcov files were produced"; exit 1
fi
log "consolidating ${#GLOBAL_INPUTS[@]} suite lcov files -> $FINAL"
run python3 "$MERGER" -o "$FINAL" "${GLOBAL_INPUTS[@]}"

# ---- lcov -> Cobertura XML ------------------------------------------------
COBERTURA="$OUT_DIR/coverage.cobertura.xml"
ensure_lcov_cobertura
log "converting $FINAL -> $COBERTURA"
run "$VENV/bin/lcov_cobertura" "$FINAL" -b "$REPO_ROOT" -o "$COBERTURA"

# Make the report portable: lcov_cobertura writes an absolute <source> (the -b
# base dir). sonar-cxx's CoberturaParser joins <source> + filename, so an
# absolute host path produces keys that don't exist inside the scanner container
# (files are at /usr/src) -> 0% coverage. Emptying <source> makes sonar-cxx keep
# the filenames relative and resolve them against the project base dir, which
# works regardless of where the repo is mounted.
sed -i 's#<source>[^<]*</source>#<source></source>#' "$COBERTURA"
log "patched <source> to empty for portable path resolution"

log "DONE. Cobertura report: $COBERTURA"

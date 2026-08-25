#!/usr/bin/env bash
#
# Copyright (C) 2024-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Spin up a local SonarQube Community server with the sonar-cxx plugin
# installed, plus its MCP server (mcp/sonarqube), for the ScyllaDB SonarQube
# POC. The containers are defined in ./docker-compose.yaml; this script drives
# compose and does everything compose can't (provisioning, scans).
#
#   https://github.com/SonarOpenCommunity/sonar-cxx
#
# All configuration is injected through the compose file, the bind-mounted
# ./plugins dir, and the rule list under ./config (applied via the API by
# `provision`), so you can switch checks on/off WITHOUT rebuilding or
# re-pulling anything:
#
#   docker-compose.yaml       -> containers, ports, server env  (apply: restart)
#   plugins/*.jar             -> /opt/sonarqube/extensions/plugins/     (plugins)
#   config/enabled-rules.txt  -> C++ profile allowlist, applied by `provision`
#
# Usage:
#   ./sonarqube.sh up          # pull images, fetch sonar-cxx plugin, start stack
#   ./sonarqube.sh wait        # block until the server API is UP
#   ./sonarqube.sh provision   # set admin password, create the project, apply
#                               # the rule allowlist, save an API token to .env
#   ./sonarqube.sh scan        # run the scanner against this checkout, using the
#                               # token from .env (extra args are passed through
#                               # to sonar-scanner-cli)
#   ./sonarqube.sh status      # container + server health
#   ./sonarqube.sh logs        # follow the stack's logs
#   ./sonarqube.sh restart     # restart the stack (picks up docker-compose.yaml edits)
#   ./sonarqube.sh down        # stop + remove the containers (keeps data volumes)
#   ./sonarqube.sh destroy     # down + delete data/logs volumes (full reset)
#
# Everything is overridable via env vars (see the knobs block below), e.g.:
#   SONAR_PORT=9001 SONAR_IMAGE=sonarqube:2025.4-community ./sonarqube.sh up
#
# `scan` works against a normal checkout or a git *worktree* -- see the
# comment on cmd_scan() below for why a worktree needs extra work.
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- knobs (all overridable via env) --------------------------------------
CONTAINER="${SONAR_CONTAINER:-scylla-sonarqube}"
# Default to the Community LTA line that sonar-cxx 2.3.0 is tested against
# (Community Build 25.8 / Server 2025.4 LTA). Pin a specific tag to reproduce.
IMAGE="${SONAR_IMAGE:-sonarqube:community}"
HTTP_PORT="${SONAR_PORT:-9090}"

# sonar-cxx plugin release (asset name carries a build number, hence 2.3.0.1496).
CXX_PLUGIN_TAG="${SONAR_CXX_TAG:-cxx-2.3.0}"
CXX_PLUGIN_JAR="${SONAR_CXX_JAR:-sonar-cxx-plugin-2.3.0.1496.jar}"
CXX_PLUGIN_URL="${SONAR_CXX_URL:-https://github.com/SonarOpenCommunity/sonar-cxx/releases/download/${CXX_PLUGIN_TAG}/${CXX_PLUGIN_JAR}}"

# Fresh SonarQube ships with admin/admin and forces a password change.
# The replacement must satisfy SonarQube's password policy (>=12 chars, at
# least one uppercase character, as of Community Build 26.8).
ADMIN_USER="${SONAR_ADMIN_USER:-admin}"
DEFAULT_ADMIN_PASSWORD="${SONAR_DEFAULT_ADMIN_PASSWORD:-admin}"
ADMIN_PASSWORD="${SONAR_ADMIN_PASSWORD:-Scylla-Sonar-Admin-1}"

# Project provisioned by `provision`; matches sonar.projectKey/projectName in
# sonar-project.properties.
PROJECT_KEY="${SONAR_PROJECT_KEY:-scylladb}"
PROJECT_NAME="${SONAR_PROJECT_NAME:-ScyllaDB}"

# API token `provision` generates for `scan`, saved here (never committed --
# see .gitignore) so `scan` has one input point and doesn't need SONAR_TOKEN
# passed by hand every time.
ENV_FILE="${SONAR_ENV_FILE:-$HERE/.env}"
TOKEN_NAME="${SONAR_TOKEN_NAME:-scan}"

PLUGINS_DIR="$HERE/plugins"
ENABLED_RULES="$HERE/config/enabled-rules.txt"
CXX_PROFILE_NAME="${SONAR_CXX_PROFILE:-scylla-cxx}"
# Rule repositories to bulk-activate in the C++ profile. sonar-cxx's built-in
# "Sonar way" profile ships EMPTY (0 active rules), and issues imported from
# clang-tidy/cppcheck reports are silently dropped unless their rule is active
# in the assigned profile -- without this, a scan imports ~nothing no matter
# how many findings the reports contain.
CXX_RULE_REPOS="${SONAR_CXX_RULE_REPOS:-cxx,clangtidy,cppcheck}"
# sonar-cxx ships sonar.cxx.file.suffixes=- (i.e. disabled) out of the box, and
# -- unlike most language plugins -- treats it as a GLOBAL-only setting: a
# project-level sonar.cxx.file.suffixes in sonar-project.properties is silently
# ignored. Without this set globally, scan indexes ~nothing as C++, so ncloc,
# issues and coverage all come out near-empty. `provision` sets it via the API.
CXX_FILE_SUFFIXES="${SONAR_CXX_FILE_SUFFIXES:-.cc,.hh,.h,.hpp,.cpp,.cxx,.c}"

DATA_VOL="${SONAR_DATA_VOL:-scylla_sonarqube_data}"
LOGS_VOL="${SONAR_LOGS_VOL:-scylla_sonarqube_logs}"

BASE_URL="http://localhost:${HTTP_PORT}"
WAIT_TIMEOUT="${SONAR_WAIT_TIMEOUT:-300}"   # seconds

log()  { printf '%s [sonarqube] %s\n' "$(date '+%H:%M:%S')" "$*"; }
fail() { log "ERROR: $*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || fail "'$1' is required but not installed"; }

# ---------------------------------------------------------------------------
ensure_sysctl() {  # Elasticsearch (bundled in SonarQube) needs a high mmap count
    local cur
    cur="$(cat /proc/sys/vm/max_map_count 2>/dev/null || echo 0)"
    if (( cur < 524288 )); then
        log "vm.max_map_count=$cur is below the required 524288; trying to raise it"
        if sysctl -w vm.max_map_count=524288 >/dev/null 2>&1 \
           || sudo sysctl -w vm.max_map_count=524288 >/dev/null 2>&1; then
            log "raised vm.max_map_count to 524288 (add it to /etc/sysctl.conf to persist)"
        else
            log "WARNING: could not raise vm.max_map_count. The server may fail to start."
            log "         Run manually: sudo sysctl -w vm.max_map_count=524288"
        fi
    fi
}

fetch_plugin() {
    mkdir -p "$PLUGINS_DIR"
    local dest="$PLUGINS_DIR/$CXX_PLUGIN_JAR"
    if [[ -s "$dest" ]]; then
        log "sonar-cxx plugin already present: ${dest#$HERE/}"
        return 0
    fi
    log "downloading sonar-cxx plugin: $CXX_PLUGIN_URL"
    curl -fSL --retry 3 --retry-delay 2 -o "$dest.tmp" "$CXX_PLUGIN_URL" \
        || fail "failed to download sonar-cxx plugin from $CXX_PLUGIN_URL"
    mv -f "$dest.tmp" "$dest"
    log "installed plugin -> ${dest#$HERE/}"
}

# All lifecycle goes through docker compose (docker-compose.yaml next to this
# script). Resolved knobs are exported explicitly so the compose file and the
# script can never disagree on names/ports/volumes.
compose() {
    SONAR_IMAGE="$IMAGE" SONAR_CONTAINER="$CONTAINER" SONAR_PORT="$HTTP_PORT" \
    SONAR_DATA_VOL="$DATA_VOL" SONAR_LOGS_VOL="$LOGS_VOL" \
        docker compose --project-directory "$HERE" "$@"
}

# ---------------------------------------------------------------------------
cmd_up() {
    need docker
    need curl
    ensure_sysctl
    fetch_plugin

    log "starting SonarQube ($IMAGE) on ${BASE_URL} + MCP server (docker compose)"
    compose up -d
    log "containers started. Follow startup with: $0 logs   (or: $0 wait)"
}

cmd_wait() {
    need curl
    log "waiting up to ${WAIT_TIMEOUT}s for ${BASE_URL} to come UP ..."
    local deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
    while (( $(date +%s) < deadline )); do
        if curl -sS "${BASE_URL}/api/system/status" 2>/dev/null | grep -q '"status":"UP"'; then
            log "SonarQube is UP at ${BASE_URL}"
            return 0
        fi
        sleep 5
    done
    fail "SonarQube did not become UP within ${WAIT_TIMEOUT}s (see: $0 logs)"
}

cmd_status() {
    need docker
    compose ps
    local status
    status="$(curl -sS "${BASE_URL}/api/system/status" 2>/dev/null || true)"
    echo "server: ${status:-no response from ${BASE_URL}}"
}

cmd_logs()    { need docker; compose logs -f; }
# up -d --force-recreate rather than `compose restart`: restart alone bounces
# the processes but keeps the old container config, silently ignoring
# docker-compose.yaml edits -- the one thing `restart` is documented to apply.
cmd_restart() { need docker; compose up -d --force-recreate; log "recreated the compose stack"; }

cmd_down() {
    need docker
    compose down
    log "removed containers (data volumes kept)"
}

cmd_destroy() {
    need docker
    compose down -v
    log "removed containers and data/logs volumes ($DATA_VOL, $LOGS_VOL)"
}

# ---- provisioning: password + rule toggles --------------------------------
try_auth() {  # try_auth <password> -> 0 if it authenticates as admin
    local pw="$1" code
    code="$(curl -sS -o /dev/null -w '%{http_code}' \
            -u "${ADMIN_USER}:${pw}" "${BASE_URL}/api/system/health" 2>/dev/null || echo 000)"
    [[ "$code" == "200" ]]
}

sonar_api() {  # sonar_api <METHOD> <path> [extra curl args...]
    local method="$1" path="$2"; shift 2
    curl -sS -u "${ADMIN_USER}:${API_PASSWORD}" -X "$method" "${BASE_URL}${path}" "$@"
}

token_is_valid() {  # token_is_valid <token> -- /validate always returns HTTP
    local body               # 200, so the "valid" field in the body is what matters.
    body="$(curl -sS -u "${1}:" "${BASE_URL}/api/authentication/validate" 2>/dev/null || true)"
    [[ "$body" == *'"valid":true'* ]]
}

resolve_admin_password() {
    if try_auth "$ADMIN_PASSWORD"; then
        API_PASSWORD="$ADMIN_PASSWORD"
        log "authenticated with configured admin password"
        return 0
    fi
    if try_auth "$DEFAULT_ADMIN_PASSWORD"; then
        log "changing default admin password"
        local response code
        # -sS alone doesn't fail on HTTP 4xx/5xx (only connection errors), so
        # check the status explicitly -- a silently-failed change here (e.g.
        # SONAR_ADMIN_PASSWORD not meeting the server's password policy)
        # would otherwise leave the real password unset admin/admin while the
        # rest of this function happily reports success.
        response="$(curl -sS -w '\n%{http_code}' -u "${ADMIN_USER}:${DEFAULT_ADMIN_PASSWORD}" -X POST \
            "${BASE_URL}/api/users/change_password" \
            --data-urlencode "login=${ADMIN_USER}" \
            --data-urlencode "previousPassword=${DEFAULT_ADMIN_PASSWORD}" \
            --data-urlencode "password=${ADMIN_PASSWORD}")"
        code="${response##*$'\n'}"
        if [[ "$code" != 2* ]]; then
            fail "failed to change admin password (HTTP $code): ${response%$'\n'*}"
        fi
        API_PASSWORD="$ADMIN_PASSWORD"
        return 0
    fi
    fail "could not authenticate to the SonarQube admin API (tried configured + default passwords)"
}

configure_cxx_language() {
    local IFS=','
    local suffix values=()
    for suffix in $CXX_FILE_SUFFIXES; do
        values+=(--data-urlencode "values=${suffix}")
    done
    log "setting global sonar.cxx.file.suffixes=${CXX_FILE_SUFFIXES} (sonar-cxx ships this disabled)"
    sonar_api POST "/api/settings/set" \
        --data-urlencode "key=sonar.cxx.file.suffixes" \
        "${values[@]}" >/dev/null \
        || fail "failed to set sonar.cxx.file.suffixes"
}

create_project() {
    if sonar_api GET "/api/projects/search?projects=${PROJECT_KEY}" \
        | python3 -c "import json,sys; sys.exit(0 if json.load(sys.stdin).get('components') else 1)" 2>/dev/null; then
        log "project '${PROJECT_KEY}' already exists"
        return 0
    fi
    log "creating project '${PROJECT_KEY}' (${PROJECT_NAME})"
    sonar_api POST "/api/projects/create" \
        --data-urlencode "project=${PROJECT_KEY}" \
        --data-urlencode "name=${PROJECT_NAME}" >/dev/null \
        || fail "failed to create project '${PROJECT_KEY}'"
}

provision_token() {
    if [[ -f "$ENV_FILE" ]]; then
        local existing
        existing="$(sed -n 's/^SONAR_TOKEN=//p' "$ENV_FILE" | tail -1)"
        if [[ -n "$existing" ]] && token_is_valid "$existing"; then
            log "existing token in ${ENV_FILE#$HERE/} is still valid; keeping it"
            return 0
        fi
    fi
    log "generating API token '${TOKEN_NAME}' for the 'scan' command"
    # Revoke any stale server-side token under the same name first --
    # generate errors out on a name collision, and the file above may have
    # been deleted independently of the token it once pointed to.
    sonar_api POST "/api/user_tokens/revoke" --data-urlencode "name=${TOKEN_NAME}" >/dev/null 2>&1 || true
    local resp token
    resp="$(sonar_api POST "/api/user_tokens/generate" --data-urlencode "name=${TOKEN_NAME}")"
    token="$(printf '%s' "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("token",""))' 2>/dev/null || true)"
    [[ -n "$token" ]] || fail "failed to generate API token: $resp"
    printf 'SONAR_TOKEN=%s\n' "$token" > "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    log "saved token to ${ENV_FILE#$HERE/} (used automatically by 'scan')"
    # The MCP container bakes SONARQUBE_TOKEN in at creation; recreate it so
    # it picks up the fresh token (no-op when the env didn't change).
    compose up -d mcp >/dev/null 2>&1 || true
}

# Find-or-create the editable C++ profile (built-in profiles can't be
# modified) and make it the default. Sets CXX_PROFILE_KEY for the callers.
ensure_cxx_profile() {
    if [[ -n "${CXX_PROFILE_KEY:-}" ]]; then
        return 0
    fi
    local from_key our_key
    { read -r from_key; read -r our_key; } < <(sonar_api GET "/api/qualityprofiles/search?language=cxx" \
        | python3 -c "import json,sys
ps=json.load(sys.stdin).get('profiles',[])
df=[p for p in ps if p.get('isDefault')] or ps
print(df[0]['key'] if df else '')
print(next((p['key'] for p in ps if p.get('name')=='${CXX_PROFILE_NAME}'), ''))" 2>/dev/null || true) || true
    [[ -n "$from_key" ]] || fail "no C++ (cxx) quality profile found; is the sonar-cxx plugin loaded?"

    if [[ -z "$our_key" ]]; then
        log "creating quality profile '${CXX_PROFILE_NAME}' (copy of the default C++ profile)"
        our_key="$(sonar_api POST "/api/qualityprofiles/copy" \
            --data-urlencode "fromKey=${from_key}" \
            --data-urlencode "toName=${CXX_PROFILE_NAME}" \
            | python3 -c 'import json,sys;print(json.load(sys.stdin).get("key",""))' 2>/dev/null || true)"
    fi
    [[ -n "$our_key" ]] || fail "could not create/find profile '${CXX_PROFILE_NAME}'"
    CXX_PROFILE_KEY="$our_key"

    log "setting '${CXX_PROFILE_NAME}' as the default cxx profile"
    sonar_api POST "/api/qualityprofiles/set_default" \
        --data-urlencode "language=cxx" \
        --data-urlencode "qualityProfile=${CXX_PROFILE_NAME}" >/dev/null || true
}

# Bulk-activate every rule from $CXX_RULE_REPOS in the profile. Without this
# the profile has 0 active rules and the clang-tidy/cppcheck report importers
# silently drop every issue (see CXX_RULE_REPOS above). Idempotent: already
# active rules are simply counted as unchanged. Rule *templates* can't be
# activated and show up in the "failed" count -- that's expected (6 of them
# in sonar-cxx 2.3).
activate_cxx_rules() {
    ensure_cxx_profile
    log "activating all rules from repos [${CXX_RULE_REPOS}] in '${CXX_PROFILE_NAME}'"
    local resp
    resp="$(sonar_api POST "/api/qualityprofiles/activate_rules" \
        --data-urlencode "targetKey=${CXX_PROFILE_KEY}" \
        --data-urlencode "repositories=${CXX_RULE_REPOS}")"
    log "rule activation result: $(printf '%s' "$resp" \
        | python3 -c "import json,sys
d=json.load(sys.stdin)
print('activated=%s, failed=%s (failures on rule templates are expected)' % (d.get('succeeded'), d.get('failed')))" 2>/dev/null || printf '%s' "$resp")"
}

# Curated allowlist mode: when config/enabled-rules.txt lists rules, wipe the
# profile (bulk-deactivate everything from $CXX_RULE_REPOS) and activate
# exactly the listed rules. Returns 1 when the file is missing or empty so
# provision can fall back to activate_cxx_rules (activate everything).
apply_enabled_rules() {
    local rules=() line
    if [[ -f "$ENABLED_RULES" ]]; then
        while IFS= read -r line; do
            line="${line%%#*}"; line="${line//[[:space:]]/}"
            [[ -n "$line" ]] && rules+=("$line")
        done < "$ENABLED_RULES"
    fi
    (( ${#rules[@]} > 0 )) || return 1

    ensure_cxx_profile
    log "applying rule allowlist from ${ENABLED_RULES#$HERE/} (${#rules[@]} rules)"
    log "deactivating all rules from repos [${CXX_RULE_REPOS}] in '${CXX_PROFILE_NAME}'"
    sonar_api POST "/api/qualityprofiles/deactivate_rules" \
        --data-urlencode "targetKey=${CXX_PROFILE_KEY}" \
        --data-urlencode "repositories=${CXX_RULE_REPOS}" >/dev/null

    local r ok=0 bad=0
    for r in "${rules[@]}"; do
        if sonar_api POST "/api/qualityprofiles/activate_rule" \
            --data-urlencode "key=${CXX_PROFILE_KEY}" \
            --data-urlencode "rule=${r}" >/dev/null 2>&1; then
            ok=$((ok+1))
        else
            log "  WARNING: could not activate rule '$r' (unknown key or rule template)"; bad=$((bad+1))
        fi
    done
    log "rule allowlist applied: ${ok} activated, ${bad} skipped"
}

cmd_provision() {
    need curl
    cmd_wait
    resolve_admin_password
    configure_cxx_language
    create_project
    provision_token
    apply_enabled_rules || activate_cxx_rules
    log "provisioning done."
    log "  admin login: ${ADMIN_USER} / ${API_PASSWORD}"
    log "  (default admin password is '${ADMIN_PASSWORD}'; override by setting SONAR_ADMIN_PASSWORD before running provision)"
    log "  scan token saved to ${ENV_FILE#$HERE/} -- run '$0 scan' to analyze this checkout"
}

# ---- scan: run the scanner against this checkout --------------------------
# Runs sonar-scanner-cli against this checkout. Works unmodified for a normal
# checkout, but a git *worktree* (.git here is a file, not a directory --
# "gitdir: <main-repo>/.git/worktrees/<name>") needs a workaround for two
# problems that have nothing to do with what's bind-mounted:
#
#   1. JGit (the scanner's SCM/blame sensor) does not understand the
#      linked-worktree "commondir" layout at all -- it throws
#      RepositoryNotFoundException even when the main repo's real .git is
#      mounted into the container at the exact path the worktree references.
#   2. The scanner refuses to follow symlinks that point outside its project
#      base dir, so overlaying a real .git on a directory of
#      symlinks-back-to-the-real-tree doesn't work either -- every file it
#      reads has to be a real file inside the mounted directory.
#
# The workaround: build a throwaway, real (hardlinked, so cheap -- no data is
# duplicated) copy of the working tree via rsync, paired with a standalone
# (non-worktree) .git built from a local clone of the main repo with its
# HEAD/index corrected to this worktree's actual commit, and scan that
# instead. Both scratch copies are removed when the scanner exits.
cmd_scan() {
    need docker
    need git
    need rsync
    # provision writes $ENV_FILE as exactly one SONAR_TOKEN=... line; a real
    # exported SONAR_TOKEN wins over the file.
    [[ -z "${SONAR_TOKEN:-}" && -f "$ENV_FILE" ]] && export "$(grep -m1 '^SONAR_TOKEN=' "$ENV_FILE")" || true
    : "${SONAR_TOKEN:?SONAR_TOKEN not set and no usable token in ${ENV_FILE#$HERE/} -- run '$0 provision' first, or pass SONAR_TOKEN=...}"
    # Always the server this script manages -- BASE_URL is the single source
    # of truth (derived from HTTP_PORT/SONAR_PORT at the top of the script),
    # so scan can't silently drift to a stale port cached in $ENV_FILE.
    local host_url="$BASE_URL"

    local repo_root; repo_root="$(cd "$HERE/../.." && pwd)"
    cd "$repo_root"

    local git_dir common_dir
    git_dir="$(git rev-parse --git-dir)"
    common_dir="$(git rev-parse --git-common-dir)"

    if [[ "$git_dir" == "$common_dir" ]]; then
        log "not a worktree checkout -- running the scanner directly against $repo_root"
        docker run --rm --network=host -v "$repo_root:/usr/src" \
            -e SONAR_HOST_URL="$host_url" -e SONAR_TOKEN="$SONAR_TOKEN" \
            sonarsource/sonar-scanner-cli "$@"
        return 0
    fi

    log "git worktree detected (gitdir=$git_dir, common=$common_dir) -- building a standalone scan copy"

    local main_repo work_parent scratch_git scratch_src
    # Not `local`: the EXIT trap below fires at real script exit, after this
    # function (and its local scope) has already returned, so it needs a
    # variable that's still alive then.
    main_repo="$(dirname "$common_dir")"
    work_parent="$(dirname "$repo_root")"
    scratch_root="$(mktemp -d -p "$work_parent" .sonar-scan.XXXXXX)"
    trap 'rm -rf "$scratch_root"' EXIT
    scratch_git="$scratch_root/git-standalone"
    scratch_src="$scratch_root/scan-src"

    log "cloning $main_repo locally for a standalone .git"
    if ! git clone --local --no-checkout --quiet "$main_repo" "$scratch_git" 2>/dev/null; then
        log "hardlinked clone failed (different filesystem?), falling back to a full copy"
        rm -rf "$scratch_git"
        git clone --local --no-hardlinks --no-checkout --quiet "$main_repo" "$scratch_git"
    fi
    local head_sha; head_sha="$(git rev-parse HEAD)"
    git -C "$scratch_git" update-ref HEAD "$head_sha"
    git -C "$scratch_git" read-tree "$head_sha"

    log "building a real (hardlinked) copy of the working tree, minus build/testlog/submodules"
    mkdir -p "$scratch_src"
    rsync -a --link-dest="$repo_root" \
        --exclude='.git' --exclude='build' --exclude='testlog' \
        --exclude='seastar' --exclude='abseil' --exclude='swagger-ui' \
        --exclude='__pycache__' \
        "$repo_root/" "$scratch_src/"
    # The scanner reads its coverage/issue reports from testlog/coverage/sonar
    # (see sonar-project.properties), which the testlog exclude above drops --
    # copy just that directory back in (minus the lcov_cobertura helper venv).
    if [[ -d "$repo_root/testlog/coverage/sonar" ]]; then
        mkdir -p "$scratch_src/testlog/coverage"
        rsync -a --link-dest="$repo_root/testlog/coverage/sonar" --exclude='venv' \
            "$repo_root/testlog/coverage/sonar/" "$scratch_src/testlog/coverage/sonar/"
    fi
    cp -al "$scratch_git/.git" "$scratch_src/.git"

    log "running the scanner against $scratch_src"
    docker run --rm --network=host -v "$scratch_src:/usr/src" \
        -e SONAR_HOST_URL="$host_url" -e SONAR_TOKEN="$SONAR_TOKEN" \
        sonarsource/sonar-scanner-cli "$@"
}

# ---------------------------------------------------------------------------
usage() {
    sed -n '2,38p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
    up)       cmd_up ;;
    wait)     cmd_wait ;;
    provision) cmd_provision ;;
    scan)     shift; cmd_scan "$@" ;;
    status)   cmd_status ;;
    logs)     cmd_logs ;;
    restart)  cmd_restart ;;
    down)     cmd_down ;;
    destroy)  cmd_destroy ;;
    ""|-h|--help|help) usage ;;
    *) fail "unknown command '$1' (try: $0 --help)" ;;
esac

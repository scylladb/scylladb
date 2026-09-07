# Local SonarQube server (Docker) for the ScyllaDB SonarQube POC

`sonarqube.sh` spins up a local [SonarQube Community] server in Docker with the
[sonar-cxx] plugin installed, so you can analyze ScyllaDB C++ sources and import
the coverage / static-analysis reports produced by the `scripts/sonar-*.sh`
pipeline.

[SonarQube Community]: https://hub.docker.com/_/sonarqube
[sonar-cxx]: https://github.com/SonarOpenCommunity/sonar-cxx

## Layout

```
docker/sonarqube/
├── sonarqube.sh            # lifecycle: up / wait / provision / status / logs / restart / down / destroy
├── docker-compose.yaml     # the containers: SonarQube server + its MCP server (mcp/sonarqube)
├── config/
│   └── enabled-rules.txt   # C++ profile allowlist: the rules to switch ON (applied by `provision`)
└── plugins/                # plugin jars, bind-mounted -> /opt/sonarqube/extensions/plugins/
                            # (sonar-cxx is auto-downloaded here; drop extra jars in too)
```

## Quick start

```bash
cd docker/sonarqube

./sonarqube.sh up          # pull images, download sonar-cxx, start server + MCP
./sonarqube.sh wait        # block until http://localhost:9090 is UP
./sonarqube.sh provision   # set admin password, enable C++ file indexing, create the
                            # project, apply the allowlist from config/enabled-rules.txt
```

> **Why `provision` sets file suffixes via the API:** sonar-cxx ships
> `sonar.cxx.file.suffixes` disabled (`-`) by default, and treats it as a
> GLOBAL-only setting -- unlike most language plugins, it ignores the same key
> set in `sonar-project.properties` or on the scanner command line. Without
> `provision` (or the equivalent `POST /api/settings/set`), a scan indexes
> ~nothing as C++, so ncloc, issues and coverage all come out empty/near-zero
> despite a successful-looking scan.

Then browse to <http://localhost:9090> (login `admin`, password from
`SONAR_ADMIN_PASSWORD`, default `Scylla-Sonar-Admin-1`).

Stop it with `./sonarqube.sh down` (keeps the analysis data) or wipe everything
with `./sonarqube.sh destroy`.

> **Elasticsearch requirement:** SonarQube bundles Elasticsearch, which needs
> `vm.max_map_count >= 524288`. `up` tries to raise it automatically (via `sudo
> sysctl`); if it can't, run `sudo sysctl -w vm.max_map_count=524288` yourself
> first and add it to `/etc/sysctl.conf` to persist.

## Injecting configuration / switching checks off

There are three independent injection points, all file-driven — no UI clicking,
nothing baked into an image:

1. **Containers and server behaviour — `docker-compose.yaml`.** Images, ports,
   volumes, and `SONAR_*` server env vars (SonarQube reads any
   `sonar.properties` key as an env var, e.g. `SONAR_WEB_JAVAOPTS`). Edit it
   and `./sonarqube.sh restart` to apply.

2. **C++ rules — `config/enabled-rules.txt`.**
   `provision` copies the built-in C++ profile to an editable `scylla-cxx`
   profile and makes it the default. When `enabled-rules.txt` lists rules, it
   deactivates everything from the cxx/clangtidy/cppcheck repositories and
   activates exactly that allowlist; when the file is missing or empty, it
   bulk-activates every rule instead. To turn a rule off, remove it from the
   allowlist and re-run `provision`. After tuning rules in the web UI, refresh
   the allowlist with the snippet in the header of `enabled-rules.txt`.

3. **Imported external issues (clang-tidy / cppcheck) — at scan time.** These
   come from the reports referenced in `sonar-project.properties`
   (`sonar.cxx.clangtidy.reportPaths`, `sonar.cxx.cppcheck.reportPaths`). To drop
   them from a run, override the paths on the scanner command line, e.g.
   `-Dsonar.cxx.clangtidy.reportPaths= -Dsonar.cxx.cppcheck.reportPaths=`. Which
   clang-tidy checks run at all is controlled by `CHECKS` in
   `scripts/sonar-cxx-analyze.sh`.

Additional plugins: drop any `*.jar` into `plugins/` and `./sonarqube.sh
restart`.

## MCP server

The stack also starts [SonarQube's MCP server](https://hub.docker.com/r/mcp/sonarqube)
(HTTP transport) on <http://localhost:8080/mcp>, so agents can query analysis
results. Register it with e.g.:

```bash
claude mcp add --transport http sonarqube-mcp http://localhost:8080/mcp \
    --header "Authorization: Bearer $(sed -n 's/^SONAR_TOKEN=//p' docker/sonarqube/.env)"
```

`provision` recreates the MCP container whenever it mints a fresh token.

## End-to-end with the coverage / analysis pipeline

The reports the scanner imports are produced from a `coverage`-mode build
(see `../../scripts/`):

```bash
# 1. build coverage mode and run the suites you want covered
./configure.py --mode coverage
ninja build/coverage/scylla            # + the unit-test binaries you need
./test.py --mode coverage --coverage   # produces *.profraw + testlog/coverage

# 2. turn raw profiles into a Cobertura report (memory-safe, resumable)
scripts/sonar-coverage.sh              # -> testlog/coverage/sonar/coverage.cobertura.xml

# 3. (optional) whole-repo clang-tidy / cppcheck reports
scripts/sonar-cxx-analyze.sh           # -> testlog/coverage/sonar/{clang-tidy.txt,cppcheck.xml}

# 4. start + provision the server (this directory) -- also saves an API
#    token to docker/sonarqube/.env for `scan` to use automatically
docker/sonarqube/sonarqube.sh up && docker/sonarqube/sonarqube.sh provision

# 5. run the scanner against the repo
docker/sonarqube/sonarqube.sh scan
```

`sonar-project.properties` (repo root) already declares the C++ file suffixes,
exclusions, and the report paths above.

`scan` mounts the checkout at `/usr/src` and runs `sonarsource/sonar-scanner-cli`
directly -- *unless* this checkout is a git worktree, in which case a plain
mount can't produce SCM blame data at all (JGit doesn't understand the
linked-worktree layout, and the scanner refuses to follow symlinks pointing
outside its project dir, so neither of the obvious workarounds helps). For a
worktree, `scan` builds a throwaway, real (hardlinked -- no data duplication)
copy of the tree paired with a standalone `.git` corrected to this worktree's
exact commit, runs the scanner against that, and removes the copy afterwards.
See the comment block above `cmd_scan()` in the script for the full
explanation.

`scan` always targets the server this script manages (`http://localhost:$SONAR_PORT`,
computed once as `BASE_URL` at the top of the script) -- it's not
configurable per invocation, so a port change can't leave `scan` silently
pointed at a stale host. It reads `SONAR_TOKEN` from `docker/sonarqube/.env`
if not already set as a real env var (an env var always wins over the
file). `.env` is written by `provision` and gitignored -- delete it and
re-run `provision` to get a fresh token, or pass `SONAR_TOKEN=...` on the
command line to use one without touching the file.

## Configuration knobs (env vars)

| Var | Default | Purpose |
|-----|---------|---------|
| `SONAR_PORT` | `9090` | Host port for the web UI. |
| `SONAR_IMAGE` | `sonarqube:community` | SonarQube image/tag. Pin e.g. `sonarqube:2025.4-community` for reproducibility. |
| `SONAR_CXX_TAG` / `SONAR_CXX_JAR` | `cxx-2.3.0` / `sonar-cxx-plugin-2.3.0.1496.jar` | sonar-cxx release to install. |
| `SONAR_CXX_URL` | GitHub release URL | Override to install from a mirror/local file server. |
| `SONAR_ADMIN_PASSWORD` | `Scylla-Sonar-Admin-1` | Admin password set by `provision`. |
| `SONAR_PROJECT_KEY` / `SONAR_PROJECT_NAME` | `scylladb` / `ScyllaDB` | Project created by `provision` (matches `sonar-project.properties`). |
| `SONAR_CONTAINER` | `scylla-sonarqube` | Container name. |
| `SONAR_DATA_VOL` / `SONAR_LOGS_VOL` | `scylla_sonarqube_data` / `_logs` | Named volumes for persistence. |
| `SONAR_WAIT_TIMEOUT` | `300` | Seconds `wait`/`provision` will poll for `UP`. |

## Version notes

sonar-cxx `2.3.0` requires **Java 21** on both server and scanner side and is
tested against SonarQube Community Build 25.8 (Server 2025.4 LTA) and 26.1
(Server 2026.1 LTA). The default `sonarqube:community` image tracks a compatible
build; pin `SONAR_IMAGE` if you need an exact version.

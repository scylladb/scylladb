# GitHub Actions performance

This document records an audit of every workflow in `.github/workflows/` and a
set of recommendations for making them faster and cheaper. The findings below
are based on real run data pulled from the GitHub Actions API (run durations,
per-step timings and conclusions for the most recent runs of each workflow),
not on the workflow definitions alone.

## Where the time actually goes

Median and p90 wall-clock per completed run, with the dominant step:

| Workflow | median | p90 | Dominant cost |
| --- | --- | --- | --- |
| seastar (daily, 3-config matrix) | 2304s (38m) | 4265s (71m) | 3 full C++ builds, from scratch |
| reproducible-build (weekly) | 4751s (79m) | 5607s | 2 full release builds |
| iwyu (every PR) | 354s | 399s | compilation DB + generated targets, from scratch |
| docs / Publish | 355s | 389s | `fetch-depth: 0` checkout + `sphinx -j auto` |
| compare-build-systems | 155s | 176s | full C++ configure |
| conflict-reminder | 120s | 201s | GitHub API loop |
| docs / Build PR | 99s | 109s | checkout + `sphinx -j auto` |
| shellcheck / license-header / codespell / docs-validate | 22–46s | | mostly `actions/checkout` |

Additional observations:

- **No workflow uses `actions/cache`.** Every C++ build (`iwyu`, `seastar`,
  `reproducible-build`, `compare-build-systems`, `clang-tidy`) compiles from
  scratch on every run.
- **20 of 26 workflows have no `concurrency` block**, so a rapid series of
  pushes queues superseded runs that nobody needs.
- Only `scylla-ci-route` sets `timeout-minutes`; nothing caps runaway builds.
- `clang-tidy` and `clang-nightly` are `disabled_manually`.
- Failure-driven reruns are significant for some workflows: `seastar` shows
  ~74% `startup_failure` in a recent sample, `docs / Publish` ~32% and
  `compare-build-systems` ~31% failures.
- On the public `scylladb/scylladb`, standard GitHub-hosted runners are free,
  so "smaller runner" changes save no money there. The cost lever is the
  private `scylla-enterprise` mirror, where per-minute billing applies.

## Ten recommendations

1. **Right-size runners.** Move compile-heavy jobs to more vCPUs; move trivial
   single-script jobs to `ubuntu-slim`. *Not applied: on a public repository
   runner minutes are free, so `ubuntu-slim` saves nothing here and can only
   regress parallel work (for example `sphinx -j auto`). Deferred.*
2. **Add `ccache`/`sccache` to every C++ compile job.** No cache exists today;
   `iwyu`, `seastar` and `reproducible-build` all recompile from zero.
3. **Remove the `sleep 1m` in `pr-require-backport-label`.** Replace the
   payload-based label check with one that reads live labels via the API, so a
   newly opened or pushed PR no longer idles for a minute before checking.
4. **Add `concurrency` with `cancel-in-progress`** to the workflows that lack
   it, so superseded runs are cancelled instead of queued.
5. **Add `paths`/`paths-ignore` filters** so C++-only checks (iwyu), the license
   header check and the shell linter do not run on documentation-only or
   metadata-only changes. `scylla-ci-route` already classifies docs-only
   changes for Jenkins; the GitHub checks should match.
6. **Use shallow/partial checkouts.** `check-license-header` and
   `differential-shellcheck` fetch the entire history of this large monorepo
   only to compute a diff against the base commit.
7. **Remove the `read-toolchain` prepare job** *(investigated, not pursued)*.
   It provisions a separate runner purely to read `tools/toolchain/image` and
   pass it to each build job's `container:`. There is no ownership-free way to
   remove it: `jobs.<id>.container.image` is resolved before the job starts, so
   it cannot be read from a file in the repository. The alternatives are a
   repository variable (the image is bumped by ordinary commits, and updating a
   variable needs `actions_variables: write`, so nobody owns it) or duplicating
   the literal into every workflow. The job is small and free, so it stays.

8. **Cache the docs Python/uv environment.** `docs-pr` and `docs-pages` set up
   `setup-python` + `setup-uv` and then run `make setupenv` on every run.
9. **Cap runaway builds and fix failure-driven reruns.** Add `timeout-minutes`
   to jobs, and investigate the high `startup_failure`/failure rates that cause
   paid-for reruns.
10. **Trim redundant triggers and checkouts.** For example
    `make-pr-ready-for-review` clones all of `master` just to run
    `gh pr ready`; several Jira/backport wrappers fire on many event types.

## Status

Applied in this series of pull requests:

- #3 remove the backport-label sleep
- #4 add concurrency
- #5 add path filters
- #6 shallow checkout
- #8 cache the docs environment
- #10 trim redundant triggers/checkouts

Investigated and dropped: #7 (remove the `read-toolchain` prepare job) - no
ownership-free way to keep the container image in sync (see above).
Deferred, pending a decision on paid runners: #1 (runner sizing).
Not applied: #2 (build caching) and #9 (timeouts).

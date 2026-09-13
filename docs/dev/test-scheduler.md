<!--
Copyright (C) 2026-present ScyllaDB
SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-->

# How to write a test.py scheduler

A **scheduler** decides how the selected tests are spread over the machine. One
is picked per run:

    $ ./test.py --scheduler=passthrough
    $ ./test.py --scheduler=list          # what is available

`passthrough` is the default. It decides nothing of its own and lets
pytest-xdist distribute the tests, work-stealing. It adds no arguments of its
own to the pytest command line.

Adding a scheduler should cost **one module plus one registry entry**. No change
to `test.py`, no change to CI. If it ever costs more, the platform is wrong and
the platform should be fixed.

## The interface `test.py` expects

This is all of it. `test/pylib/scheduling/registry.py` maps a name to your
class, and `test.py` then does this and nothing more:

```python
scheduler = SCHEDULERS[options.scheduler]()   # created once, no arguments
cfg       = RunConfig.defaults(options)       # the command line

scheduler.configure(cfg)                      # the only place cfg changes
                                              # (not called for --list)
cfg.log_scheduler(scheduler)                  # reads name, version, plugin

exit_code = execution.run_pytest(cfg, scheduler)   # reads plugin
```

So a scheduler is a `Scheduler` subclass with a name:

```python
class MyScheduler(Scheduler):
    """One line about what it does. This is what --scheduler=list shows."""

    name = "my-scheduler"
    version = "1"                                       # defaults to "1"
    plugin = "test.pylib.scheduling.schedulers.my_plugin"   # defaults to None

    def configure(self, cfg: RunConfig) -> None:        # defaults to a no-op
        ...

    def parameters(self) -> dict:                       # defaults to {}
        """Its own settings, to keep with the run."""
```

Only `name` is required. Everything else has a default that does nothing, so a
scheduler writes down only the part it uses. `test.py` never asks what kind of
scheduler it got: it calls `configure()` every time and hands the result to the
execution module, which is the only code that knows about pytest flags.

Three objects, each with one job. The **scheduler** decides. The **`RunConfig`**
holds what it decided. The **execution module** carries it out.

The registry is keyed by the name each class declares, so an entry cannot
disagree with the name a run reports:

```python
SCHEDULERS = {cls.name: cls for cls in (
    Passthrough,
    MyScheduler,
)}
```

> These signatures are a starting point, not a fixed API. Expect them to change
> once a second real scheduler exists. That is what shows which parts were
> general, and which were shaped around the first one. What should stay is the
> shape above: `test.py` picks an object, asks it to configure the run, and runs
> pytest once.

## The two halves, and where a scheduler really lives

A scheduler may want to decide things at two different times, so it has two
halves. Both are optional.

| Half | Runs | Can do |
| --- | --- | --- |
| `configure(cfg)` | in `test.py`, **before** pytest starts | set how many tests run at once, and the distribution mode |
| `plugin` | **inside** pytest, on the controller and the workers | everything else: placing tests, collection, per-test hooks, fixtures, checks on the workers, and anything that depends on what the tests say about themselves |

**Expect to write the plugin.** `configure()` can only set things that must be
known before the pytest process exists. It cannot place a test, drop one, read a
marker, or watch anything while the run goes on. Real scheduling belongs in the
plugin:

* a `configure()`-only scheduler is the cheap case. It works out one number for
  the whole run and lets xdist place the tests;
* a plugin-only scheduler is fine when nothing has to be decided before pytest
  starts. Inheriting the empty `configure()` is a complete answer;
* `passthrough` overrides neither, so xdist alone distributes the run.

## `plugin`: the half inside pytest

Your plugin is an ordinary pytest plugin module, loaded with `-p`.

**Only one scheduler plugin is loaded per run**, because only one scheduler is
selected. That is what makes taking over placement safe.
`pytest_xdist_make_scheduler` is a `firstresult` hook, and xdist's own
`DSession` implements it too, so two implementations would be decided by
registration order. They never meet. Mark yours `tryfirst=True`.

Each scheduler has its own plugin. A shared one would have to carry every
scheduler's hooks, markers and fixtures at once, and pick between them by name.

| Hook | Used for |
| --- | --- |
| `pytest_xdist_make_scheduler` | place tests yourself with a custom `Scheduling`, still on xdist |
| `pytest_collection_modifyitems` | order, group or drop tests |
| `pytest_runtest_protocol` / `makereport` | per-test bookkeeping, for example recording how long a test took |
| `pytest_configure`, `pytest_addoption` | your own markers and options |
| fixtures | for example, holding back cluster creation |
| worker-side code | a plugin on the controller cannot check anything inside a worker |
| `pytest_runtestloop` | run the loop yourself, with xdist not loaded. See the last section |

Markers are read here, in the plugin, because this is where the tests are:

```python
def pytest_collection_modifyitems(items, config):
    for item in items:
        claim = item.get_closest_marker("whatever_you_read")
```

## `configure(cfg)`: the half before pytest

```python
def configure(self, cfg: RunConfig) -> None:
    if cfg.options.repeat > 1:
        raise SchedulerError("it packs a fixed selection; --repeat is not supported with it")
    cfg.set_concurrency(self.capacity())
```

Rules:

* Called **once** per run that executes tests. It is not called for `--list`,
  which collects tests without scheduling them. The base class version decides
  nothing, so inheriting it is a complete answer.
* **Return nothing.** Say what you want by changing `cfg`. That is what the run
  then records.
* **Do not** run anything, build a command line, import xdist, or change global
  state. This method decides; it does not act.
* **Raise `SchedulerError` to stop the run.** `test.py` prints it as one line
  naming the scheduler, and exits with pytest's usage-error code. This is how a
  scheduler turns down a command line it cannot work with. Write the message
  yourself: only you know what you could not do. Any other exception is a bug in
  the scheduler, and gets a normal traceback.

What `cfg` gives you to read: `cfg.options`, the parsed `test.py` command line
as the user gave it, and `cfg.concurrency` and `cfg.dist`, the values the run
would use right now, so you can decide relative to them.

What you may change is a short, named list, not `add_arg(anything)`. Today that
is `cfg.set_concurrency(n)` and `cfg.set_dist(mode)`. The list will grow. Adding
to it means changing `RunConfig` and the execution module on purpose. It never
means changing `test.py`.

Options your scheduler cannot work with are your business, not `test.py`'s. A
scheduler that works out the concurrency itself has to decide what `--jobs`
means under it: ignore it, use it, or refuse the run with `SchedulerError`. Say
which in your own docstring. `test.py` has no opinion.

## Test metadata

What a test says about itself is a **marker**, and only a scheduler reads one.
Nothing in this platform does, and a test in `test/pylib_test` checks that.

Markers are visible where the test modules are imported: in the pytest workers,
and in the controller's `pytest_collection_modifyitems`. So a scheduler that
acts on them does it from its plugin. If one ever needs them *before* pytest
starts, because it has to decide something like `-n`, it can run its own
`--collect-only` pass. That belongs to the scheduler that needs it, not here.

The other source is what we already measure. `test/pylib/db/` writes
`time_taken`, `memory_peak`, `usage_sec` and more into
`{tmpdir}/sqlite_<HOST_ID>.db` on every run. The platform ships no reader for
it, because a scheduler that reads measured data first has to decide what "the"
duration of a test is: median, p95, the last N runs, only from a similar
machine. That decision belongs to the first scheduler that needs it, along with
the reader it implies.

## What every run says about its scheduler

Without this, "we switched something and CI got weird" is a claim nobody can
check. Every run prints one line before the tests start, so the console log of a
CI job names what scheduled it:

    scheduler: my-scheduler@1 plugin=test.pylib.scheduling.schedulers.my_plugin

With `--gather-metrics`, which is on by default and turns on the rest of the
metrics too, the run is also written to the metrics DB
(`{tmpdir}/sqlite_<HOST_ID>.db`), one `scheduler_runs` row per run:

| column | |
| --- | --- |
| `name`, `version`, `plugin` | which scheduler, so runs can be looked up by it |
| `config` | what it decided, as JSON: the xdist parameters the run ended up with, plus whatever `parameters()` returned |
| `host_id`, `timestamp` | which machine, and when |

```json
{"concurrency": 4, "dist": "worksteal", "parameters": {"budget_shards": 160}}
```

**The command line is not recorded.** It is not what the scheduler decided, and
it is not ours to keep: `--pytest-arg` can pass this repo's `--auth_password`
and `--aws-secret-key`, and this DB is collected as a build artifact. If your
scheduler reads something off the command line and it is worth keeping, return
it from `parameters()`. Then you decide what is safe to record, in your own
terms.

The row says what the scheduler decided, which is not always what pytest ran.
`--pytest-arg` is passed on unparsed and lands last on the pytest command line,
so a `-n` or `--dist` in it wins over the scheduler, and the row will not know.
There is no `test.py` option for `--dist`, so passing it that way is a real
thing to do. The same escape hatch can carry `--collect-only`, and then a row is
written for a session that only collected tests.

Because it is one JSON column and not a column per setting, a scheduler with
settings of its own needs no schema change:

```sql
SELECT timestamp, json_extract(config, '$.concurrency')
FROM scheduler_runs WHERE name = 'my-scheduler';
```

Each run writes its own `sqlite_<HOST_ID>.db`, so comparing runs means
collecting those files first. That is true of every other metric we record.

The row says what a run *was*, not what the scheduler changed along the way.
`configure()` is the only thing that touches a `RunConfig`, so the result always
belongs to the scheduler that was selected.

This is also why `version` exists, and why adding a new scheduler is better than
changing one in place. Once a CI step uses a scheduler, changing what that name
does changes what that step does.

## Adopting one in CI

The scheduler is a runtime argument, **per pipeline step**. One job can run one
scheduler while another job runs a different one, on the same commit. Going back
is a revert in the CI repo: every scheduler stays in the registry, so switching
back to `passthrough` is a change to the job, not to scylladb.

The path for a new scheduler: build it, run it on a staging job on real
hardware, decide go or no-go against numbers agreed **before** that run, then
use it on one CI step.

## If you ever need to leave xdist

The way out, if a scheduler ever needs something xdist cannot do, is
`-p no:xdist` plus a plugin that implements `pytest_runtestloop`. That is still
a plugin, not a second way to start tests.

It is **not built**. Nothing needs it, and adding it is one `RunConfig` setter
plus one branch in the execution module.

Before reaching for it, know what it costs. xdist's process pool, crash
handling, worker restart and report collection would all have to be replaced.
There is also a trap. `xdist.is_xdist_worker()` is just
`hasattr(config, "workerinput")`, and several places in the framework use it to
answer "am I the one that does the once-per-run setup". With xdist not loaded,
**every** process answers "I am the controller". A plugin that starts its own
workers would have each of them redo that setup, including the part that
*deletes* `testlog`, and each would start its own system resource monitor. Such
a plugin needs its own `is_controller()` and `worker_id()`, and its worker ids
must keep the `gw<N>` shape, because `host_registry.py` works out each node's IP
subnet from `int(worker_id[2:]) + 1`.

## Tests

`test/pylib_test/test_scheduling.py` runs the platform itself, with a stub
scheduler of each shape, whatever CI happens to run:

    $ ./tools/toolchain/dbuild -- pytest test/pylib_test

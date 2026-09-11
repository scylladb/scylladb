# How to write a test.py scheduler

A **scheduler** decides how the selected tests are spread over the machine.
Exactly one is selected per run:

    $ ./test.py --scheduler=passthrough
    $ ./test.py --scheduler=list          # what is available

`passthrough` is the default. It makes no decisions and hands the run to
pytest-xdist, which is what `test.py` has always done; its pytest command line
is byte-identical to the one produced before schedulers existed.

Adding a scheduler must cost **one module plus one registry entry**, with no
change to `test.py` or to CI. If it ever costs more than that, the abstraction
failed and it is the abstraction that should be fixed.

## The interface `test.py` expects

This is the whole of it. `test/pylib/scheduling/registry.py` maps a name to
your class, and `test.py` then does exactly this and nothing else:

```python
scheduler = SCHEDULERS[options.scheduler]()   # instantiated once, no arguments
cfg       = RunConfig.defaults(options)       # the command line

scheduler.configure(cfg)                      # the only place cfg is changed
cfg.log_scheduler(scheduler)                  # reads scheduler.name, .version, .plugin

exit_code = execution.run_pytest(cfg, scheduler)   # reads scheduler.plugin
```

So a scheduler is a `Scheduler` subclass with a name:

```python
class MyScheduler(Scheduler):
    """One line saying what it does — this is what --scheduler=list shows."""

    name = "my-scheduler"
    version = "1"                                       # defaults to "1"
    plugin = "test.pylib.scheduling.schedulers.my_plugin"   # defaults to None

    def configure(self, cfg: RunConfig) -> None:        # defaults to a no-op
        ...
```

Only `name` is required. Everything else inherits a default that does nothing,
so a scheduler writes down only the half it uses — and `test.py` never asks
what kind of scheduler it has: it calls `configure()` unconditionally and hands
the result to the execution module, the only code that knows about pytest
flags. Three objects, each with one job: the **scheduler** decides, the
**`RunConfig`** holds what was decided, the **execution module** carries it out.

The registry keys each entry by the name the class declares, so a registration
cannot disagree with the name a run reports:

```python
SCHEDULERS = {cls.name: cls for cls in (
    Passthrough,
    MyScheduler,
)}
```

> These signatures are a **starting point, not a frozen API**. Expect them to
> be revisited once a second real scheduler exists — that is what shows which
> parts were general and which were shaped around the first one. What should
> not drift is the shape above: `test.py` selects an object, asks it to
> configure the run, and runs pytest once.

## The two halves — and where a scheduler really lives

The decisions a scheduler may want to make happen at two different times, so it
has two optional halves:

| Half | Runs | Can do |
| --- | --- | --- |
| `configure(cfg)` | in `test.py`, **before** pytest starts | pre-run decisions only — concurrency, distribution mode |
| `plugin` | **inside** pytest, on the controller and the workers | everything else — dispatch, collection, per-test hooks, fixtures, worker-side enforcement, and anything that depends on what the tests declare about themselves |

**Expect to write the plugin.** `configure()` can only turn knobs that have to
exist before the pytest process does; it cannot place a test, deselect one,
read a marker, or observe anything while the run proceeds. Anything that is
scheduling rather than knob-turning belongs in the plugin half, and a scheduler
with both halves is the normal shape:

* a `configure()`-only scheduler is the cheap case — it computes one number for
  the whole run and leaves placement to xdist;
* a plugin-only scheduler is fine when nothing has to be decided before pytest
  starts: inheriting the no-op `configure()` is a complete answer;
* `passthrough` overrides neither, which is the whole of `passthrough`.

## `plugin` — the in-pytest half

Your plugin is an ordinary pytest plugin module, loaded with `-p`. **Exactly
one scheduler plugin is loaded per run**, because exactly one scheduler is
selected — which is what makes taking over dispatch safe:
`pytest_xdist_make_scheduler` is a `firstresult` hook that xdist's own
`DSession` also implements, so rival implementations would be resolved by
registration order, but rivals never coexist. Mark your implementation
`tryfirst=True`. Each scheduler gets its own plugin rather than sharing one; a
shared plugin would have to carry every scheduler's hooks, markers and fixtures
at once and demultiplex them by name.

| Hook | Used for |
| --- | --- |
| `pytest_xdist_make_scheduler` | take over placement with a custom `Scheduling`, staying on xdist |
| `pytest_collection_modifyitems` | ordering, grouping, deselection |
| `pytest_runtest_protocol` / `makereport` | per-test bookkeeping, e.g. feeding observed durations back |
| `pytest_configure`, `pytest_addoption` | its own markers and options |
| fixtures | e.g. gating cluster creation, enforcement |
| worker-side code | a controller-only plugin cannot enforce anything inside a worker |
| `pytest_runtestloop` | own the run loop outright, with xdist not loaded — see the escape hatch below |

Markers are read here, in the plugin, because that is where the tests are:

```python
def pytest_collection_modifyitems(items, config):
    for item in items:
        claim = item.get_closest_marker("whatever_you_read")
```

## `configure(cfg)` — the pre-run half

```python
def configure(self, cfg: RunConfig) -> None:
    if cfg.options.repeat > 1:
        raise SchedulerError("it packs a fixed selection; --repeat is not supported with it")
    cfg.set_concurrency(self.capacity())
```

Rules:

* Called **once**, always — the base class's version decides nothing.
* **Returns nothing.** Everything you want is expressed as changes to `cfg`,
  which is what the run then records.
* **Must not** execute anything, build a command line, import xdist, or mutate
  global state. It decides; it does not act.
* **May raise `SchedulerError` to abort the run.** `test.py` prints it as a
  one-line error naming the scheduler and exits with pytest's usage-error code.
  This is how a scheduler
  turns down a command line it cannot honour — the message is yours to write,
  because only you know what could not be honoured. Any *other* exception is a
  bug in the scheduler and gets a normal traceback.

What `cfg` offers to read: `cfg.options`, the parsed `test.py` command line
exactly as the user gave it, and `cfg.concurrency` / `cfg.dist`, the defaults
as they currently stand, so you can decide relative to them.

What it may change is a closed, named vocabulary rather than
`add_arg(anything)`: today that is `cfg.set_concurrency(n)` and
`cfg.set_dist(mode)`. The vocabulary is expected to grow; adding a call is a
deliberate change to `RunConfig` and the execution module — never to `test.py`.

Options the scheduler cannot honour are its own business, not `test.py`'s.
A scheduler that computes concurrency itself has to decide what `--jobs` means
under it — ignore it, respect it, or refuse the run with `SchedulerError` —
and say so in its own docstring. `test.py` has no opinion.

### If you ever need to leave xdist

The escape hatch, should a scheduler need something xdist's shape cannot
express, is `-p no:xdist` plus a plugin implementing `pytest_runtestloop` — a
plugin, not a second way of launching tests. It is **not built**: nothing needs
it, and adding it is a `RunConfig` setter plus one branch in the execution
module. Before reaching for it, note that xdist's process pool, crash handling,
worker restart and report aggregation would all have to be replaced, and that
there is a trap. `xdist.is_xdist_worker()` is literally `hasattr(config,
"workerinput")`, and several places in the framework branch on it to decide
"am I the one that does the once-per-run setup". With xdist unloaded, **every** process answers "I am
the controller", so a plugin that spawns its own workers would have each of
them re-run that setup — including the part that *wipes* `testlog` — and each
would start its own system resource monitor. Such a plugin needs an
`is_controller()` / `worker_id()` abstraction of its own, and its worker ids
must keep the `gw<N>` shape, because `host_registry.py` derives each node's IP
subnet from `int(worker_id[2:]) + 1`.

## Test metadata

What a test declares about itself is a **marker**, and only a scheduler
interprets it. Nothing in this framework reads one, and a test in
`test/pylib_test` enforces that.

Markers are visible where the tests are imported — in the pytest workers, and
in the controller's `pytest_collection_modifyitems` — so a scheduler that acts
on them does so from its plugin. If one ever needs them *before* pytest starts,
because it must decide something like `-n`, it can run its own `--collect-only`
pass; that belongs to the scheduler that needs it, not here.

The other source is what we already measure: `test/pylib/db/` writes
`time_taken`, `memory_peak`, `usage_sec` and more into
`{tmpdir}/sqlite_<HOST_ID>.db` on every run. No reader ships with the platform,
because a scheduler reading measured data has to decide first what "the"
duration of a test is — median, p95, last N runs, filtered to a comparable
machine class. That decision belongs to the first scheduler that needs it,
together with the reader it implies.

## What every run says about its scheduler

Without this, "we switched something and CI got weird" is unfalsifiable. Every
run prints one line before the tests start, so a CI job's console log names
what scheduled it:

    scheduler: my-scheduler@1 plugin=test.pylib.scheduling.schedulers.my_plugin

Under `--gather-metrics` — on by default, and the same flag that turns on the
rest of the metrics — the run is recorded in the metrics DB
(`{tmpdir}/sqlite_<HOST_ID>.db`), one `scheduler_runs` row per run:

| column | |
| --- | --- |
| `name`, `version`, `plugin` | which scheduler, so runs can be selected by it |
| `config` | the whole `RunConfig` as JSON — concurrency, dist, and the entire test.py command line |
| `host_id`, `timestamp` | which machine, and when |

```sql
SELECT timestamp, json_extract(config, '$.concurrency')
FROM scheduler_runs WHERE name = 'my-scheduler';
```

Each run writes its own `sqlite_<HOST_ID>.db`, so comparing runs means
collecting those files first — the same as for every other metric we record.

The command line lives in a JSON column rather than a column per option, so
adding a `test.py` option never changes this schema. That makes the row a
record of what a run *was*, not a diary of what the scheduler changed:
`configure()` is the only thing that touches a `RunConfig`, so the result is
attributable to the selected scheduler by construction.

This is also why `version` exists, and why adding a new scheduler is preferred
over changing an existing one in place: once a CI step uses a scheduler,
changing what that name does changes what that step does.

## Adopting one in CI

Selection is a runtime argument, **per pipeline step**: one job can run one
scheduler while another runs a different one on the same commit. Rollback is a
revert in the CI repo — every scheduler stays in the registry, so switching
back to `passthrough` is a change to the job, not to scylladb.

The path for a new scheduler is: build it → run it on a staging job on real
hardware → go/no-go on thresholds agreed **before** that run → adopt it on one
CI step.

## Tests

`test/pylib_test/test_scheduling.py` exercises the framework path, including a
stub scheduler of each shape, regardless of what CI runs:

    $ ./tools/toolchain/dbuild -- pytest test/pylib_test

# bpcov: breakpoint coverage

bpcov measures which lines of Scylla's source code a run executed - for
example, a run of `test/cqlpy` - using an ordinary optimized build of
Scylla rather than a build with coverage instrumentation.

## The idea: one-shot breakpoints

A conventional coverage build (clang's `-fprofile-instr-generate
-fcoverage-mapping`, or gcc's `--coverage`) adds a counter increment to
every piece of code. These increments are paid on every execution, for the
whole run, and they make the code noticeably slower. On a multi-core machine
they are also writes to memory that all cores share, which hurts Seastar's
shard-per-core design in particular.

But to know *whether* a line ran, we only need to notice the *first* time it
runs. bpcov does this with one-shot breakpoints:

1. Before the program starts, write a breakpoint instruction (`int3` on
   x86-64, a single byte) at the start of the code of every source line.
2. When a thread reaches a breakpoint, the CPU traps to the tracer, which
   records that the line was executed, puts the original byte back, and lets
   the thread continue from the same address.
3. A line's breakpoint is therefore hit at most once. After the code a
   test uses has been reached for the first time, the program runs at
   full speed, exactly the same code as without bpcov.

The addresses come from the line-number table (DWARF `.debug_line`) that the
compiler writes alongside the code. This table does not change the generated
code at all, so the binary that is measured is the same binary that is
normally tested.

The line table has one gap: it attributes every instruction of an inlined
function to the callee's lines, never to the line that called it. So a line
like `opts.enabled(false);`, which calls a small inline setter, has no code
of its own in the line table. For such lines, bpcov also reads the entries
for inlined calls in `.debug_info` (which the line-tables-only debug
information keeps too). Each entry gives the calling file and line, and the
address ranges of the inlined code. bpcov puts a breakpoint at the start of
each of these ranges, and counts the calling line as executed if any of them
was reached.

One more detail: an address found in either way may be at NOP instructions
that the compiler added as padding, to align the instruction after them
(often the beginning of a loop). Execution often doesn't pass through this
padding, but jumps over it straight to the aligned instruction, so bpcov
moves such a breakpoint to the first instruction after the NOPs, which runs
whenever the NOPs do.

So most lines get more than one breakpoint - 1,584,563 breakpoints for
175,566 lines with code. Half of the lines have one or two, but one in ten has
19 or more. There are two reasons:

1. The same line has code in many functions: in every function it was
   inlined into, in every instantiation of a template, and in the separate
   functions the compiler makes of a coroutine. This is mostly true of lines
   in headers: they have code in 12 functions on average. At the extreme,
   `class compound_wrapper {` in `keys/keys.hh`, whose implicitly defined
   members are inlined everywhere, has 15,550 breakpoints in 2,365 functions.
2. Even in one function, a line's code is usually in several pieces. The
   optimizer mixes its instructions with those of the neighboring lines, every
   inline function it calls puts the callee's code in the middle of it (and
   each inlined call gets a breakpoint of its own), and some code is
   duplicated, such as a loop's condition. Lines of `.cc` files have code in
   only 1.5 functions on average, but have 7.2 breakpoints. For example,
   `file = opts["options-file"].as<sstring>();` in `main.cc` has 11
   breakpoints, all in `read_config()`.

One breakpoint per line would not be enough: the pieces of a line are not
necessarily on the same execution path. A copy in one function tells nothing
about the copies in others, and even in one function, part of a line's code
can be skipped - for example, `b` in `a && b`, or a branch of `x ? y : z` -
and some of it may only run when an exception is thrown. The line table does
not say which piece of a line always runs; that would need analyzing the
machine code's control flow. A breakpoint on a piece that didn't run would
show an executed line as never executed. So bpcov puts a breakpoint on every
piece, and counts the line as executed if any of them was hit.

The extra breakpoints cost only traps: each breakpoint traps at most once.
In a run of the Alternator tests with an earlier version (which did not yet
have the breakpoints for inlined calls), about two thirds of the traps were
for another copy of a line that had already been executed (see "The cost of
bpcov" below). A possible improvement is for the tracer to remove the other
breakpoints of a line after its first hit, when they belong to no other line
that still wasn't executed.

The idea is not new. Tikir and Hollingsworth deleted coverage probes after
their first execution in 2002 (*Efficient Instrumentation for Code Coverage
Testing*, ISSTA 2002); [kcov](https://github.com/SimonKagstrom/kcov) and
its predecessor bcov collect coverage on Linux with breakpoints at
line-table addresses; [mesos](https://github.com/gamozolabs/mesos) does
one-shot basic-block breakpoints on Windows; and UnTracer (Nagy and Hicks,
*Full-speed Fuzzing*, IEEE S&P 2019) uses the same trick to speed up
fuzzing. What bpcov adds is fitting it to Scylla: only Scylla's own source
files get breakpoints, the line table of a build is read once and cached,
and hits are saved in a way that survives the way the test runners stop
Scylla.

## Building Scylla for bpcov

Release and debug builds already have line tables (as part of their full
debug information). Dev builds normally have no debug information at all, to
keep compiling and linking fast; add only the line tables with:

```
./configure.py --line-tables   # plus your usual options
ninja build/dev/scylla
```

This adds `-gline-tables-only` to dev mode, with the debug sections
compressed with zstd (`-gz=zstd`, and `--compress-debug-sections=zstd` when
linking). The option does the same for the other modes when they are
configured with `--debuginfo 0`; it has no effect on modes that already have
full debug information. See "The cost of `--line-tables`" below for what it
costs, and "Which build mode to measure" for why dev mode is the
recommended one.

## Usage

```
SCYLLA=scripts/bpcov/scylla test/alternator/run
scripts/bpcov/bpcov.py less alternator/executor.cc
```

The `bpcov.py` commands work on `build/dev/scylla` unless given another
binary with `--binary`.

**Preparation** of a build happens by itself, the first time the build is run
under bpcov: bpcov reads the binary's line table and inlined calls, picks the
breakpoint addresses, and stores them in `build/dev/bpcov/`. This takes about
one second, which the first boot of each new build waits for; later boots use
the stored data. The file `build/dev/bpcov/info` says which binary, which
build of it (its build-id) and which version of bpcov the data is for. When
it doesn't match - Scylla was rebuilt, or bpcov changed how it computes the
data - the data is prepared again, replacing the old data and the runs of the
old build: like `build/dev` itself, `build/dev/bpcov` only has the latest
build. If several runs start at the same time, one prepares and the others
wait for it. Preparation prints nothing unless it fails, because under the
wrapper its output would be Scylla's output. `bpcov.py prepare` does the same
preparation right away, for those who prefer to wait for it before running
tests.

**`scripts/bpcov/scylla`** stands in for the Scylla executable. It runs
`build/dev/scylla` (or `$BPCOV_SCYLLA`) under the tracer, with the same
arguments. Anything that takes the path of the Scylla executable in `$SCYLLA`
can use it, including `test/cqlpy/run`, `test/alternator/run` and
`test/rest_api/run`. Each run records the breakpoints it hit in a new file in
the `hits/` subdirectory, with one byte per breakpoint. The tracer maps this
file into its memory, so a hit is recorded as it happens, without a system
call, and nothing is lost when Scylla is killed (see "Killing Scylla loses
nothing" below). The tracer's own messages (how many breakpoints were
inserted and hit) go to a `.log` file next to it, never to Scylla's output:
some tests read Scylla's log, and expect it to start with Scylla's first log
line.

**`cat FILE`** prints a source or header file, putting `* ` before every
line that has compiled code that never ran, and two spaces before the other
lines. All the commands that show coverage combine all the runs of this build
so far.

**`less FILE`** shows the same in `less`, starting at the first line marked
with `*`. Press `n` to go to the next one.

**`report`** prints the
fraction of lines executed per top-level directory. It also writes an lcov
`coverage.info` file to the data directory. Options:

- `--html DIR`: also write an HTML report (with `genhtml`) showing each
  source file with its executed and not executed lines.
- `--prefix alternator/`: only report files whose path starts with this
  prefix. Can be given several times.
- `--reset`: forget the runs after reporting, so the next report starts from
  zero.

The HTML pages are produced by `genhtml`, from the lcov package, which reads
the `coverage.info` file. Editors can show the same file too; for example,
the VS Code extension "Coverage Gutters" colors executed and not executed
lines in the editor.

`cat --gcov FILE` prints a file in the format of gcov's `.gcov` files
instead: `#####` marks a line whose code was never executed, `1` a line that
was executed, and `-` a line without code of its own.

To see the effect of a single test, use `--reset` (or delete the `hits/`
files) before running it. Runs of different tests add up, so you can run
several test suites and then report their combined coverage.

## How to read the results

bpcov reports *line* coverage of an optimized (`-O2`) build, and the
optimizer affects what a "line" is:

- A line counts as executed if any copy of its code was executed. A function
  that was inlined in many places has a breakpoint in each copy.
- A line that has no code of its own in the binary does not appear in the
  report at all: neither as executed nor as not executed. This includes code
  the compiler removed as unreachable, templates that were never
  instantiated, and some lines whose code the compiler merged with a
  neighboring line's.
- Executed means at least once. There are no execution counts.

- Lines that consist only of closing braces (`}`, `};`, `});` and the like,
  possibly with a `//` comment) are not measured at all, and appear as lines
  without code. The only code such a line has is calls to the destructors of
  the scope's variables (or of the members a constructor has already
  constructed), and often only in code that runs when an exception passes
  through the scope, which the compiler generates and attributes to the end
  of the scope. A `*` on such a line would usually only mean that no exception
  passed through that scope during the tests - true of nearly every scope,
  and nothing to test. Leaving these lines out loses nothing: if a scope never
  ran, its other lines show that. Before this rule, 129 of the 1,428 lines
  that `test/alternator` did not execute in `alternator/` were such braces.
  (Finding the exception-handling code itself in the executable, instead of
  relying on the source text, would require following the code instruction
  by instruction: the tables that describe it only say where it starts.)

So a line reported as executed was executed, and a line reported as not
executed almost certainly was not. When exact, per-region results are
needed, use an instrumented build (the `coverage` build mode).

### Line coverage is not branch coverage

A line counts as executed when any of its code ran, which isn't the same as
all of it - or all the ways through it - having run. Some examples of
untested code that line coverage does not show:

- `if (x.empty() || y.empty()) {` is executed if this condition was ever
  checked. But if `x.empty()` was always true, `y.empty()` was never even
  evaluated, and the case of the condition being false was never tested.
- `return ok ? f() : g();` is executed if either `f()` or `g()` was called;
  a test suite that never calls `g()` here leaves this line looking fully
  tested.
- An `if` without an `else`: if the condition was always true, all the lines
  are marked as executed, yet the case where the condition is false - doing
  nothing - was never tested. That case has no line of its own to mark.
- A loop that always ran at least once, never zero times, looks the same as
  one that was tested both ways.

Branch coverage, which reports each outcome of each condition, would show
these. When writing tests for a function, it is worth reading its
conditions with this in mind, rather than only looking for `*` marks.

## Only Scylla's own code is measured

bpcov reports coverage only of Scylla's own source and header files, and
leaves out all the other code that ends up in the Scylla executable: Seastar,
abseil, fmt, the C++ and Rust standard libraries, Boost and other libraries,
and code generated during the build (under `build/`). This is deliberate:

- The report is about the code we write and test. The coverage of
  thousands of lines of `std::` or Boost templates would only hide it.
- Fewer breakpoints means fewer traps, so Scylla boots faster under the
  tracer. Most of the code in the executable comes from library headers.
- A line of a Scylla header is still measured wherever the compiler copied
  it, for example, when a function from `utils/managed_bytes.hh` is inlined
  into a `.cc` file. In the other direction, library code inlined into
  Scylla's functions gets no breakpoints, so it doesn't count as part of
  them.

A source file counts as Scylla's own if the line table names it by a
relative path, the file exists in the Scylla source tree, and it is not under
`seastar/`, `abseil/`, `fmt/` or `build/` (the list `excluded_prefixes` in
`prepare.c`). In dev mode, Seastar is also a separate shared library, which
bpcov does not instrument at all.

## Killing Scylla loses nothing

Scylla does not need to shut down cleanly for its coverage to be saved. It
can be killed with SIGKILL, crash or abort, and every line it executed until
that moment is still recorded:

- A thread that reaches a breakpoint stays stopped until the tracer has
  recorded the hit, so a line is recorded before it runs - even a line that
  then crashes Scylla.
- The hit is recorded in the tracer, a separate process, not in Scylla's
  memory: the tracer maps the hits file with `MAP_SHARED`, so setting a byte
  in it puts the change straight into the kernel's page cache of the file.
  Nothing is buffered or written at exit. Even if the tracer itself is
  killed (`test/cqlpy/run`, for example, kills the whole process group with
  SIGKILL), the hits it recorded remain in the file. Only a crash of the
  whole machine could lose hits that the kernel did not yet write to disk.

Conventional coverage tools keep their counters in the program's own memory
and write them to a file when the program exits normally: gcc's
`--coverage` and clang's `-fprofile-instr-generate` (including Scylla's
`coverage` build mode) write them from an `atexit` handler. A program killed
with SIGKILL, or one that crashes, loses all of its coverage for the run, so
using them requires stopping the program gently. (Clang has an optional
"continuous mode", `-fprofile-continuous` or `%c` in `LLVM_PROFILE_FILE`,
which maps the counters to the file like bpcov does, but it is not the
default, and Scylla's `coverage` mode does not use it.)

## Which build mode to measure

**Use dev mode** (configured with `--line-tables`). It runs all the tests,
including those that need error injection, and its line table is the most
accurate of the three modes. Debug mode is a reasonable alternative, and
needs no special configuration. Release mode is not recommended.

To measure a build other than `build/dev/scylla`, set `BPCOV_SCYLLA` when
running the tests, and pass the same executable to the commands that show
the coverage with `--binary`:

```
SCYLLA=scripts/bpcov/scylla BPCOV_SCYLLA=build/debug/scylla test/alternator/run
scripts/bpcov/bpcov.py report --binary build/debug/scylla
```

The coverage data of each build is kept next to its executable (here, in
`build/debug/bpcov/`).

These conclusions come from running `test/alternator/run` under bpcov with
dev, debug and release builds of the same source (2026-10-30), and comparing
which lines each run executed. Coverage outside `alternator/` changes a bit
from run to run even with the same build - up to about 700 lines, mostly the
sstable index reading code, which runs or not depending on when memtables
happen to be flushed - but in `alternator/`, repeated runs agreed exactly. So
the comparisons below are of `alternator/`:

- **Debug** runs exactly the same tests as dev, and already has line tables
  (as part of its full debug information), so it doesn't need
  `--line-tables`. But it is several times slower - the Alternator tests
  took 5:47 under bpcov, instead of 1:05 in dev - and the sanitizers'
  instrumentation makes its line table a bit less accurate, in both
  directions. Some lines' ordinary code is not marked as the beginning of
  the line, and the only marked code of the line is the sanitizer's error
  handler, so the line appears never executed. And exception-cleanup code
  shared by the whole function is sometimes attributed to a `throw` line, so
  that line appears executed when any exception passes through the function.
  Of the 8,158 lines of `alternator/` that have code in both builds, 13 ran
  only according to dev, and 28 only according to debug. More than half of
  the latter are of the second kind, or are real differences caused by
  debug's slowness (the TTL expiration scanner, and timeouts).
- **Release** skips the tests that need error injection (8 of the Alternator
  tests), which dev and debug builds have. Even comparing the same tests,
  its line table is less accurate, because `-O3` moves and merges more code
  between lines. Of the 7,589 lines of `alternator/` that have code in both
  builds, 18 ran only according to dev, and 4 only according to release.
  In the whole of Scylla, 1,164 lines (1.7% of the lines that ran in dev) appeared
  not executed in release although they ran in dev. Moreover, with link-time
  optimization more code is inlined and removed, so release has fewer lines
  with code (7,682 in `alternator/`, instead of 8,192 in dev), and its
  coverage percentages can't be compared with those of other modes.

## Cost at run time

The program runs at full speed, except for one trap for each line the first
time it runs. A trap is a round trip to the tracer process, about 8
microseconds. Most of these happen while Scylla boots, which reaches a large
part of its code - the boot takes about 1.6 seconds longer; later, a test that
reaches code not executed before pays a trap for each such line, once.

## Measurements

Measured on 2026-10-29, with clang on Fedora 44, on an 8-core Intel Core
Ultra 7 258V laptop with 30 GB of memory.

### The cost of `--line-tables`

**Generated code: no change.** For five source files (`utils/UUID_gen.cc`,
`alternator/executor.cc`, `cql3/statements/select_statement.cc`,
`service/storage_proxy.cc`, `replica/database.cc`), the code and data
sections of the object files compiled with and without `-gline-tables-only`
were identical (disassembly and section sizes compared after removing the
debug sections). So the run time is not affected at all. In contrast, full
`-g` changed the placement of some functions in three of these five files.

**Compile time and memory**, each file compiled alone on an idle machine,
dev mode flags, with the precompiled header built with the same flags:

| file | no debug info | `-gline-tables-only` | full `-g` |
|---|---|---|---|
| `alternator/executor.cc` | 19.6 s, 1.87 GB | 21.0 s (+7%), 1.91 GB | 24.7 s (+26%), 2.04 GB |
| `cql3/statements/select_statement.cc` | 16.0 s, 1.85 GB | 16.7 s (+4%), 1.87 GB | 19.0 s (+18%), 1.97 GB |
| `service/storage_proxy.cc` | 32.3 s, 2.57 GB | 35.3 s (+9%), 2.63 GB | 42.0 s (+30%), 2.87 GB |
| `replica/database.cc` | 26.0 s, 2.19 GB | 27.8 s (+7%), 2.25 GB | 33.3 s (+28%), 2.48 GB |
| `utils/UUID_gen.cc` | 0.46 s | 0.49 s | 0.53 s |
| precompiled header `stdafx.hh` | 6.1 s, 1.21 GB | 6.1 s, 1.21 GB | 6.1 s, 1.21 GB |

Compressing the debug sections (`-gz=zstd` or `-gz=zlib`) made no measurable
difference to these compile times (the same files, two runs each).

**Object file size**: about 1.5-1.75 times larger with the compressed line
tables (1.56 times for all of them, see the full build below; for example,
`replica/database.o` grows from 4.3 MB to 7.2 MB, `service/storage_proxy.o`
from 6.5 MB to 10.0 MB, and `alternator/executor.o` from 2.5 MB to 4.4 MB).
Uncompressed, they would be about 2.5 times larger (10.6 MB, 15.0 MB and 6.7
MB), and with full `-g`, 30.9 MB, 44.3 MB and 21.1 MB. The precompiled header
does not change (191 MB).

**Link time and memory** of `build/dev/scylla`, from the same object files:
0.34 s and 1.0 GB of memory without the line tables, 0.80-0.85 s and 1.37 GB
with the compressed line tables (0.53-0.67 s and 1.6 GB with uncompressed
ones).

**Size of `build/dev/scylla`**: 139 MB without line tables, 223 MB with the
compressed line tables, and 439 MB with uncompressed ones. Only the file
grows; the debug sections are not loaded into memory when Scylla runs. In the
compressed executable, `.debug_info` takes 43 MB (even `-gline-tables-only`
describes each function and inlined call, so debuggers and symbolizers can
name inlined frames), `.debug_line` 17 MB, `.debug_rnglists` 12 MB,
`.debug_str` 4.4 MB and `.debug_addr` 4.2 MB; uncompressed, they were 128,
71, 23, 34 and 33 MB. Compression with zlib (as release mode uses) gave 246
MB, so zstd is both smaller and faster. Tools that read the debug information
must decompress it: `llvm-dwarfdump --debug-line` took 2.70 s instead of 2.31
s, and `bpcov-prepare` 0.91 s instead of 0.58 s.

**Column numbers are needed**: leaving them out (`-gno-column-info`) would
save about 30% of `.debug_line` and 6% of `.debug_info`, about 29 MB of the
uncompressed executable, with no change in compile time. But clang then marks
fewer places in the code as the beginning of a line (`is_stmt`), and 40 lines
lost every place bpcov can use - nearly all of them compound conditions such
as `if (a && b)` - so these lines appeared to have no code. So
`--line-tables` keeps the column numbers.

**A full clean build** of `build/dev/scylla` (measured on 2026-10-30), from
an empty `build/dev`, without any compiler cache (`--compiler-cache=none`
and `CCACHE_DISABLE=1`), counting the user-space instructions of `ninja`
and everything it ran (`perf stat`) - Seastar's library, the Rust code, the
precompiled header, every object and the link:

| | without `--line-tables` | with `--line-tables` | change |
|---|---|---|---|
| instructions | 24.975 T | 26.278 T | +5.2% |
| `build/dev/scylla` | 138,947,448 bytes | 222,983,040 bytes | 1.60 times |
| the 935 object files (`*.o`, not counting Seastar's) | 348,940,144 bytes | 545,276,272 bytes | 1.56 times |
| all of `build/dev` | 1.70 GB | 1.98 GB | +17% |
| relinking `build/dev/scylla` | 20.7 G instructions, 2.25 s | 35.6 G instructions, 2.68 s | +71% instructions |

The instruction count is the reliable measure on this laptop, whose CPU has
two kinds of cores: the elapsed time of the two builds (with
`ninja-memlimit --mem-limit 6G`) was 1426 s without the option and 1392 s
with it, i.e., the difference was smaller than the noise.

### The cost of bpcov

**Preparation** (`bpcov-prepare`) took 0.58 seconds (0.91 seconds with
compressed debug sections), plus 0.3 seconds to compile it the first time. It
reads the line table (11.7 million rows), the inlined calls in `.debug_info`
(9.7 million address ranges) and the source files, and chose 1,578,739
breakpoints for 175,228 lines of Scylla source code with compiled code (a
later version, which also takes runs of rows whose first row isn't marked
`is_stmt`, and moves breakpoints off NOP padding, chose 1,584,563 breakpoints
for 175,566 lines). Its data takes 45 MB. Without leaving out lines of only
closing braces, there were 1,654,721 breakpoints for 190,577 lines: 140,628
lines with code in the line table, and 49,949 that only call inlined
functions; the 15,349 lines of only closing braces were 8% of these lines.
Many breakpoints are in small, much-inlined headers: `utils/managed_bytes.hh`
alone has 75,985 breakpoints in the line table for its 151 lines with code.
An earlier version in Python, which read the output of `llvm-dwarfdump
--debug-line`, took 6.8 seconds to produce exactly the same breakpoints
(before closing braces were left out).

**Boot**, with `test/alternator/run test_health.py`, three runs of each (the
traps are from the boot and this short test, about 205,000-209,000 of them):

| | boot time |
|---|---|
| without the tracer | 0.5, 0.6, 0.5 s |
| tracer with 7 system calls per trap (waitpid, PTRACE_GETREGS, PTRACE_PEEKTEXT, PTRACE_POKETEXT, PTRACE_SETREGS, write of the hit, PTRACE_CONT) | 2.6, 2.4, 2.4 s |
| tracer with 5 system calls per trap (the hits file is mapped into memory, and one pwrite to /proc/PID/mem restores the instruction) | 2.1, 2.2, 2.2 s |

So a trap costs about 8 microseconds, down from 9.4: most of the cost of a
trap is switching from Scylla to the tracer process and back, not the system
calls. Inserting the breakpoints took 0.03 seconds.

**`test/alternator/run`** (the whole suite): with the tracer, Scylla hit
307,535 breakpoints in 324,745 traps (the extra traps are threads reaching a
breakpoint at the same time). The tests took 64.94 seconds; without the
tracer, they took between 63.9 and 70.8 seconds in different runs. The
results were the same (1528 passed, 123 skipped, 213 xfailed). The suite
executed 6,861 of the 8,163 lines with code in `alternator/` (84.0%). The
boot took 3.0 seconds, because this was the first run of the build and
included the preparation; before closing braces were left out, the boot of
such a run took 2.2 seconds without preparation, with 317,332 breakpoints
hit in 337,746 traps, and the suite executed 7,355 of 8,786 lines (83.7%).

Before bpcov handled the lines of inlined calls (with 750,985 breakpoints in
the line table only), the same run hit 160,105 breakpoints in 166,407 traps,
for 55,966 distinct source lines - so about two thirds of the traps were for
another inlined copy of a line already executed - and the boot took 1.6-1.9
seconds. In two runs one after the other, the tests took 65.78 seconds with
the tracer and 65.79 seconds without it.

`test/cqlpy/run test_json.py` (measured with an earlier version that had only
line-table breakpoints, and also some in Rust's standard library, 795,592 in
all): Scylla hit 106,024 of them in 111,398 traps, most of them while
booting: the boot took 1.7 seconds instead of 0.4. The tests themselves then
ran at full speed (1.22 seconds, compared to 1.30 and 1.41 seconds in two runs
without the tracer). `report` took 0.25 seconds.

## Files

- `prepare.c`: chooses the breakpoints. It reads the line table
  (`.debug_line`) and the inlined calls (in `.debug_info`) of the executable,
  and writes the breakpoint addresses, and which source lines they belong
  to, to `build/dev/bpcov/`. Its comments explain the DWARF format as much
  as needed to follow the code. It reads DWARF 5 compile units, which is
  what clang produces for Scylla. Debug sections may be compressed (`-gz`)
  with zstd or zlib; it decompresses them with the zstd and zlib libraries,
  which building Scylla requires anyway.
- `tracer.c`: the tracer. A small ptrace-based program that runs a command,
  inserts the breakpoints, and records the hits.
- `bpcov.py`: the `prepare`, `run`, `cat`, `less` and `report` commands. It
  compiles the two C programs into `build/bpcov-prepare` and
  `build/bpcov-tracer` when needed, and needs the Python module `numpy`.
- `scylla`: the wrapper script for `$SCYLLA`.

### The data in `build/dev/bpcov/`

The coverage is kept per breakpoint, not per line, so the data has two
parts: the mapping between breakpoints and source lines, which
`bpcov-prepare` writes once per build, and the hits of each run.

- `info`: the path and build-id of the executable that was prepared, and
  the version of the data format. If either changes, the next command
  prepares the data again, and deletes the hits of the old build.
- `lock`: held while preparing, so that only one process prepares.
- `files.txt`: the Scylla source files that have code, one path (relative
  to the source tree) per line. A file's number is its position in this
  list, starting at 0.
- `rows.bin`: little-endian records of `{uint64_t address; uint32_t file;
  uint32_t line;}`, sorted by address: the address of a breakpoint (as
  linked, before the executable is loaded), the number of a file in
  `files.txt`, and a line number in that file. A line usually has several
  rows, one for each place the compiler put code of that line - for
  example, every copy of an inlined function. An address can also have
  several rows, for example, when it is the beginning of both the callee's
  line and the calling line of an inlined call.
- `breakpoints.bin`: the distinct addresses of `rows.bin`, as sorted
  little-endian `uint64_t`. These are the addresses of the breakpoints.
- `hits/PID.bin`: the hits of one run (PID is the process id of the run),
  with one byte for each breakpoint: byte *i* is the state of the *i*-th
  address in `breakpoints.bin`. The byte is:
  - 0: not hit.
  - 1: hit.
  - 2: unusable - the original byte at this address was already an `int3`,
    so a trap there would not tell whether the line ran. The tracer does
    not insert this breakpoint.
  - 3: hit only in a process that the program forked. The forked process
    has its own copy of the program's memory, so the program itself still
    has this breakpoint, and the tracer must still restore it there if the
    program reaches it; then the byte becomes 1.
- `hits/PID.bin.log`: the tracer's messages for that run.

A line counts as executed if any of its rows' breakpoints is 1 or 3 in any
of the hit files. To find a row's breakpoint, look up its address in
`breakpoints.bin` (with a binary search, as both are sorted).

## Limitations

- x86-64 Linux only.
- Only the main executable is measured, not shared libraries (in dev mode,
  Seastar is a shared library, but it is not Scylla's own code anyway).
- Since the tracer is a ptrace tracer, a debugger such as gdb cannot attach
  to Scylla while it runs under bpcov.
- For the same reason, in builds with AddressSanitizer (debug and sanitize
  modes), LeakSanitizer cannot check for leaks when the program exits: it
  needs ptrace itself, so it reports "LeakSanitizer has encountered a fatal
  error" and the exit status becomes 1. The Scylla server is not affected,
  because it always ends with `_exit()`, which skips LeakSanitizer's check.
  But a program that exits normally, such as Scylla's tools (`scylla
  sstable` and the like), would fail this way if run under the tracer.
- Only the Scylla process started by the test runner through
  `scripts/bpcov/scylla` is measured. Tests that run Scylla's tools (for
  example, `test/cqlpy/test_tools.py`) find the executable through the
  running server's `/proc/PID/exe`, which is the real executable, not the
  wrapper, so the code these tools run is not measured.

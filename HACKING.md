# Guidelines for developing Scylla

This document is intended to help developers and contributors to Scylla get started. The first part consists of general guidelines that make no assumptions about a development environment or tooling. The second part describes a particular environment and work-flow for exemplary purposes.

## Overview

This section covers some high-level information about the Scylla source code and work-flow.

### Getting the source code

Scylla uses [Git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules) to manage its dependency on Seastar and other tools. Be sure that all submodules are correctly initialized when cloning the project:

```bash
$ git clone https://github.com/scylladb/scylla
$ cd scylla
$ git submodule update --init --recursive
```

### Dependencies

Scylla is fairly fussy about its build environment, requiring a very recent
version of the C++23 compiler and numerous tools and libraries to build.

Run `./install-dependencies.sh` (as root) to use your Linux distributions's
package manager to install the appropriate packages on your build machine.
However, this will only work on very recent distributions. For example,
currently Fedora users must upgrade to Fedora 32 otherwise the C++ compiler
will be too old, and not support the new C++23 standard that Scylla uses.

Alternatively, to avoid having to upgrade your build machine or install
various packages on it, we provide another option - the **frozen toolchain**.
This is a script, `./tools/toolchain/dbuild`, that can execute build or run
commands inside a container that contains exactly the right build tools and
libraries. The `dbuild` technique is useful for beginners, but is also the way
in which ScyllaDB produces official releases, so it is highly recommended.

To use `dbuild`, you simply prefix any build or run command with it. Building
and running Scylla becomes as easy as:

```bash
$ ./tools/toolchain/dbuild ./configure.py
$ ./tools/toolchain/dbuild ninja build/release/scylla
$ ./tools/toolchain/dbuild ./build/release/scylla --developer-mode 1
```

Note: do not mix environemtns - either perform all your work with dbuild, or natively on the host.
Note2: you can get to an interactive shell within dbuild by running it without any parameters:
```bash
$ ./tools/toolchain/dbuild
```

### Build system

**Note**: Compiling Scylla requires, conservatively, 2 GB of memory per native
thread, and up to 3 GB per native thread while linking. GCC >= 10 is
required.

Scylla is built with [Ninja](https://ninja-build.org/), a low-level rule-based system. A Python script, `configure.py`, generates a Ninja file (`build.ninja`) based on configuration options.

To build for the first time:

```bash
$ ./configure.py
$ ninja-build
```

Afterwards, it is sufficient to just execute Ninja.

The full suite of options for project configuration is available via

```bash
$ ./configure.py --help
```

The most important option is:

- `--enable-dpdk`: [DPDK](http://dpdk.org/) is a set of libraries and drivers for fast packet processing. During development, it's not necessary to enable support even if it is supported by your platform.

Source files and build targets are tracked manually in `configure.py`, so the script needs to be updated when new files or targets are added or removed.

To save time -- for instance, to avoid compiling all unit tests -- you can also specify specific targets to Ninja. For example,

```bash
$ ninja-build build/release/tests/schema_change_test
$ ninja-build build/release/service/storage_proxy.o
```

You can also specify a single mode. For example

```bash
$ ninja-build release
```

Will build everytihng in release mode. The valid modes are

* Debug: Enables [AddressSanitizer](https://github.com/google/sanitizers/wiki/AddressSanitizer)
  and other sanity checks. It has no optimizations, which allows for debugging with tools like
  GDB. Debugging builds are generally slower and generate much larger object files than release builds.
* Release: Fewer checks and more optimizations. It still has debug info.
* Dev: No optimizations or debug info. The objective is to compile and link as fast as possible.
  This is useful for the first iterations of a patch.


Note that by default unit tests binaries are stripped so they can't be used with gdb or seastar-addr2line.
To include debug information in the unit test binary, build the test binary with a `_g` suffix. For example,

```bash
$ ninja-build build/release/tests/schema_change_test_g
```

### Unit testing

Unit tests live in the `/tests` directory. Like with application source files, test sources and executables are specified manually in `configure.py` and need to be updated when changes are made.

A test target can be any executable. A non-zero return code indicates test failure.

Most tests in the Scylla repository are built using the [Boost.Test](http://www.boost.org/doc/libs/1_64_0/libs/test/doc/html/index.html) library. Utilities for writing tests with Seastar futures are also included.

Run all tests through the test execution wrapper with

```bash
$ ./test.py --mode={debug,release}
```

or, if you are using `dbuild`, you need to build the code and the tests and then you can run them at will:

```bash
$ ./tools/toolchain/dbuild ninja {debug,release,dev}-build
$ ./tools/toolchain/dbuild ./test.py --mode {debug,release,dev}
```

The `--name` argument can be specified to run a particular test.

Alternatively, you can execute the test executable directly. For example,

```bash
$ build/release/tests/row_cache_test -- -c1 -m1G
```

The `-c1 -m1G` arguments limit this Seastar-based test to a single system thread and 1 GB of memory.

### Preparing patches

All changes to Scylla are submitted as patches to the public [mailing list](mailto:scylladb-dev@googlegroups.com). Once a patch is approved by one of the maintainers of the project, it is committed to the maintainers' copy of the repository at https://github.com/scylladb/scylla.

Detailed instructions for formatting patches for the mailing list and advice on preparing good patches are available at the [ScyllaDB website](http://docs.scylladb.com/contribute/). There are also some guidelines that can help you make the patch review process smoother:

1. Before generating patches, make sure your Git configuration points to `.gitorderfile`. You can do it by running

```bash
$ git config diff.orderfile .gitorderfile
```

2. If you are sending more than a single patch, push your changes into a new branch of your fork of Scylla on GitHub and add a URL pointing to this branch to your cover letter.

3. If you are sending a new revision of an earlier patchset, add a brief summary of changes in this version, for example:
```
In v3:
    - declared move constructor and move assignment operator as noexcept
    - used std::variant instead of a union
    ...
```

4. Add information about the tests run with this fix. It can look like
```
"Tests: unit ({mode}), dtest ({smp})"
```

The usual is "Tests: unit (dev)", although running debug tests is encouraged.

5. When answering review comments, prefer inline quotes as they make it easier to track the conversation across multiple e-mails.

6. The Linux kernel's [Submitting Patches](https://www.kernel.org/doc/html/v4.19/process/submitting-patches.html) document offers excellent advice on how to prepare patches and patchsets for review. Since the Scylla development process is derived from the kernel's, almost all of the advice there is directly applicable.

### Finding a person to review and merge your patches

You can use the `scripts/find-maintainer` script to find a subsystem maintainer and/or reviewer for your patches. The script accepts a filename in the git source tree as an argument and outputs a list of subsystems the file belongs to and their respective maintainers and reviewers. For example, if you changed the `cql3/statements/create_view_statement.hh` file, run the script as follows:

```bash
$ ./scripts/find-maintainer cql3/statements/create_view_statement.hh
```

and you will get output like this:

```
CQL QUERY LANGUAGE
  Tomasz Grabiec <tgrabiec@scylladb.com>   [maintainer]
MATERIALIZED VIEWS
  Nadav Har'El <nyh@scylladb.com>          [reviewer]
```

### Running Scylla

Once Scylla has been compiled, executing the (`debug` or `release`) target will start a running instance in the foreground:

```bash
$ build/release/scylla
```

The `scylla` executable requires a configuration file, `scylla.yaml`. By default, this is read from `$SCYLLA_HOME/conf/scylla.yaml`. A good starting point for development is located in the repository at `/conf/scylla.yaml`.

For development, a directory at `$HOME/scylla` can be used for all Scylla-related files:

```bash
$ mkdir -p $HOME/scylla $HOME/scylla/conf
$ cp conf/scylla.yaml $HOME/scylla/conf/scylla.yaml
$ # Edit configuration options as appropriate
$ SCYLLA_HOME=$HOME/scylla build/release/scylla
```

The `scylla.yaml` file in the repository by default writes all database data to `/var/lib/scylla`, which likely requires root access. Change the `data_file_directories`, `commitlog_directory` and `schema_commitlog_directory` fields as appropriate.

Scylla has a number of requirements for the file-system and operating system to operate ideally and at peak performance. However, during development, these requirements can be relaxed with the `--developer-mode` flag.

Additionally, when running on under-powered platforms like portable laptops, the `--overprovisioned` flag is useful.

On a development machine, one might run Scylla as

```bash
$ SCYLLA_HOME=$HOME/scylla build/release/scylla --overprovisioned --developer-mode=yes
```

To interact with scylla it is recommended to build our versions of
cqlsh and nodetool. They are available at
https://github.com/scylladb/scylla-tools-java and can be built with

```bash
$ sudo ./install-dependencies.sh
$ ant jar
```

cqlsh should work out of the box, but nodetool depends on a running
scylla-jmx (https://github.com/scylladb/scylla-jmx). It can be build
with

```bash
$ mvn package
```

and must be started with

```bash
$ ./scripts/scylla-jmx
```

### Branches and tags

Multiple release branches are maintained on the Git repository at https://github.com/scylladb/scylla. Release 1.5, for instance, is tracked on the `branch-1.5` branch.

Similarly, tags are used to pin-point precise release versions, including hot-fix versions like 1.5.4. These are named `scylla-1.5.4`, for example.

Most development happens on the `master` branch. Release branches are cut from `master` based on time and/or features. When a patch against `master` fixes a serious issue like a node crash or data loss, it is backported to a particular release branch with `git cherry-pick` by the project maintainers.

## Example: development on Fedora 25

This section describes one possible work-flow for developing Scylla on a Fedora 25 system. It is presented as an example to help you to develop a work-flow and tools that you are comfortable with.

### Preface

This guide will be written from the perspective of a fictitious developer, Taylor Smith.

### Git work-flow

Having two Git remotes is useful:

- A public clone of Seastar (`"public"`)
- A private clone of Seastar (`"private"`) for in-progress work or work that is not yet ready to share

The first step to contributing a change to Scylla is to create a local branch dedicated to it. For example, a feature that fixes a bug in the CQL statement for creating tables could be called `ts/cql_create_table_error/v1`. The branch name is prefaced by the developer's initials and has a suffix indicating that this is the first version. The version suffix is useful when branches are shared publicly and changes are requested on the mailing list. Having a branch for each version of the patch (or patch set) shared publicly makes it easier to reference and compare the history of a change.

Setting the upstream branch of your development branch to `master` is a useful way to track your changes. You can do this with

```bash
$ git branch -u master ts/cql_create_table_error/v1
```

As a patch set is developed, you can periodically push the branch to the private remote to back-up work.

Once the patch set is ready to be reviewed, push the branch to the public remote and prepare an email to the `scylladb-dev` mailing list. Including a link to the branch on your public remote allows for reviewers to quickly test and explore your changes.

### Development environment and source code navigation

Scylla uses [CMake](https://cmake.org/) and ninja as its building system. This allows for seamless
integration with any IDE or tool that supports CMake projects or can utilize `compile_commands.json`.

[CLion](https://www.jetbrains.com/clion/) is a commercial IDE offers reasonably good source code navigation and advice for code hygiene, though its C++ parser sometimes makes errors and flags false issues.

Other good options that directly parse CMake files are [KDevelop](https://www.kdevelop.org/) and [QtCreator](https://wiki.qt.io/Qt_Creator).

[Eclipse](https://eclipse.org/cdt/) is another open-source option. It doesn't natively work with CMake projects, and its C++ parser has many similar issues as CLion.

### Distributed compilation: `distcc` and `ccache`

Scylla's compilations times can be long. Two tools help somewhat:

- [ccache](https://ccache.samba.org/) caches compiled object files on disk and reuses them when possible
- [distcc](https://github.com/distcc/distcc) distributes compilation jobs to remote machines

A reasonably-powered laptop acts as the coordinator for compilation. A second, more powerful, machine acts as a passive compilation server.

Having a direct wired connection between the machines ensures that object files can be transmitted quickly and limits the overhead of remote compilation.
The coordinator has been assigned the static IP address `10.0.0.1` and the passive compilation machine has been assigned `10.0.0.2`.

On Fedora, installing the `ccache` package creates symbolic links for `gcc` and `g++` in your `PATH`. This enables transparent compilation caching on the local filesystem without any additional configuration. For explicit configuration or when using other build systems, tell CMake to use ccache by setting these environment variables before running `configure.py` or `cmake`: 

```bash
export CMAKE_CXX_COMPILER_LAUNCHER=/usr/bin/ccache
export CMAKE_C_COMPILER_LAUNCHER=/usr/bin/ccache
```

Next, set `CCACHE_PREFIX` so that `ccache` is responsible for invoking `distcc` as necessary:

```bash
export CCACHE_PREFIX="distcc"
```

On each host, edit `/etc/sysconfig/distccd` to include the allowed coordinators and the total number of jobs that the machine should accept.
This example is for the laptop, which has 2 physical cores (4 logical cores with hyper-threading):

```
OPTIONS="--allow 10.0.0.2 --allow 127.0.0.1 --jobs 4"
```

`10.0.0.2` has 8 physical cores (16 logical cores) and 64 GB of memory.

As a rule-of-thumb, the number of jobs that a machine should be specified to support should be equal to the number of its native threads.

Restart the `distccd` service on all machines.

On the coordinator machine, edit `$HOME/.distcc/hosts` with the available hosts for compilation. Order of the hosts indicates preference.

```
10.0.0.2/16 localhost/2
```

In this example, `10.0.0.2` will be sent up to 16 jobs and the local machine will be sent up to 2. Allowing for two extra threads on the host machine for coordination, we run compilation with `16 + 2 + 2 = 20` jobs in total: `ninja-build -j20`.

When a compilation is in progress, the status of jobs on all remote machines can be visualized in the terminal with `distccmon-text` or graphically as a GTK application with `distccmon-gnome`.

One thing to keep in mind is that linking object files happens on the coordinating machine, which can be a bottleneck. See the next sections speeding up this process.

### Using the `gold` linker

Linking Scylla can be slow. The gold linker can replace GNU ld and often speeds the linking process. On Fedora, you can switch the system linker using

```bash
$ sudo alternatives --config ld
```

### Using split dwarf

With debug info enabled, most of the link time is spent copying and
relocating it. It is possible to leave most of the debug info out of
the link by writing it to a side .dwo file. This is done by passing
`-gsplit-dwarf` to gcc.

Unfortunately just `-gsplit-dwarf` would slow down `gdb` startup. To
avoid that the gold linker can be told to create an index with
`--gdb-index`.

More info at https://gcc.gnu.org/wiki/DebugFission.

Both options can be enable by passing `--split-dwarf` to configure.py.

Note that distcc is *not* compatible with it, but icecream
(https://github.com/icecc/icecream) is.

### Testing changes in Seastar with Scylla

Sometimes Scylla development is closely tied with a feature being developed in Seastar. It can be useful to compile Scylla with a particular check-out of Seastar.

One way to do this it to create a local remote for the Seastar submodule in the Scylla repository:

```bash
$ cd $HOME/src/scylla
$ cd seastar
$ git remote add local /home/tsmith/src/seastar
$ git remote update
$ git checkout -t local/my_local_seastar_branch
```

### Generating code coverage report

Install dependencies:

    $ dnf install llvm # for llvm-profdata and llvm-cov
    $ dnf install lcov # for genhtml

Instruct `configure.py` to generate build files for `coverage` mode:

    $ ./configure.py --mode=coverage

Build the tests you want to run, then run them via `test.py` (important!):

    $ ./test.py --mode=coverage [...]

Alternatively, you can run individual tests via `./scripts/coverage.py --run`.

Open the link printed at the end. Be horrified. Go and write more tests.

For more details see `./scripts/coverage.py --help`.

### Resolving stack backtraces

Scylla may print stack backtraces to the log for several reasons.
For example:
- When aborting (e.g. due to assertion failure, internal error, or segfault)
- When detecting seastar reactor stalls (where a seastar task runs for a long time without yielding the cpu to other tasks on that shard)

The backtraces contain code pointers so they are not very helpful without resolving into code locations.
To resolve the backtraces, one needs the scylla relocatable package that contains the scylla binary (with debug information),
as well as the dynamic libraries it is linked against.

Builds from our automated build system are uploaded to the cloud
and can be searched on http://backtrace.scylladb.com/

Make sure you have the scylla server exact `build-id` to locate
its respective relocatable package, required for decoding backtraces it prints.

The build-id is printed to the system log when scylla starts.
It can also be found by executing `scylla --build-id`, or
by using the `file` utility, for example:
```
$ scylla --build-id
4cba12e6eb290a406bfa4930918db23941fd4be3

$ file scylla
scylla: ELF 64-bit LSB executable, x86-64, version 1 (SYSV), dynamically linked, interpreter /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////lib64/ld-linux-x86-64.so.2, for GNU/Linux 3.2.0, BuildID[sha1]=4cba12e6eb290a406bfa4930918db23941fd4be3, with debug_info, not stripped, too many notes (256)
```

To find the build-id of a coredump, use the `eu-unstrip` utility as follows:
```
$ eu-unstrip -n --core <coredump> | awk '/scylla$/ { s=$2; sub(/@.*$/, "", s); print s; exit(0); }'
4cba12e6eb290a406bfa4930918db23941fd4be3
```

### Core dump debugging

See [debugging.md](docs/dev/debugging.md).

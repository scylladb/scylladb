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

### Build system

Scylla is built with [Ninja](https://ninja-build.org/), a low-level rule-based system. A Python script, `configure.py`, generates a Ninja file (`build.ninja`) based on configuration options.

The full suite of options for project configuration is available via

```bash
$ ./configure.py --help
```

The most important options are:

- `--mode={release,debug,all}`: Debug mode enables [AddressSanitizer](https://github.com/google/sanitizers/wiki/AddressSanitizer) and allows for debugging with tools like GDB. Debugging builds are generally slower and generate much larger object files than release builds.

- `--with={scylla,tests/schema_registry_test,tests,keys_test, ...}`: Build only the selected targets. Multiple occurrances of `--with` are supported. Scylla's tests add considerable compilation time, so only building a target of interest is often a time-saver.

- `--{enable,disable}-dpdk`: [DPDK](http://dpdk.org/) is a set of libraries and drivers for fast packet processing. During development, it's not necessary to enable support even if it is supported by your platform.

The configuration script will also ensure that you have all necessary system dependencies installed.

Source files and build targets are tracked manually in `configure.py`, so the script needs to be updated when new files or targets are added or removed.

As an alternative to configuring only select targets with the `--with` argument to `configure.py`, you can also specify a specific target to Ninja. For example,

```bash
$ ninja-build build/release/tests/schema_change_test
```

### Unit testing

Unit tests live in the `/tests` directory. Like with application source files, test sources and executables are specified manually in `configure.py` and need to be updated when changes are made.

A test target can be any executable. A non-zero return code indicates test failure.

Most tests in the Scylla repository are built using the [Boost.Test](http://www.boost.org/doc/libs/1_64_0/libs/test/doc/html/index.html) library. Utilities for writing tests with Seastar futures are also included.

### Preparing patches

All changes to Scylla are submitted as patches to the public mailing list. Once a patch is approved by one of the maintainers of the project, it is committed to the maintainers' copy of the repository at https://github.com/scylladb/scylla.

Detailed instructions for formatting patches for the mailing list and advice on preparing good patches are available at the [ScyllaDB website](http://docs.scylladb.com/kb/contributing/).

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

The `scylla.yaml` file in the repository by default writes all database data to `/var/lib/scylla`, which likely requires root access. Change the `data_file_directories` and `commitlog_directory` fields as appropriate.

Scylla has a number of requirements for the file-system and operating system to operate ideally and at peak performance. However, during development, these requirements can be relaxed with the `--developer-mode` flag.

Additionally, when running on under-powered platforms like portable laptops, the `--overprovisioned` flag is useful.

On a development machine, one might run Scylla as

```bash
$ SCYLLA_HOME=$HOME/scylla build/release/scylla --overprovisioned --developer-mode=yes
```

### Branches and tags

**TODO:** (Describe how releases are branched from `master` and how patches make their way into release series'. Describe tagging conventions.)

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

Scylla includes a [CMake](https://cmake.org/) file, `CMakeLists.txt`, for use only with [CLion](https://www.jetbrains.com/clion/): a commercial C++ development environment. CLion offers reasonably good source code navigation and advice for code hygiene, though its C++ parser sometimes makes errors and flags false issues. When new source or header files are added, it's important to reload the `CMake` configuration.

[Eclipse](https://eclipse.org/cdt/) is a free development environment that also works well with Scylla.

### Distributed compilation: `distcc` and `ccache`

Scylla's compilations times can be long. Two tools help somewhat:

- [ccache](https://ccache.samba.org/) caches compiled object files on disk and re-uses them when possible
- [distcc](https://github.com/distcc/distcc) distributes compilation jobs to remote machines

A reasonably-powered laptop acts as the coordinator for compilation. A second, more powerful, machine acts as a passive compilation server.

Having a direct wired connection between the machines ensures that object files can be transmitted quickly and limits the overhead of remote compilation.
The coordinator has been assigned the static IP address `10.0.0.1` and the passive compilation machine has been assigned `10.0.0.2`.

First, set up symbolic links for `ccache`. The `PATH` includes `$HOME/bin` on this machine:

```
$ ls $HOME/bin
total 0
lrwxrwxrwx 1 tsmith tsmith 15 Jun 16 13:27 g++ -> /usr/bin/ccache
lrwxrwxrwx 1 tsmith tsmith 15 Jul  5 10:43 gcc -> /usr/bin/ccache
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

`10.0.0.2` has 8 physical cores (16 logical cores) and 64 GB of memory. It can usually handle 40 jobs.

Restart the `distccd` service on all machines.

On the coordinator machine, edit `$HOME/.distcc/hosts` with the available hosts for compilation. Order of the hosts indicates preference.

```
10.0.0.2/40 localhost/2
```

In this example, `10.0.0.2` will be sent up to 40 jobs and the local machine will be sent up to 2. Since we've allowed for 42 jobs in total, run `ninja-build -j42`.

When a compilation is in progress, the status of jobs on all remote machines can be visualized in the terminal with `distccmon-text` or graphically as a GTK application with `distccmon-gnome`.

One thing to keep in mind is that linking object files happens on the coordinating machine, which can be a bottleneck. See the next section speeding up this process.

### Using the `gold` linker

Linking Scylla can be slow. The gold linker can replace GNU ld and often speeds the linking process. On Fedora, you can switch the system linker using

```bash
$ sudo alternatives --config ld
```

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

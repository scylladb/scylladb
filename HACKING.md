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

Scylla depends on the system package manager for its development dependencies.

Running `./install-dependencies.sh` (as root) installs the appropriate packages based on your Linux distribution.

### Build system

**Note**: Compiling Scylla requires, conservatively, 2 GB of memory per native thread, and up to 3 GB per native thread while linking.

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

The most important options are:

- `--mode={release,debug,all}`: Debug mode enables [AddressSanitizer](https://github.com/google/sanitizers/wiki/AddressSanitizer) and allows for debugging with tools like GDB. Debugging builds are generally slower and generate much larger object files than release builds.

- `--{enable,disable}-dpdk`: [DPDK](http://dpdk.org/) is a set of libraries and drivers for fast packet processing. During development, it's not necessary to enable support even if it is supported by your platform.

Source files and build targets are tracked manually in `configure.py`, so the script needs to be updated when new files or targets are added or removed.

To save time -- for instance, to avoid compiling all unit tests -- you can also specify specific targets to Ninja. For example,

```bash
$ ninja-build build/release/tests/schema_change_test
```

### Unit testing

Unit tests live in the `/tests` directory. Like with application source files, test sources and executables are specified manually in `configure.py` and need to be updated when changes are made.

A test target can be any executable. A non-zero return code indicates test failure.

Most tests in the Scylla repository are built using the [Boost.Test](http://www.boost.org/doc/libs/1_64_0/libs/test/doc/html/index.html) library. Utilities for writing tests with Seastar futures are also included.

Run all tests through the test execution wrapper with

```bash
$ ./test.py --mode={debug,release}
```

The `--name` argument can be specified to run a particular test.

Alternatively, you can execute the test executable directly. For example,

```bash
$ build/release/tests/row_cache_test -- -c1 -m1G
```

The `-c1 -m1G` arguments limit this Seastar-based test to a single system thread and 1 GB of memory.

### Preparing patches

All changes to Scylla are submitted as patches to the public mailing list. Once a patch is approved by one of the maintainers of the project, it is committed to the maintainers' copy of the repository at https://github.com/scylladb/scylla.

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

The usual is "Tests: unit (release)", although running debug tests is encouraged.

5. When answering review comments, prefer inline quotes as they make it easier to track the conversation across multiple e-mails.

### Finding a person to review and merge your patches

You can use the `scripts/find-maintainer` script to find a subsystem maintainer and/or reviewer for your patches. The script accepts a filename in the git source tree as an argument and outputs a list of subsystems the file belongs to and their respective maintainers and reviewers. For example, if you changed the `cql3/statements/create_view_statement.hh` file, run the script as follows:

```bash
$ ./scripts/find-maintainer cql3/statements/create_view_statement.hh
```

and you will get output like this:

```
CQL QUERY LANGUAGE
  Tomasz Grabiec <tgrabiec@scylladb.com>   [maintainer]
  Pekka Enberg <penberg@scylladb.com>      [maintainer]
MATERIALIZED VIEWS
  Pekka Enberg <penberg@scylladb.com>      [maintainer]
  Duarte Nunes <duarte@scylladb.com>       [maintainer]
  Nadav Har'El <nyh@scylladb.com>          [reviewer]
  Duarte Nunes <duarte@scylladb.com>       [reviewer]
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

The `scylla.yaml` file in the repository by default writes all database data to `/var/lib/scylla`, which likely requires root access. Change the `data_file_directories` and `commitlog_directory` fields as appropriate.

Scylla has a number of requirements for the file-system and operating system to operate ideally and at peak performance. However, during development, these requirements can be relaxed with the `--developer-mode` flag.

Additionally, when running on under-powered platforms like portable laptops, the `--overprovisined` flag is useful.

On a development machine, one might run Scylla as

```bash
$ SCYLLA_HOME=$HOME/scylla build/release/scylla --overprovisioned --developer-mode=yes
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

Scylla includes a [CMake](https://cmake.org/) file, `CMakeLists.txt`, for use only with development environments (not for building) so that they can properly analyze the source code.

[CLion](https://www.jetbrains.com/clion/) is a commercial IDE offers reasonably good source code navigation and advice for code hygiene, though its C++ parser sometimes makes errors and flags false issues.

Other good options that directly parse CMake files are [KDevelop](https://www.kdevelop.org/) and [QtCreator](https://wiki.qt.io/Qt_Creator).

To use the `CMakeLists.txt` file with these programs, define the `FOR_IDE` CMake variable or shell environmental variable.

[Eclipse](https://eclipse.org/cdt/) is another open-source option. It doesn't natively work with CMake projects, and its C++ parser has many similar issues as CLion.

### Distributed compilation: `distcc` and `ccache`

Scylla's compilations times can be long. Two tools help somewhat:

- [ccache](https://ccache.samba.org/) caches compiled object files on disk and re-uses them when possible
- [distcc](https://github.com/distcc/distcc) distributes compilation jobs to remote machines

A reasonably-powered laptop acts as the coordinator for compilation. A second, more powerful, machine acts as a passive compilation server.

Having a direct wired connection between the machines ensures that object files can be transmitted quickly and limits the overhead of remote compilation.
The coordinator has been assigned the static IP address `10.0.0.1` and the passive compilation machine has been assigned `10.0.0.2`.

On Fedora, installing the `ccache` package places symbolic links for `gcc` and `g++` in the `PATH`. This allows normal compilation to transparently invoke `ccache` for compilation and cache object files on the local file-system.

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

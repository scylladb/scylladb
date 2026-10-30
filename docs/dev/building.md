# Building Scylla

This document describes how to build Scylla's executables, tests, and packages.

*Please note that these instructions use `dbuild` -- a Docker-based development environment -- to build Scylla.
However, `dbuild` is optional and you can also build on your host machine by running the same commands without the `dbuild` prefix.*

## Table of Contents

* [Getting Started](#getting-started)
* [Configuring](#configuring)
* [Building](#building)
* [Switching between branches](#switching-between-branches)
* [Packaging](#packaging)
* [Artifacts](#artifacts)

## Getting Started

To build everything, run:

```console
git clone https://github.com/scylladb/scylla
cd ./scylla
git submodule update --init --force --recursive
./tools/toolchain/dbuild ./configure.py --mode=<mode>
./tools/toolchain/dbuild ninja
```

where `mode` is `dev` for development builds, `release` for release builds, and `debug` for debug builds.

Alternatively, you can also pass the `mode` directly to the `ninja` command:

```console
git submodule update --init --force --recursive
./tools/toolchain/dbuild ./configure.py
./tools/toolchain/dbuild ninja <mode>
```

After the build completes, you can find build artifacts in `build/<mode>` directory.

You can run unit tests with:

```console
./tools/toolchain/dbuild ninja test
```

or launch a Scylla server locally with:

```console
./tools/toolchain/dbuild ./build/dev/scylla --workdir tmp --smp 1 --memory 1G
```

For more help with build targets, run:

```console
./tools/toolchain/dbuild ninja help
```

That's it!

## Configuring

The `configure.py` script, which is run as the first step of a build, generates a `build.ninja` build file (similar to a `Makefile`) for the [Ninja] build tool that we use for building.

To configure the build system for a specific build mode, run:

```console
./tools/toolchain/dbuild ./configure.py --mode=<mode>
```

The `mode` flag determines build flags for the `scylla` executable:

* `release` is the release build that targets fast execution time (slow build, fast execution).
* `dev` is the development build that targets fast build times (fast build, reasonable execution)
* `debug` is the debug build that targets error discovery (slow build, slow execution).

If you don't specify a build mode, the `build.ninja` will contain configuration for _all build modes_.

[Ninja]: https://ninja.org/

## Building

To build Scylla executables and tests, run:

```console
./tools/toolchain/dbuild ninja build
```

This will build Scylla for the build mode specified by the `configure.py` command.

Alternatively, if you did not specify a build mode in the `configure.py` step, you can build executables and tests for a specific build mode with:

```console
./tools/toolchain/dbuild ninja <mode>-build
```

## Switching between branches

A checkout is often reused for several branches, for example `master` and
one or more release branches (`branch-X.Y`). After switching, the build tree
still carries the previous branch's configuration, so before building:

1. Sync the submodules with the new baseline:

   ```console
   git submodule sync --recursive
   git submodule update --init --recursive
   ```

2. Remove `build/<mode>/seastar/CMakeCache.txt`. It caches where Seastar found
   fmt, and that differs between branches: `master` builds fmt from the `fmt/`
   submodule, while older release branches use the system fmt. A stale cache
   breaks the build in both directions: compile errors such as
   `explicit specialization of fmt::formatter<...> after instantiation` or
   `redefinition of formatter<exception_ptr>` when building a release branch
   after `master`, or a link error `undefined symbol: ... fmt::v12 ...` when
   building `master` after a release branch.

3. Re-run `configure.py` with the arguments recorded on the first line of
   `build.ninja` (`configure_args = ...`), in the same environment as the
   original build: under `./tools/toolchain/dbuild` for a Docker build, on
   the host for a native one. Configuring on the host records host compiler
   paths that a later container build cannot use. `build.ninja` is generated
   per branch: source lists, include paths and which test files form their
   own binary differ.

4. Build, then confirm that the binary is the one you think it is before
   trusting any test result, again in the environment it was built in:

   ```console
   ./tools/toolchain/dbuild ./build/<mode>/scylla --version   # Docker build
   ./build/<mode>/scylla --version                            # native build
   git rev-parse HEAD
   ```

Untracked leftovers from other branches, such as a `fmt/` or `c-ares/`
directory, are harmless once the cache is reset and need not be deleted.

Some notes specific to `dbuild`:

* `dbuild` selects its toolchain image from the `image` file next to it, so
  always use the `dbuild` of the tree you are building. A sibling checkout on
  another branch has another toolchain.
* Run the built binaries under `dbuild` as well, including the perf
  microbenchmarks; the host usually lacks the toolchain's library versions.
* Older toolchain images (e.g. the `branch-2025.1` one) may fail every compile
  with `ccache: error: Permission denied`, because `ccache` defaults to a
  directory that is not mounted into the container. Either disable it for the
  build with `dbuild -e CCACHE_DISABLE=1 -- ninja ...`, or mount a ccache
  directory via `SCYLLADB_DBUILD` (see `dbuild --help`).
* `dbuild` runs with the host network, so a `dbuild` in another checkout can
  own the sccache server port. If hashing fails with sccache errors, pass
  `-e SCCACHE_SERVER_PORT=<free port>` to `dbuild`.

## Testing

To run unit tests, run:

```console
./tools/toolchain/dbuild ninja test
```

Alternatively, to run unit tests for a specific build mode, run:

```console
./tools/toolchain/dbuild ninja <dev>-test
```

## Packaging

The build system generates _relocatable packages_, which means that the packages contain all the dependencies they need, and you can, therefore, install and run the same binaries on all Linux distributions.
The relocatable package tarball is used as a base for building the Linux distribution specific packages, `.rpm`s and `.deb`s.

To build packages, run:

```console
./tools/toolchain/dbuild ninja dist
```

Alternatively, to build packages for a specific build mode, run:

```console
./tools/toolchain/dbuild ninja <mode>-dist
```

To verify the packages, run:

```console
ninja dist-check
```

*Please note that you need to run `dist-check` on the host because it requires Docker to perform the verification.*

## Artifacts

This section lists all the artifacts generated by the build system.

**Executables**:

* `build/<mode>/scylla`

**Tests**:

* `build/<mode>/test/**`

**Tarballs**:

* `build/<mode>/dist/tar/scylla-unified-package.tar.gz`
* `build/<mode>/dist/tar/scylla-package.tar.gz`
* `build/<mode>/dist/tar/scylla-python3-package.tar.gz`
* `build/<mode>/dist/tar/scylla-jmx-package.tar.gz`
* `build/<mode>/dist/tar/scylla-tools-package.tar.gz`

**.rpms:**

* `build/dist/<mode>/redhat/RPMS/x86_64/scylla-*.rpm`
* `build/dist/<mode>/redhat/RPMS/x86_64/scylla-conf-*.rpm`
* `build/dist/<mode>/redhat/RPMS/x86_64/scylla-debuginfo-*.rpm`
* `build/dist/<mode>/redhat/RPMS/x86_64/scylla-kernel-conf-*.rpm`
* `build/dist/<mode>/redhat/RPMS/x86_64/scylla-node-exporter-*.rpm`
* `build/dist/<mode>/redhat/RPMS/x86_64/scylla-server-*.rpm`
* `tools/python3/build/redhat/RPMS/x86_64/scylla-python3-*.rpm`

**.debs:**

* `build/dist/<mode>/debian/scylla-conf_*.deb`
* `build/dist/<mode>/debian/scylla_*.deb`
* `build/dist/<mode>/debian/scylla-server_*.deb`
* `build/dist/<mode>/debian/scylla-kernel-conf_*.deb`
* `build/dist/<mode>/debian/scylla-node-exporter_*.deb`
* `build/dist/<mode>/debian/scylla-server-dbg_*.deb`
* `tools/python3/build/debian/scylla-python3_*.deb`

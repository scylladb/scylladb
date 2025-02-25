# Building Scylla

This document describes how to build Scylla's executables, tests, and packages.

*Please note that these instructions use `dbuild` -- a Docker-based development environment -- to build Scylla.
However, `dbuild` is optional and you can also build on your host machine by running the same commands without the `dbuild` prefix.*

## Table of Contents

* [Getting Started](#getting-started)
* [Configuring](#configuring)
* [Building](#building)
* [Packaging](#packaging)
* [Artifacts](#artifacts)

## Getting Started

To build everything, run:

```console
git clone https://github.com/scylladb/scylla
cd ./scylla
git submodule update --init --force --recursive
./tools/toolchain/dbuild ./configure.py --mode=<mode>
./tools/toolchain/dbuild ninja -C build
```

where `mode` is `dev` for development builds, `release` for release builds, and `debug` for debug builds.

After the build completes, you can find build artifacts in `build` directory.

You can run unit tests with:

```console
./tools/toolchain/dbuild ninja -C build test
```

or launch a Scylla server locally with:

```console
./tools/toolchain/dbuild ./build/Dev/scylla --workdir tmp --smp 1 --memory 1G
```

For more help with build targets, run:

```console
./tools/toolchain/dbuild cmake --build build --target help
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

In the CMake-based building system, the build modes are mapped to the configuration names:

|  mode      | config           |
| ---------- | ---------------- |
| `dev`      | `Dev`            |
| `release`  | `RelWithDebInfo` |
| `debug`    | `Debug`          |
| `sanitize` | `Sanitize`       |
| `coverage` | `Coverage`       |

## Building

To build Scylla executables and tests, run:

```console
./tools/toolchain/dbuild ninja -C build
```

This will build Scylla for the build mode specified by the `configure.py` command.

Alternatively, if you did not specify a build mode in the `configure.py` step, you can build executables and tests for a specific config with:

```console
./tools/toolchain/dbuild cmake --build build --config <config>
```

Instead of using `cmake`, you can build using `ninja` directly with the notation of
`<target>:<config>`:

```console
./tools/toolchain/dbuild ninja -C build tests:<config>
```

The above command build all tests. To build a single test

```console
./tools/toolchain/dbuild ninja -C build <test_name>:<config>
```

and the built test is located at `build/<test_directory>/<config>/<test_name>`.
take `big_decimal_test` for an example

```console
./tools/toolchain/dbuild ninja -C build big_decimal_test:Debug
```

And the executable of this test is located at `build/test/boost/Debug/big_decimal_test`.

## Testing

To execute the complete test suite:

```console
./tools/toolchain/dbuild ninja test
```

To run tests for specific configuration:

```console
./tools/toolchain/dbuild cmake --build build --config <config> --target test
```

or

```console
./tools/toolchain/dbuild ninja -C build test:<config>
```

## Packaging

The build system generates _relocatable packages_, which means that the packages contain all the dependencies they need, and you can, therefore, install and run the same binaries on all Linux distributions.
The relocatable package tarball is used as a base for building the Linux distribution specific packages, `.rpm`s and `.deb`s.

To build packages, run:

```console
./tools/toolchain/dbuild ninja dist
```

To build packages for a specific configuration:

```console
./tools/toolchain/dbuild ninja dist:<config>
```

To verify the packages:

```console
ninja dist-check
```

> [!NOTE]
> Please note that you need to run `dist-check` on the host because it requires Docker to perform the verification.*

## Artifacts

The build system generates the following artifacts:

**Main Executable**:

* `build/<config>/scylla`

**Tests**:

* `build/<test_directory>/<config>/**`

The test directory path matches the relative path of the test's `CMakeLists.txt`.

The build directory structure:

```
scylla/
├── CMakeLists.txt           # Builds scylla
├── test/
│   ├── CMakeLists.txt
│   └── boost
│       └── CMakeLists.txt   # Builds unit tests
└── build/
    ├── CMakeFiles/
    │   └── scylla.dir/
    │       ├── Debug/
    │       │   └── main.cc.o
    │       └── Dev/
    │           └── main.cc.o
    └── test/
        └── boost/
            ├── Debug/
            │   └── big_decimal_test
            ├── Dev/
            │   └── big_decimal_test
            └── CMakeFiles/
                └── big_decimal_test.dir
                    ├── Debug/
                    │   └── big_decimal_test.cc.o
                    └── Dev/
                        └── big_decimal_test.cc.o

```

**Tarballs**:

* `build/<config>/dist/tar/scylla-unified-package.tar.gz`
* `build/<config>/dist/tar/scylla-package.tar.gz`
* `build/<config>/dist/tar/scylla-python3-package.tar.gz`
* `build/<config>/dist/tar/scylla-jmx-package.tar.gz`
* `build/<config>/dist/tar/scylla-tools-package.tar.gz`

**.rpms:**

* `build/dist/<config>/redhat/RPMS/x86_64/scylla-*.rpm`
* `build/dist/<config>/redhat/RPMS/x86_64/scylla-conf-*.rpm`
* `build/dist/<config>/redhat/RPMS/x86_64/scylla-debuginfo-*.rpm`
* `build/dist/<config>/redhat/RPMS/x86_64/scylla-kernel-conf-*.rpm`
* `build/dist/<config>/redhat/RPMS/x86_64/scylla-node-exporter-*.rpm`
* `build/dist/<config>/redhat/RPMS/x86_64/scylla-server-*.rpm`
* `tools/python3/build/redhat/RPMS/x86_64/scylla-python3-*.rpm`
* `tools/jmx/build/redhat/RPMS/noarch/scylla-jmx-*.rpm`
* `tools/java/build/redhat/RPMS/noarch/scylla-tools-*.rpm`
* `tools/java/build/redhat/RPMS/noarch/scylla-tools-core-*.rpm`

**.debs:**

* `build/dist/<config>/debian/scylla-conf_*.deb`
* `build/dist/<config>/debian/scylla_*.deb`
* `build/dist/<config>/debian/scylla-server_*.deb`
* `build/dist/<config>/debian/scylla-kernel-conf_*.deb`
* `build/dist/<config>/debian/scylla-node-exporter_*.deb`
* `build/dist/<config>/debian/scylla-server-dbg_*.deb`
* `tools/python3/build/debian/scylla-python3_*.deb`
* `tools/jmx/build/debian/scylla-jmx_*.deb`
* `tools/java/build/debian/scylla-tools-core_*.deb`
* `tools/java/build/debian/scylla-tools_*.deb`

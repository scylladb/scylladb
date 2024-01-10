---

# Scylla

[![Slack](https://img.shields.io/badge/slack-scylla-brightgreen.svg?logo=slack)](http://slack.scylladb.com)
[![Twitter](https://img.shields.io/twitter/follow/ScyllaDB.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=ScyllaDB)

## What is Scylla?

Scylla is the real-time big data database that is API-compatible with Apache Cassandra and Amazon DynamoDB.
Scylla embraces a shared-nothing approach that increases throughput and storage capacity to realize order-of-magnitude performance improvements and reduce hardware costs.

For more information, please see the [ScyllaDB web site].

[ScyllaDB web site]: https://www.scylladb.com

## Build Prerequisites

Scylla is fairly fussy about its build environment, requiring very recent
versions of the C++20 compiler and of many libraries to build. The document
[HACKING.md](HACKING.md) includes detailed information on building and
developing Scylla, but to get Scylla building quickly on (almost) any build
machine, Scylla offers a [frozen toolchain](tools/toolchain/README.md),
This is a pre-configured Docker image which includes recent versions of all
the required compilers, libraries and build tools. Using the frozen toolchain
allows you to avoid changing anything in your build machine to meet Scylla's
requirements - you just need to meet the frozen toolchain's prerequisites
(mostly, Docker or Podman being available).

## Building Scylla

Building Scylla with the frozen toolchain `dbuild` is as easy as:

```bash
$ git submodule update --init --force --recursive
$ ./tools/toolchain/dbuild ./configure.py
$ ./tools/toolchain/dbuild ninja build/release/scylla


For further information, please see:

* [Developer documentation] for more information on building Scylla.
* [Build documentation] on how to build Scylla binaries, tests, and packages.
* [Docker image build documentation] for information on how to build Docker images.

[developer documentation]: HACKING.md
[build documentation]: docs/dev/building.md
[docker image build documentation]: dist/docker/debian/README.md

## Running Scylla

To start Scylla server, run:

```bash
$ ./tools/toolchain/dbuild ./build/release/scylla --workdir tmp --smp 1 --developer-mode 1
```

This will start a Scylla node with one CPU core allocated to it and data files stored in the `tmp` directory.
The `--developer-mode` is needed to disable the various checks Scylla performs at startup to ensure the machine is configured for maximum performance (not relevant on development workstations).
Please note that you need to run Scylla with `dbuild` if you built it with the frozen toolchain.

For more run options, run:

```bash
$ ./tools/toolchain/dbuild ./build/release/scylla --help
```

## Testing

See [test.py manual](docs/dev/testing.md).

## Scylla APIs and compatibility
By default, Scylla is compatible with Apache Cassandra and its APIs - CQL and
Thrift. There is also support for the API of Amazon DynamoDB™,
which needs to be enabled and configured in order to be used. For more
information on how to enable the DynamoDB™ API in Scylla,
and the current compatibility of this feature as well as Scylla-specific extensions, see
[Alternator](docs/alternator/alternator.md) and
[Getting started with Alternator](docs/alternator/getting-started.md).

## Documentation

- **[Developer Documentation](docs/dev/README.md):** Detailed information on Scylla development.
- **[Seastar Documentation](http://docs.seastar.io/master/index.html):** Documentation for the Seastar framework used in Scylla.
- **[User Documentation](https://docs.scylladb.com/):** User guides and resources for Scylla users.

## Training

Explore free, self-paced online courses and training materials at [Scylla University](https://university.scylladb.com/). These resources cover various topics, including:
- Scylla data modeling
- Administration and architecture
- Basic NoSQL concepts
- Application development using Scylla drivers
- Scylla setup, failover, and compactions
- Multi-datacenters and integration with third-party applications

## Contributing to Scylla

- **[Contribution Guidelines](CONTRIBUTING.md):** Learn how to report bugs, submit pull requests, or patches.
- **[Developer Guidelines](HACKING.md):** Guidelines for developers contributing to Scylla development.

## Contact

- **[Community Forum](https://forum.scylladb.com/):** Discuss ScyllaDB configuration, management, and operations with other users.
- **[Slack Channel](http://slack.scylladb.com/):** Join the ScyllaDB Slack channel for real-time discussions.
- **[Developers Mailing List](https://groups.google.com/forum/#!forum/scylladb-dev):** Participate in technical discussions about ScyllaDB development.


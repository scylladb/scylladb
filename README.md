# Scylla

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
$ git submodule update --init --force --recursive
$ ./tools/toolchain/dbuild ./configure.py
$ ./tools/toolchain/dbuild ninja build/release/scylla
```

Please see the [developer documentation] for more information on building Scylla and [packaging documentation] on how to build Scylla packages for different Linux distributions.

[developer documentation]: HACKING.md
[packaging documentation]: docs/building-packages.md

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
$ ./tools/toolchain/dbuild ./build/release/scylla --help
```

## Testing

See [test.py manual](docs/testing.md).

## Scylla APIs and compatibility
By default, Scylla is compatible with Apache Cassandra and its APIs - CQL and
Thrift. There is also experimental support for the API of Amazon DynamoDB,
but being experimental it needs to be explicitly enabled to be used. For more
information on how to enable the experimental DynamoDB compatibility in Scylla,
and the current limitations of this feature, see
[Alternator](docs/alternator/alternator.md) and
[Getting started with Alternator](docs/alternator/getting-started.md).

## Documentation

Documentation can be found in [./docs](./docs) and on the
[wiki](https://github.com/scylladb/scylla/wiki). There is currently no clear
definition of what goes where, so when looking for something be sure to check
both.
Seastar documentation can be found [here](http://docs.seastar.io/master/index.html).
User documentation can be found [here](https://docs.scylladb.com/).

## Training 

Training material and online courses can be found at [Scylla University](https://university.scylladb.com/). 
The courses are free, self-paced and include hands-on examples. They cover a variety of topics including Scylla data modeling, 
administration, architecture, basic NoSQL concepts, using drivers for application development, Scylla setup, failover, compactions, 
multi-datacenters and how Scylla integrates with third-party applications.

## Building a CentOS-based Docker image

Build a Docker image with:

```
cd dist/docker/redhat
docker build -t <image-name> .
```

This build is based on executables downloaded from downloads.scylladb.com,
**not** on the executables built in this source directory. See further
instructions in dist/docker/redhat/README.md to build a docker image from
your own executables.

Run the image with:

```
docker run -p $(hostname -i):9042:9042 -i -t <image name>
```

## Contributing to Scylla

[Hacking howto](HACKING.md)
[Guidelines for contributing](CONTRIBUTING.md)

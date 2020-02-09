# Scylla

## Quick-start

To get the build going quickly, Scylla offers a [frozen toolchain](tools/toolchain/README.md)
which would build and run Scylla using a pre-configured Docker image.
Using the frozen toolchain will also isolate all of the installed
dependencies in a Docker container.
Assuming you have met the toolchain prerequisites, which is running
Docker in user mode, building and running is as easy as:

```bash
$ ./tools/toolchain/dbuild ./configure.py
$ ./tools/toolchain/dbuild ninja build/release/scylla
$ ./tools/toolchain/dbuild ./build/release/scylla --developer-mode 1
 ```

Please see [HACKING.md](HACKING.md) for detailed information on building and developing Scylla.

**Note**: GCC >= 8.1.1 is required to compile Scylla.

## Running Scylla

* Run Scylla
```
./build/release/scylla

```

* run Scylla with one CPU and ./tmp as work directory

```
./build/release/scylla --workdir tmp --smp 1
```

* For more run options:
```
./build/release/scylla --help
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

## Building Fedora RPM

As a pre-requisite, you need to install [Mock](https://fedoraproject.org/wiki/Mock) on your machine:

```
# Install mock:
sudo yum install mock

# Add user to the "mock" group:
usermod -a -G mock $USER && newgrp mock
```

Then, to build an RPM, run:

```
./dist/redhat/build_rpm.sh
```

The built RPM is stored in ``/var/lib/mock/<configuration>/result`` directory.
For example, on Fedora 21 mock reports the following:

```
INFO: Done(scylla-server-0.00-1.fc21.src.rpm) Config(default) 20 minutes 7 seconds
INFO: Results and/or logs in: /var/lib/mock/fedora-21-x86_64/result
```

## Building Fedora-based Docker image

Build a Docker image with:

```
cd dist/docker
docker build -t <image-name> .
```

Run the image with:

```
docker run -p $(hostname -i):9042:9042 -i -t <image name>
```

## Contributing to Scylla

[Hacking howto](HACKING.md)
[Guidelines for contributing](CONTRIBUTING.md)

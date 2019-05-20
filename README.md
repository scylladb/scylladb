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

* run Scylla with one CPU and ./tmp as data directory

```
./build/release/scylla --datadir tmp --commitlog-directory tmp --smp 1
```

* For more run options:
```
./build/release/scylla --help
```

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

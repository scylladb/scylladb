# Scylla

## Building Scylla

In addition to required packages by Seastar, the following packages are required by Scylla.

### Submodules
Scylla uses submodules, so make sure you pull the submodules first by doing:
```
git submodule init
git submodule update --init --recursive
```

### Building and Running Scylla on Fedora
* Installing required packages:

```
sudo dnf install yaml-cpp-devel lz4-devel zlib-devel snappy-devel jsoncpp-devel thrift-devel antlr3-tool antlr3-C++-devel libasan libubsan gcc-c++ gnutls-devel ninja-build ragel libaio-devel cryptopp-devel xfsprogs-devel numactl-devel hwloc-devel libpciaccess-devel libxml2-devel python3-pyparsing lksctp-tools-devel protobuf-devel protobuf-compiler systemd-devel libunwind-devel
```

* Build Scylla
```
./configure.py --mode=release --with=scylla --disable-xen
ninja-build build/release/scylla -j2 # you can use more cpus if you have tons of RAM

```

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

[Guidelines for contributing](CONTRIBUTING.md)

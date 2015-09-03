#Urchin

##Building Urchin

In addition to required packages by Seastar, the following packages are required by Urchin.

### Submodules
Urchin uses submodules, so make sure you pull the submodules first by doing:
```
git submodule init
git submodule update --recursive
```

### Building urchin on Fedora
Installing required packages:

```
sudo yum install yaml-cpp-devel lz4-devel zlib-devel snappy-devel jsoncpp-devel thrift-devel antlr3-tool antlr3-C++-devel libasan libubsan
```

### Building urchin on Ubuntu 14.04
Installing required packages:

```
sudo apt-get install libyaml-cpp-dev liblz4-dev zlib1g-dev libsnappy-dev libjsoncpp-dev
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

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


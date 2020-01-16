# Building Scylla Packages

This document describes how to build Scylla's packages for supported Linux distributions.

The build system generates _relocatable packages_, which means that the packages contain all the dependencies they need, and you can, therefore, install and run the same binaries on all Linux distributions.
The packages are built using Scylla's `dbuild` tool, which builds the packages in a Docker container.

### Scylla Server

To build a tarball, which contains the full Scylla server, run (remember to substitute `<mode>` with, for example, `release`):

```
./tools/toolchain/dbuild ./reloc/build_reloc.sh --mode <mode>
```

This step generates the following tarball:

```
build/<mode>/scylla-package.tar.gz
```

You can then generate RPM packages with:

```
./tools/toolchain/dbuild ./reloc/build_rpm.sh --reloc-pkg build/<mode>/scylla-package.tar.gz
```

This step generates the following RPM packages:

```
build/redhat/RPMS/x86_64/scylla-666.development-0.20190807.7d0c99e268.x86_64.rpm
build/redhat/RPMS/x86_64/scylla-server-666.development-0.20190807.7d0c99e268.x86_64.rpm
build/redhat/RPMS/x86_64/scylla-debuginfo-666.development-0.20190807.7d0c99e268.x86_64.rpm
build/redhat/RPMS/x86_64/scylla-conf-666.development-0.20190807.7d0c99e268.x86_64.rpm
build/redhat/RPMS/x86_64/scylla-kernel-conf-666.development-0.20190807.7d0c99e268.x86_64.rpm
```

You can also generate deb packages with:

```
./tools/toolchain/dbuild ./reloc/build_deb.sh --reloc-pkg build/debug/scylla-package.tar.gz
```

This step generates the following deb packages:

```
build/debian/scylla-server-dbg_666.development-0.20190807.7d0c99e268-1_amd64.deb
build/debian/scylla-conf_666.development-0.20190807.7d0c99e268-1_amd64.deb
build/debian/scylla-kernel-conf_666.development-0.20190807.7d0c99e268-1_amd64.deb
build/debian/scylla-server_666.development-0.20190807.7d0c99e268-1_amd64.deb
build/debian/scylla_666.development-0.20190807.7d0c99e268-1_amd64.deb
```

### Python interpreter

To build a tarball, which contains a [portable Python interpreter](https://www.scylladb.com/2019/02/14/the-complex-path-for-a-simple-portable-python-interpreter-or-snakes-on-a-data-plane/), run:

```
./tools/toolchain/dbuild ./reloc/python3/build_reloc.sh
```

This step generates the following tarball:

```
build/release/scylla-python3-package.tar.gz
```

You can then generate a RPM package:

```
./tools/toolchain/dbuild ./reloc/python3/build_rpm.sh                       
```

This step generates the following RPM package:

```
build/redhat/RPMS/x86_64/scylla-python3-3.7.2-0.20190807.689fc72bab.x86_64.rpm
```

You can also generate a deb package with:

```
./tools/toolchain/dbuild ./reloc/python3/build_deb.sh                       
```

This step generates the following deb package:

```
build/debian/scylla-python3_3.7.2-0.20190807.689fc72bab-1_amd64.deb
```

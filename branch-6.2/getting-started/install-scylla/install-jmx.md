# Install scylla-jmx Package

scylla-jmx becomes optional package from ScyllaDB 6.2, not installed by default.
If you need JMX server you can still install it from scylla-jmx GitHub page.

Debian/Ubuntu

1. Download .deb package from scylla-jmx page.
   > Access to [https://github.com/scylladb/scylla-jmx](https://github.com/scylladb/scylla-jmx), select latest
   > release from “releases”, download a file end with “.deb”.
2. (Optional) Transfer the downloaded package to the install node.
   > If the pc from which you downloaded the package is different from
   > the node where you install scylladb, you will need to transfer
   > the files to the node.
3. Install scylla-jmx package.
   > ```console
   > sudo apt install -y ./scylla-jmx_<version>_all.deb
   > ```

Centos/RHEL

1. Download .rpm package from scylla-jmx page.
   > Access to [https://github.com/scylladb/scylla-jmx](https://github.com/scylladb/scylla-jmx), select latest
   > release from “releases”, download a file end with “.rpm”.
2. (Optional) Transfer the downloaded package to the install node.
   > If the pc from which you downloaded the package is different from
   > the node where you install scylladb, you will need to transfer
   > the files to the node.
3. Install scylla-jmx package.
   > ```console
   > sudo yum install -y ./scylla-jmx-<version>.noarch.rpm
   > ```

Install without root privileges

1. Download .tar.gz package from scylla-jmx page.
   > Access to [https://github.com/scylladb/scylla-jmx](https://github.com/scylladb/scylla-jmx), select latest
   > release from “releases”, download a file end with “.tar.gz”.
2. (Optional) Transfer the downloaded package to the install node.
   > If the pc from which you downloaded the package is different from
   > the node where you install scylladb, you will need to transfer
   > the files to the node.
3. Install scylla-jmx package.
   > ```console
   > tar xpf scylla-jmx-<version>.noarch.tar.gz
   > cd scylla-jmx
   > ./install.sh --nonroot
   > ```

## Next Steps

* [Configure ScyllaDB](https://opensource.docs.scylladb.com/stable/getting-started/system-configuration.md)
* Manage your clusters with [ScyllaDB Manager](https://manager.docs.scylladb.com/)
* Monitor your cluster and data with [ScyllaDB Monitoring](https://monitoring.docs.scylladb.com/)
* Get familiar with ScyllaDB’s [command line reference guide](https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool.md).
* Learn about ScyllaDB at [ScyllaDB University](https://university.scylladb.com/)

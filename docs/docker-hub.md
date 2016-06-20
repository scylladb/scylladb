# Supported tags and respective `Dockerfile` links

-	[`1.3.0`, `1.3`, `1` (*1.3/Dockerfile*)](https://github.com/docker-library/scylladb/blob/?????/1.3/Dockerfile)

For more information about this image and its history, please see [the relevant manifest file (`library/scylladb`)](https://github.com/docker-library/official-images/blob/master/library/scylladb). This image is updated via [pull requests to the `docker-library/official-images` GitHub repo](https://github.com/docker-library/official-images/pulls?q=label%3Alibrary%2Fscylladb).

For detailed information about the virtual/transfer sizes and individual layers of each of the above supported tags, please see [the `scylladb/tag-details.md` file](https://github.com/docker-library/docs/blob/master/scylladb/tag-details.md) in [the `docker-library/docs` GitHub repo](https://github.com/docker-library/docs).

# What is ScyllaDB ?

ScyllaDB is a high-performance Cassandra implementation written in C++14. Classified as a NoSQL database, ScyllaDB deliver a high number of transactions per seconds making it one of the fastest database on the planet. ScyllaDB is released under the GNU Affero General Public License version 3 and the Apache License, ScyllaDB is free and open-source software.

> [ScyllaDB](http://www.scylladb.com/)

![logo](http://www.scylladb.com/img/logo.svg)

## Testing with docker

To launch a Scylla instance, run:

```
docker pull scylladb/scylla

docker run -p 127.0.0.1:9042:9042 -i -t scylladb/scylla

```

##  Docker for production usage

First disable SELinux if present.

ScyllaDB needs XFS to perform well.  Get a kernel with XFS patches. On Ubuntu see the kernel section of [Getting Started with Scylla on Red Hat Enterprise, CentOS, and Fedora](/doc/getting-started-rpm/).

### Install xfsprogs on Ubuntu

```sh
apt-get install xfsprogs
```

### Install xfsprogs on Centos

```sh
yum install xfsprogs
```

### Format and prepare the XFS volume

Remember the volume device file you want to use as `$VOLUME`.

As root on the host (`sudo su -`) do:

```sh
mkfs.xfs /dev/$VOLUME
echo "/dev/$VOLUME       /var/lib/scylla  xfs    defaults                0 2" >> /etc/fstab
mkdir /var/lib/scylla
mount /var/lib/scylla
ln -s /etc/scylla /var/lib/scylla/conf
mkdir /var/lib/scylla/data/
mkdir /var/lib/scylla/commitlog
chown -R 997.1000 /var/lib/scylla/
```

Prepare the `rc.local` script

```sh
echo '#!/bin/sh -e' > /etc/rc.local
echo "chown -R 997.1000 /var/lib/scylla/" >> /etc/rc.local
echo "exit 0" >> /etc/rc.local
chmod +x /etc/rc.local
```

Read the `--cpuset` reference in [Docker run reference](https://docs.docker.com/engine/reference/run/) and determine and set the `CPUSET` variable corresponding to your needs.

Then to launch a Scylla instance, run:

```sh
docker pull scylladb/scylla

docker run -e "SCYLLA_PRODUCTION=true" -e "SCYLLA_CPU_SET=$CPUSET" -v /var/lib/scylla:/var/lib/scylla --cpuset-cpus="$CPUSET" -p 127.0.0.1:9042:9042 -i -t scylladb/scylla

./tools/bin/cassandra-stress write -mode cql3 native
```

## Docker clustering on the same physical machine

```sh
docker pull scylladb/scylla

docker run -p 127.0.0.1:9042:9042 -d --name scylla_seed_node -t scylladb/scylla
docker run -p 127.0.0.1:9043:9042 -e SCYLLA_SEEDS="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla_seed_node)" -d --name scylla_node -t scylladb/scylla
```

## Docker clustering with multiple physical machines

Given two machine with first machine having ip IP1 and
second having ip IP2 you can do.


On the first machine

```sh
docker pull scylladb/scylla

docker run -p $IP1:9042:9042 -e SCYLLA_SEEDS=$IP1 -e SCYLLA_BROADCAST_ADDRESS=$IP1 -p 7000:7000 -d --name scylla_seed_node -t scylladb/scylla
```

And on the second
```sh
docker pull scylladb/scylla

docker run -p $IP2:9042:9042 -e SCYLLA_SEEDS=$IP1 -e SCYLLA_BROADCAST_ADDRESS=$IP2 -p 7000:7000  -d --name scylla_node -t scylladb/scylla
```

## List of ports that are nice to forward from the host to inside the container

7199: JMX (for using nodetool)
7000: Internode communication (The cluster communicate with it)
9042: CQL native transport port (How the client application access the database)

## ScyllaDB Docker special variables

### SCYLLA_BROADCAST_ADDRESS

Ip address to communicate to other node of the cluster.


### SCYLLA_CPU_SET

Cpu set to pass to Scylla as exposed in the [Docker run reference](https://docs.docker.com/engine/reference/run/).

### SCYLLA_PRODUCTION

Can be set to true in order to desactivate developer mode. This must be combined with a bind mount of an XFS volume
in the /var/lib/scylla destination.

### SCYLLA_SEEDS

List of seed nodes IPs. Seed nodes are used to discover the Scylla cluster topology at startup.

## Issues

Don't hesitage to report bugs or issues on http://www.github.com/scylladb/scylla's bug tracker.

## Contributing

Want to scratch your own itch and contribute a patch.
We are eager to review and merge your code.
Please consult the [Contributing on Scylla page](http://www.scylladb.com/kb/contributing/)

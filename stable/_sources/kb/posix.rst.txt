POSIX networking for ScyllaDB
=============================
**Topic: Planning and setup**

**Learn: How to configure POSIX networking for ScyllaDB**

**Audience: Developers, devops, integration testers**

The Seastar framework used in ScyllaDB can support two networking modes.
For high-performance production workloads, use the Data Plane
Development Kit (DPDK) for maximum performance on specific modern
network hardware.

For ease of development, you can also use POSIX mode, which works on all
physical and virtual network devices supported by your operating system.

Kernel Configuration
--------------------

The Linux “transparent hugepages” feature is recommended, but not
required, in POSIX mode to minimize overhead on memory allocations. The
value of ``/sys/kernel/mm/transparent_hugepage/enabled`` should be
``always`` or ``madvise``. See your Linux distribution's documentation
for instructions on how to enable transparent hugepages.

Firewall Configuration
----------------------

For a single node, the firewall will need to be set up to allow TCP on
the following :ref:`ports <cqlsh-networking>`.

ScyllaDB Configuration
----------------------

POSIX mode is the default, in ``/etc/sysconfig/scylla-server``. Check
that ``NETWORK_MODE`` is set to ``posix``.

::

    # choose following mode: virtio, dpdk, posix
    NETWORK_MODE=posix.

The ``ETHDRV`` option is ignored in POSIX mode.

More Information
----------------

`Cassandra: configuring firewall port
access <http://docs.datastax.com/en//cassandra/2.0/cassandra/security/secureFireWall_r.html>`__

`How to use, monitor, and disable transparent hugepages in Red Hat
Enterprise Linux 6 <https://access.redhat.com/solutions/46111>`__

:doc:`Knowledge Base </kb/index>`


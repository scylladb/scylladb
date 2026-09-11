
=================================
ScyllaDB in a Shared Environment
=================================

Scylla is designed to utilize all of the resources on the machine. It
runs on: disk and network bandwidth, RAM, and CPU. This allows you to
achieve maximum performance with a minimal node count. In development
and test, however, your nodes might be using a shared machine, which
Scylla cannot dominate. This article explains how to configure Scylla
for shared environments. For some production environments, these settings
may be preferred as well.

Note that a Docker image is a viable and even simpler option - `Scylla
on dockerhub <https://hub.docker.com/r/scylladb/scylla/>`_


Memory
------

The most critical resource that Scylla consumes is memory. By default,
when Scylla starts up, it inspects the node's hardware configuration and
claims *all* memory to itself, leaving some reserve for the operating
system (OS). This is in contrast to most open-source databases that
leave most memory for the OS, but is similar to most commercial
databases.

In a shared environment, particularly on a desktop or laptop, gobbling
up all the machine's memory can reduce the user experience, so Scylla
allows reducing its memory usage to a given quantity.

On Ubuntu, open a terminal and edit ``/etc/default/scylla-server``, and add ``--memory 2G``
to restrict Scylla to 2 gigabytes of RAM.

On Red Hat / CentOS, open a terminal and edit ``/etc/sysconfig/scylla-server``, and add
``--memory 2G`` to restrict Scylla to 2 gigabytes of RAM.

If starting Scylla from the command line, simply append ``--memory 2G``
to your command line.

CPU
---

By default, Scylla will utilize *all* of your processors (in some
configurations, particularly on Amazon AWS, it may leave a core for the
operating system). In addition, Scylla will pin its threads to specific
cores in order to maximize the utilization of the processor on-chip
caches. On a dedicated node, this allows maximum throughput, but on a
desktop or laptop, it can cause a sluggish user interface.

Scylla offers two options to restrict its CPU utilization:

-  ``--smp N`` restricts Scylla to N logical cores; for example with
   ``--smp 2`` Scylla will not utilize more than two logical cores
-  ``--overprovisioned`` tells Scylla that the machine it is running on
   is used by other processes; so Scylla will not pin its threads or
   memory, and will reduce the amount of polling it does to a minimum.

On Ubuntu edit ``/etc/default/scylla-server``, and add
``--smp 2 --overprovisioned`` to restrict Scylla to 2 logical cores.

On Red Hat / CentOS  edit ``/etc/sysconfig/scylla-server``, and add
``--smp 2 --overprovisioned`` to restrict Scylla to 2 logical cores.

If starting Scylla from the command line, simply append
``--smp 2 --overprovisioned`` to your command line.

Other Restrictions
------------------

When starting up, Scylla will check the hardware and operating system
configuration to verify that it is compatible with Scylla's performance requirements. See :doc:`developer mode </getting-started/installation-common/dev-mod>` for more instructions.

Summary
-------

Scylla comes out of the box ready for production use with maximum
performance but may need to be tuned for development or test uses. This
tuning is simple to apply and results in a Scylla server that can
coexist with other processes or a GUI on the system.

.. include:: /rst_include/advance-index.rst

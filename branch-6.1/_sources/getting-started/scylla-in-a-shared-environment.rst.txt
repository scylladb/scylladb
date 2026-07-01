
=================================
ScyllaDB in a Shared Environment
=================================

ScyllaDB is designed to utilize all of the resources on the machine. It
runs on: disk and network bandwidth, RAM, and CPU. This allows you to
achieve maximum performance with a minimal node count. In development
and test, however, your nodes might be using a shared machine, which
ScyllaDB cannot dominate. This article explains how to configure ScyllaDB
for shared environments. For some production environments, these settings
may be preferred as well.

Note that a Docker image is a viable and even simpler option - `ScyllaDB
on dockerhub <https://hub.docker.com/r/scylladb/scylla/>`_


Memory
------

The most critical resource that ScyllaDB consumes is memory. By default,
when ScyllaDB starts up, it inspects the node's hardware configuration and
claims *all* memory to itself, leaving some reserve for the operating
system (OS). This is in contrast to most open-source databases that
leave most memory for the OS, but is similar to most commercial
databases.

In a shared environment, particularly on a desktop or laptop, gobbling
up all the machine's memory can reduce the user experience, so ScyllaDB
allows reducing its memory usage to a given quantity.

On Ubuntu, open a terminal and edit ``/etc/default/scylla-server``, and add ``--memory 2G``
to restrict ScyllaDB to 2 gigabytes of RAM.

On Red Hat / CentOS, open a terminal and edit ``/etc/sysconfig/scylla-server``, and add
``--memory 2G`` to restrict ScyllaDB to 2 gigabytes of RAM.

If starting ScyllaDB from the command line, simply append ``--memory 2G``
to your command line.

CPU
---

By default, ScyllaDB will utilize *all* of your processors (in some
configurations, particularly on Amazon AWS, it may leave a core for the
operating system). In addition, ScyllaDB will pin its threads to specific
cores in order to maximize the utilization of the processor on-chip
caches. On a dedicated node, this allows maximum throughput, but on a
desktop or laptop, it can cause a sluggish user interface.

ScyllaDB offers two options to restrict its CPU utilization:

-  ``--smp N`` restricts ScyllaDB to N logical cores; for example with
   ``--smp 2`` ScyllaDB will not utilize more than two logical cores
-  ``--overprovisioned`` tells ScyllaDB that the machine it is running on
   is used by other processes; so ScyllaDB will not pin its threads or
   memory, and will reduce the amount of polling it does to a minimum.

On Ubuntu edit ``/etc/default/scylla-server``, and add
``--smp 2 --overprovisioned`` to restrict ScyllaDB to 2 logical cores.

On Red Hat / CentOS  edit ``/etc/sysconfig/scylla-server``, and add
``--smp 2 --overprovisioned`` to restrict ScyllaDB to 2 logical cores.

If starting ScyllaDB from the command line, simply append
``--smp 2 --overprovisioned`` to your command line.

Overprovisioning at the Hypervisor level
----------------------------------------

Sometimes, you can dedicate the entire node to ScyllaDB, but the node itself
is running on a hypervisor that overcommits the CPU (this is not true for most
public cloud instance types). You can tune ScyllaDB to
this configuration by using the command line flags ``--idle-poll-time-us 0 --poll-aio 0``.
These settings tell ScyllaDB to never poll while waiting for a network or disk event,
and instead relinquish the CPU quickly. This lets the hypervisor schedule other
virtual machines, and in turns avoids penalizing the ScyllaDB node for overconsumption.

These settings are a subset of the `--overprovisioned` flag (omitting memory and affinity settings).

On Ubuntu edit ``/etc/default/scylla-server``, and add
``--idle-poll-time-us 0 --poll-aio 0`` to tune ScyllaDB for an overcommitted hypervisor.

On Red Hat / CentOS  edit ``/etc/sysconfig/scylla-server``, and add
``--idle-poll-time-us 0 --poll-aio 0`` to tune ScyllaDB for an overcommitted hypervisor.

If starting ScyllaDB from the command line, simply append
``--idle-poll-time-us 0 --poll-aio 0`` to your command line.



Other Restrictions
------------------

When starting up, ScyllaDB will check the hardware and operating system
configuration to verify that it is compatible with ScyllaDB's performance requirements. See :doc:`developer mode </getting-started/installation-common/dev-mod>` for more instructions.

Summary
-------

ScyllaDB comes out of the box ready for production use with maximum
performance but may need to be tuned for development or test uses. This
tuning is simple to apply and results in a ScyllaDB server that can
coexist with other processes or a GUI on the system.

.. include:: /rst_include/advance-index.rst

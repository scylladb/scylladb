==========================
Map CPUs to Scylla Shards
==========================

Due to its thread-per-core architecture, many things within Scylla can be better understood when you look at it on a per-CPU basis. There are Linux tools such as ``top`` and ``perf`` that can give information about what is happening within a CPU, given a CPU number.

A common mistake users make is to assume that there is a direct and predictable relationship between the Scylla Shard ID and the CPU ID, which is not true.

Starting in version 3.0, Scylla ships with a script to let users know about the mapping between CPUs and Scylla Shards. For users of older versions, a copy of the script can be downloaded from the `Seastar git tree <https://raw.githubusercontent.com/scylladb/seastar/master/scripts/seastar-cpu-map.sh>`_.

Examples of usage
------------------

To list the mapping of a specific shard:

.. code-block:: bash

   seastar-cpu-map.sh -n scylla -s 0

Output:

.. code-block:: console
   :class: hide-copy-button

   shard: 0, cpu: 1

To list the mapping of all shards:

.. code-block:: bash

   seastar-cpu-map.sh -n scylla 

Output:

.. code-block:: console
   :class: hide-copy-button

   shard: 0, cpu: 1
   shard: 1, cpu: 2
   shard: 3, cpu: 4
   shard: 2, cpu: 3


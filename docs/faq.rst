==============
ScyllaDB FAQ
==============

.. meta::
   :title:
   :description: Frequently Asked Questions about ScyllaDB
   :keywords: questions, ScyllaDB, ScyllaDB, DBaaS, FAQ, error, problem

Performance
-----------

ScyllaDB is using all of my memory! Why is that? What if the server runs out of memory?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ScyllaDB uses available memory to cache your data. ScyllaDB knows how to dynamically manage memory for optimal performance; for example, if many clients connect to ScyllaDB, it will evict some data from the cache to make room for these connections; when the connection count drops again, this memory is returned to the cache.

Can I limit ScyllaDB to use less CPU and memory?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :code:`--smp` option (for instance, :code:`--smp 2`) will restrict ScyllaDB to a smaller number of CPUs. It will still use 100 % of those CPUs, but at least won’t take your system out completely. An analogous option exists for memory: :code:`-m`.

What are some of the techniques ScyllaDB uses to achieve its performance?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ScyllaDB tries to utilize all available resources (processor cores, memory, storage, and networking) by always operating in parallel and never blocking. If ScyllaDB needs to read a disk block, it initiates the read and immediately moves on to another task. Later, when the read completes ScyllaDB resumes the original task from where it left off. By never blocking, a high degree of concurrency is achieved, allowing all resources to be utilized to their limit.
Read more on ScyllaDB Architecture:

* `ScyllaDB Technology <http://www.scylladb.com/product/technology/>`_
* `ScyllaDB Memory Management <http://www.scylladb.com/product/technology/memory-management/>`_

I thought that ScyllaDB's underlying `Seastar framework <https://github.com/scylladb/seastar>`_ uses one thread per core, but I see more than two threads per core. Why?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Seastar creates an extra thread per core for blocking syscalls (like :code:`open()`/ :code:`fsync()` / :code:`close()` ); this allows the Seastar reactor to continue executing while a blocking operation takes place. Those threads are usually idle, so they don’t contribute to significant context switching activity.

I’m seeing X compaction running in parallel on a single ScyllaDB node. Is it normal?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Yes, for more than one reason:

* each shard (core) will run its compactions independently, often at the same time,
* each table will run its compactions independently, often at the same time
* depending on the compaction strategy, more than one compaction can run in parallel. For example in Sized Tier Compaction Strategy (STCS), large sstable compaction can take time, allowing smaller sstable to be compacted at the same time

Do I need to configure ``swap`` on a ScyllaDB node?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Yes, configuring ``swap`` on a ScyllaDB node is recommended.
``swap`` size should be set to either ``total_mem``/3 or 16GB - lower of the two.

``total_mem`` is the total size of the nodes memory.

For example:

* If the node ``total_mem`` is 18GB ``swap`` size should be set to 6GB.

* If the node ``total_mem`` is 240GB ``swap`` size should be set to 16GB.

Swap can be set up in several ways. One way to set up swap is detailed in the KB Article :doc:`How to Set up a Swap Space </kb/set-up-swap>`.

My query does not return any or some of the data? What happened?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If you are using a time range in the query, refer to the solution in the troubleshooting document, :doc:`Time Range Queries Do Not Return Some or All of the Data </troubleshooting/time-zone>`.


Using the Java driver SimpleStatements are slow. Why does this happen?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Java driver's `SimpleStatement <https://java-driver.docs.scylladb.com/stable/manual/statements/simple/>`_ is token unaware by default. This means that requests sent out will reach the Controller node before it is known which shard it's supposed to access. We suggest using `PreparedStatements <https://java-driver.docs.scylladb.com/stable/manual/statements/prepared/>`_ instead.

Disk Space
-----------

.. _reclaim-space:

Dropping a table does not reduce storage used by ScyllaDB, how can I clean the disk from dropped tables?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
scylla.yaml includes an ``auto_snapshot`` parameter; when true (it is by default), ScyllaDB creates a snapshot for a table just before dropping it, as a safety measure.
You can find the snapshot in the ``snapshots`` directory, under the table SSTable. For example, for dropped table ``users`` in keyspace ``mykeyspace``:

:code:`/var/lib/scylla/data/mykeyspace/users-bdba4e60f6d511e7a2ab000000000000/snapshots/1515678531438-users`


As the snapshot take the same space as the dropped table, disk usage will remain the same.
You can clean snapshots by using :doc:`nodetool clearsnapshot </operating-scylla/nodetool-commands/clearsnapshot>`. Read more on :doc:`snapshot and clearsnapshot </operating-scylla/procedures/backup-restore/delete-snapshot/>`
  
Features
--------

How do I check the current version of ScyllaDB that I am running?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* On a regular system or VM (running Ubuntu, CentOS, or RedHat Enterprise): :code:`$ scylla --version`

Check the `Operating System Support Guide <https://docs.scylladb.com/stable/versioning/os-support-per-version.html>`_ for a list of supported operating systems and versions.

* On a docker node: :code:`$ docker exec -it Node_Z scylla --version`


Docker
-------

What if I get an error when connecting an application to a ScyllaDB cluster in Docker?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Connectivity problems may occur if you are trying to connect to the ScyllaDB nodes with their Docker internal IP addresses.

If you need to reach your nodes from outside the internal Docker network, you must expose the appropriate ports to the Docker host. 
See `Error connecting Java Spring application to ScyllaDB Cluster in Docker <https://stackoverflow.com/questions/72165195/error-connecting-java-spring-application-to-scylladb-cluster-in-docker>`_ for more information and an example.


Installation
-----------------------------------------------------

 .. TODO Update the example below then a patch release for 2025.1 is available

.. _faq-pinning:

Can I install or upgrade to a patch release other than latest on Debian or Ubuntu?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The APT package manager used for Ubuntu, Debian, and image installations of ScyllaDB installs the latest patch 
release (x.y.z) of a given major release (x.y). To remain on a ScyllaDB patch release that is not the latest, you can 
use pinning as a workaround.

The following example shows pinning ScyllaDB Enterprise version 2021.1.0-0.20210511.9e8e7d58b-1:

.. code-block:: console

   $ cat <<EOF | sudo tee /etc/apt/preferences.d/99scylla-enterprise
   Package: scylla-enterprise*
   Pin: version 2021.1.0-0.20210511.9e8e7d58b-1
   Pin-Priority: 1001
   EOF

Pinning may be particularly useful when you want to downgrade ScyllaDB or upgrade to a version that is not the latest 
available version. See `AptConfiguration <https://wiki.debian.org/AptConfiguration>`_ for details about pinning on Debian-based systems.

Alternatively, you can explicitly install **all** the ScyllaDB packages for the desired non-latest version. For example:

.. code-block:: console

   sudo apt-get install scylla-enterprise{,-server,-tools,-tools-core,-kernel-conf,-node-exporter,-conf,-python3}=2021.1.0-0.20210511.9e8e7d58b-1
   sudo apt-get install scylla-enterprise-machine-image=2021.1.0-0.20210511.9e8e7d58b-1  # only execute on AMI instance



.. _faq-snitch-strategy:

Which snitch or replication strategy should I use?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If you are creating a production cluster or if your cluster is going to have more than one data center you need to use a **DC-aware** snitch, e.g. :code:`GossipingPropertyFileSnitch` or :code:`Ec2MultiRegionSnitch`. You will also need to use a **DC-aware** replication strategy, e.g. :code:`NetworkTopologyStrategy`.

Our general recommendation is to always use a :code:`NetworkTopologyStrategy` and use :code:`Ec2XXX` snitches on AWS based clusters and :code:`GossipingPropertyFileSnitch` in all other cases.

A description of all snitch options we support may be found in :doc:`Snitches </operating-scylla/system-configuration/snitch>`.

Note: trying to mix a :code:`SimpleSnitch` with a :code:`DC-aware strategy` or a :code:`DC-aware snitch` with a :code:`SimpleStrategy` may cause your cluster not to work as intended therefore we **strongly discourage** these types of configurations in general.

Not using a proper snitch-strategy combination may cause different types of errors.

For instance:

.. code-block:: console

   Unavailable: code=1000 [Unavailable exception] message="Cannot achieve consistency level for cl LOCAL_ONE. Requires 1, alive 0" info={'required_replicas': 1, 'alive_replicas': 0, 'consistency': 'LOCAL_ONE'}

If you see this error you should always check that you are not using a :code:`SimpleSnitch` in your cluster configuration in conjunction with some :code:`DC-aware replication strategy` for a keyspace of a table you are failing to query.

When working with ``GossipingPropertyFileSnitch`` or ``Ec2MultiRegionSnitch`` you should edit the ``cassandra-rackdc.properties``

For node using ``GossipingPropertyFileSnitch``, the file should look like the following:

.. code-block:: cql

   dc=asia_datacenter
   rack=rack1
   prefer_local= true

When the node is the Asia data center, on rack1 and to minimize BW usage
for inter-datacenter, use the prefer_local

For ``Ec2MultiRegion`` the file should include the following information

.. code-block:: cql
   
   dc_suffix=my_dc

This will create a suffix for the node location for example:

.. code-block:: cql

   us-east1_my_dc


The problem may also arise if you are using some :code:`DC-aware snitch`, e.g. :code:`Ec2MultiRegionSnitch`, and a :code:`SimpleStrategy` in a multi-DC cluster.

Please make sure that both the snitch and the replication strategy of the keyspace are :code:`DC-aware`.

After that, if you are using a :code:`DC-aware` configuration, make sure that the replication strategy uses the proper data centers' names. Verify the data centers names in your cluster using a :code:`nodetool status` command.

Can I change the replication factor (a keyspace) on a live cluster?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Yes, but it will require running a full repair (or cleanup) to change the replica count of existing data:

- :ref:`Alter <alter-keyspace-statement>` the replication factor for desired keyspace (using cqlsh for instance).
- If you're reducing the replication factor, run ``nodetool cleanup <updated Keyspace>`` on the keyspace you modified to remove surplus replicated data.
  Cleanup runs on a per-node basis.
- If you're increasing the replication factor, refer to :doc:`How to Safely Increase the RF </kb/rf-increase>`
- Note that you need to provide the keyspace name. If you do not, the cleanup or repair operation runs on all keyspaces for the specific node.

.. _faq-best-scenario-node-multi-availability-zone:

What is the best scenario to add a node to a multi availability zone (AZ)?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If using three node cluster, with RF=3, each node located on a different availability zone (AZ).

For example:

.. code-block:: shell

   Datacenter: DC1
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  192.168.1.201  118.82 KB  256     33.6%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   A1
   UN  192.168.1.202  111.82 KB  256     33.1%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   UN  192.168.1.203  114.82 KB  256     33.3%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   C1

All nodes holds 100% of the data.
If needed to add a single node to the cluster (scale out), the cluster will become imbalance.
Because the single additional node will split the tokens only with the existing node in the same AZ.

.. Note:: 

   This is only an example, if having more nodes or different RF the number of nodes may be different.


The token distribution will be:

.. code-block:: shell

   AZ A1 node A: 100% of the data
   AZ B1 node B: 100% of the data
   AZ C1 node C: 50% of the data
   AZ C1 node D: 50% of the data

The solution is to add a node in each AZ.

.. code-block:: shell

   Datacenter: DC1
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  192.168.1.201  118.82 KB  256     16.6%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   A1
   UN  192.168.1.202  111.82 KB  256     16.1%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   UN  192.168.1.203  114.82 KB  256     16.3%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   C1
   UN  192.168.1.204  118.82 KB  256     16.6%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   A1
   UN  192.168.1.205  111.82 KB  256     16.1%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   UN  192.168.1.206  114.82 KB  256     16.3%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   C1

More info
---------
Where can I ask a question not covered here?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* `ScyllaDB Community Forum <https://forum.scylladb.com>`_: Discuss using ScyllaDB and developing client applications.
* `scylladb-dev <https://groups.google.com/d/forum/scylladb-dev>`_: Discuss the development of ScyllaDB itself.


I deleted data from ScyllaDB, but disk usage stays the same. Why?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Data you write to ScyllaDB gets persisted to SSTables. Since SSTables are immutable, the data can't actually be removed
when you perform a delete, instead, a marker (also called a "tombstone") is written to indicate the value's new status.
Never fear though, on the first compaction that occurs between the data and the tombstone, the data will be expunged
completely and the corresponding disk space recovered. 

.. _faq-raid0-required:

Is RAID0 required for ScyllaDB? Why?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

No, it is not required, but it is highly recommended when using ScyllaDB with more than one drive. ScyllaDB requires one drive for its data file and one drive for commit log (can be the same). If you want to take advantage of more than one drive, the easiest way to do so is set RAID0 (striped) across all of them. If you choose, scylla_setup will setup RAID0 for you on your selected drive, as well as XFS file system (recommended).
Similarly, ScyllaDB AMI on EC2 will automatically mount all available SSD drives in RAID0.

Should I use RAID for replications, such as RAID1, RAID4 or higher?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can, but it is not recommended. ScyllaDB :doc:`clustering architecture </architecture/ringarchitecture/index/>` already provides data replication across nodes and DCs.
Adding another layer of replication in each node is redundant, slows down I/O operation and reduces available storage.
Want a higher level of replication?
Increase the Replication Factor (RF) of :doc:`relevant Keyspaces </cql/ddl/>`.

Can I use JBOD and not use RAID0?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:term:`JBOD` is not supported by ScyllaDB.

:abbr:`JBOD (Just a Bunch Of Disks)` may be a reasonable solution for Cassandra because it rebuilds nodes very slowly. As this is not an issue for ScyllaDB, it's more efficient to use RAID. 

Explanation: There are two types of deployment when multiple disks exist. In the JBOD case, each disk is an isolated filesystem. I/O isn't stripped and thus performance can be slower than that of RAID. In addition, as the free space isn't shared, a single disk can be full while the others are available.

The benefit of JBOD vs RAID is that it isolates failures to individual disk and not the entire node.
However, ScyllaDB rebuilds nodes quickly and thus it is not an issue when rebuilding an entire node.

As a result, it is much more advantageous to use RAID with ScyllaDB


Is ``Nodetool Repair`` a Local (One Node) Operation or a Global (Full Cluster) Operation?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When running :doc:`nodetool repair </operating-scylla/nodetool-commands/repair/>` on a node, it performs a repair on every token range this node owns; this will also repair other nodes that share the same range.

If you wish to repair the entire cluster, it is recommended to run ``nodetool repair -pr`` on each node in the cluster, sequentially, or use the `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_.


How can I change the maximum number of IN restrictions?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can restrict the number of items in the IN clause with the following options:

* ``--max-partition-key-restrictions-per-query`` - Specifies the maximum number of distinct partition keys restrictions per query. This limit places a bound 
  on the size of IN tuples, especially when multiple partition key columns have IN restrictions. The default is ``100``.
* ``--max-clustering-key-restrictions-per-query`` - Specifies the maximum number of distinct clustering keys restrictions per query. This limit 
  places a bound on the size of IN tuples, especially when multiple clustering key columns have IN restrictions. The default is ``100``.

.. warning::

   We recommend that you use these options with caution. Changing the maximum number of IN restrictions to more than 100 may result in server instability.

The options can be configured on the command line, passed with ``SCYLLA_ARGS`` in ``/etc/default/scylla-server`` or ``/etc/sysconfig/scylla-server``, 
or added to your ``scylla.yaml`` (see :doc:`ScyllaDB Configuration<operating-scylla/admin>`).

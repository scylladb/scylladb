===================
System Requirements
===================

.. _system-requirements-supported-platforms:

Supported Platforms
===================
ScyllaDB runs on 64-bit Linux. The x86_64 and AArch64 architectures are supported (AArch64 support includes AWS EC2 Graviton).

See :doc:`OS Support by Platform and Version </getting-started/os-support>` for information about 
supported operating systems, distros, and versions.

See :doc:`Cloud Instance Recommendations for AWS, GCP, and Azure </getting-started/cloud-instance-recommendations>` for information
about instance types recommended for cloud deployments.

.. _system-requirements-hardware:

Hardware Requirements
=====================

It’s recommended to have a balanced setup. If there are only 4-8 :term:`Logical Cores <Logical Core (lcore)>`, large disks or 10Gbps networking may not be needed.
This works in the opposite direction as well.
ScyllaDB can be used in many types of installation environments.

To see which system would best suit your workload requirements, use the `ScyllaDB Sizing Calculator <https://www.scylladb.com/product/scylla-cloud/get-pricing/>`_ to customize ScyllaDB for your usage.



Core Requirements 
-----------------
ScyllaDB tries to maximize the resource usage of all system components. The shard-per-core approach allows linear scale-up with the number of cores. As you have more cores, it makes sense to balance the other resources, from memory to network.

CPU
^^^

ScyllaDB requires modern Intel/AMD CPUs that support the SSE4.2 instruction set and will not boot without it.


ScyllaDB supports the following CPUs:

* Intel core: Westmere and later (2010)
* Intel atom: Goldmont and later (2016)
* AMD low power: Jaguar and later (2013)
* AMD standard: Bulldozer and later (2011)
* Apple M1 and M2
* Ampere Altra
* AWS Graviton, Graviton2, Graviton3


In terms of the number of cores, any number will work since ScyllaDB scales up with the number of cores. 
A practical approach is to use a large number of cores as long as the hardware price remains reasonable. 
Between 20-60 logical cores (including hyperthreading) is a recommended number. However, any number will fit. 
When using virtual machines, containers, or the public cloud, remember that each virtual CPU is mapped to a single logical core, or thread. 
Allow ScyllaDB to run independently without any additional CPU intensive tasks on the same server/cores as Scylla.

.. _system-requirements-memory:

Memory Requirements
-------------------
The more memory available, the better ScyllaDB performs, as ScyllaDB uses all of the available memory for caching. The wider the rows are in the schema, the more memory will be required. 64 GB-256 GB is the recommended range for a medium to high workload. Memory requirements are calculated based on the number of :abbr:`lcores (logical cores)` you are using in your system. 

* Recommended size: 16 GB or 2GB per lcore (whichever is higher)
* Maximum: 1 TiB per lcore, up to 256 lcores
* Minimum: 

  - For test environments: 1 GB or 256 MiB per lcore (whichever is higher)
  - For production environments: 4 GB or 0.5 GB per lcore (whichever is higher)

.. _system-requirements-disk:

Disk Requirements
-----------------

SSD
^^^
We highly recommend SSD and local disks. ScyllaDB is built for a large volume of data and large storage per node.
You can use up to 100:1 Disk/RAM ratio, with 30:1 Disk/RAM ratio as a good rule of thumb; for example, 30 TB of storage requires 1 TB of RAM.
We recommend a RAID-0 setup and a replication factor of 3 within the local datacenter (RF=3) when there are multiple drives.  

HDD
^^^
HDDs are supported but may become a bottleneck. Some workloads may work with HDDs, especially if they play nice and minimize random seeks. An example of an HDD-friendly workload is a write-mostly (98% writes) workload, with minimal random reads. If you use HDDs, try to allocate a separate disk for the commit log (not needed with SSDs).

Disk Space
^^^^^^^^^^
ScyllaDB is flushing memtables to SSTable data files for persistent storage. SSTables are periodically compacted to improve performance by merging and rewriting data and discarding the old one. Depending on compaction strategy, disk space utilization temporarily increases during compaction. For this reason, you should leave an adequate amount of free disk space available on a node.
Use the following table as a guidelines for the minimum disk space requirements based on the compaction strategy:

======================================  ===========  ============  
Compaction Strategy                     Recommended  Minimum
======================================  ===========  ============  
Size Tiered Compaction Strategy (STCS)  50%          70% 
--------------------------------------  -----------  ------------  
Leveled Compaction Strategy (LCS)       50%          80% 
--------------------------------------  -----------  ------------  
Time-window Compaction Strategy (TWCS)  50%          70%
--------------------------------------  -----------  ------------  
Incremental Compaction Strategy (ICS)   70%          80%
======================================  ===========  ============

Use the default ICS  unless you'll have a clear understanding that another strategy is better for your use case. More on :doc:`choosing a Compaction Strategy </architecture/compaction/compaction-strategies>`.
In order to maintain a high level of service availability, keep 50% to 20% free disk space at all times!

.. _system-requirements-network:

Network Requirements
====================

A network speed of 10 Gbps or more is recommended, especially for large nodes. To tune the interrupts and their queues, run the ScyllaDB setup scripts.

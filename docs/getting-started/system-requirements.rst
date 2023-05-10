===================
System Requirements
===================

.. _system-requirements-supported-platforms:

Supported Platforms
===================
ScyllaDB runs on 64-bit Linux. The x86_64 and AArch64 architectures are supported (AArch64 support includes AWS EC2 Graviton).

See :doc:`OS Support by Platform and Version </getting-started/os-support>` for information about 
supported operating systems, distros, and versions.

.. _system-requirements-hardware:

Hardware Requirements
=====================

It’s recommended to have a balanced setup. If there are only 4-8 :term:`Logical Cores <Logical Core (lcore)>`, large disks or 10Gbps networking may not be needed.
This works in the opposite direction as well.
ScyllaDB can be used in many types of installation environments.

To see which system would best suit your workload requirements, use the `ScyllaDB Sizing Calculator <https://price-calc.gh.scylladb.com/>`_ to customize ScyllaDB for your usage.



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

Use the default ICS (ScyllaDB Enterprise) or STCS  (ScyllaDB Open Source) unless you'll have a clear understanding that another strategy is better for your use case. More on :doc:`choosing a Compaction Strategy </architecture/compaction/compaction-strategies>`.
In order to maintain a high level of service availability, keep 50% to 20% free disk space at all times!

.. _system-requirements-network:

Network Requirements
====================

A network speed of 10 Gbps or more is recommended, especially for large nodes. To tune the interrupts and their queues, run the ScyllaDB setup scripts.


Cloud Instance Recommendations
===============================

Amazon Web Services (AWS)
--------------------------------

* The recommended instance types are :ref:`i3 <system-requirements-i3-instances>`, :ref:`i3en <system-requirements-i3en-instances>`, and :ref:`i4i <system-requirements-i4i-instances>`.
* We recommend using enhanced networking that exposes the physical network cards to the VM.

.. note::

  Some of the ScyllaDB configuration features rely on querying instance metadata. 
  Disabling access to instance metadata will impact using Ec2 Snitches and tuning performance.
  See `AWS - Configure the instance metadata options <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-options.html>`_ for more information.

.. _system-requirements-i3-instances:

i3 instances
^^^^^^^^^^^^
This family includes the High Storage Instances that provide very fast SSD-backed instance storage optimized for very high random I/O performance and provide high IOPS at a low cost. We recommend using enhanced networking that exposes the physical network cards to the VM.

i3 instances are designed for I/O intensive workloads and equipped with super-efficient NVMe SSD storage. It can deliver up to 3.3 Million IOPS.
An i3 instance is great for low latency and high throughput, compared to the i2 instances, the i3 instance provides storage that it's less expensive and denser along with the ability to deliver substantially more IOPS and more network bandwidth per CPU core.


===========================  ===========  ============  =====================
Model	                     vCPU         Mem (GB)      Storage (NVMe SSD)
===========================  ===========  ============  =====================
i3.xlarge	             4	          30.5	        0.950 TB
---------------------------  -----------  ------------  ---------------------
i3.2xlarge	             8	          61	        1.9 TB
---------------------------  -----------  ------------  ---------------------
i3.4xlarge	             16	          122           3.8 TB
---------------------------  -----------  ------------  ---------------------
i3.8xlarge	             32	          244           7.6 TB
---------------------------  -----------  ------------  ---------------------
i3.16xlarge	             64	          488	        15.2 TB
---------------------------  -----------  ------------  ---------------------
i3.metal New in version 2.3  72 :sup:`*`  512           8 x 1.9 NVMe SSD
===========================  ===========  ============  =====================

:sup:`*` i3.metal provides 72 logical processors on 36 physical cores

Source: `Amazon EC2 I3 Instances <https://aws.amazon.com/ec2/instance-types/i3/>`_

More on using ScyllaDB with `i3.metal vs i3.16xlarge <https://www.scylladb.com/2018/06/21/impact-virtualization-database/>`_ 

.. _system-requirements-i3en-instances:

i3en instances
^^^^^^^^^^^^^^
i3en instances have up to 4x the networking bandwidth of i3 instances, enabling up to 100 Gbps of sustained network bandwidth. 

i3en support is available for ScyllaDB Enterprise 2019.1.1 and higher and ScyllaDB Open Source 3.1 and higher. 


===========================  ===========  ============  =====================
Model	                     vCPU         Mem (GB)      Storage (NVMe SSD)
===========================  ===========  ============  =====================
i3en.large	             2	          16	        1 x 1,250 GB
---------------------------  -----------  ------------  ---------------------
i3en.xlarge	             4	          32	        1 x 2,500 GB
---------------------------  -----------  ------------  ---------------------
i3en.2xlarge	             8	          64	        2 x 2,500 GB
---------------------------  -----------  ------------  ---------------------
i3en.3xlarge	             12	          96            1 x 7,500 GB
---------------------------  -----------  ------------  ---------------------
i3en.6xlarge	             24	          192           2 x 7,500 GB
---------------------------  -----------  ------------  ---------------------
i3en.12xlarge	             48	          384	        4 x 7,500 GB
---------------------------  -----------  ------------  ---------------------
i3en.24xlarge	             96	          768	        8 x 7,500 GB
===========================  ===========  ============  =====================

All i3en instances have the following specs:

* 3.1 GHz all-core turbo Intel® Xeon® Scalable (Skylake) processors
* Intel AVX†, Intel AVX2†, Intel AVX-512†, Intel Turbo 
* EBS Optimized
* Enhanced Networking

See `Amazon EC2 I3en Instances <https://aws.amazon.com/ec2/instance-types/i3en/>`_ for details. 


.. _system-requirements-i4i-instances:

i4i instances
^^^^^^^^^^^^^^
i4i support is available for ScyllaDB Open Source 5.0 and later and ScyllaDB Enterprise 2021.1.10 and later.

===========================  ===========  ============  =====================
Model	                     vCPU         Mem (GB)      Storage (NVMe SSD)
===========================  ===========  ============  =====================
i4i.large	  	             2	          16	        1 x 468 GB
---------------------------  -----------  ------------  ---------------------
i4i.xlarge	             4	          32	        1 x 937 GB
---------------------------  -----------  ------------  ---------------------
i4i.2xlarge	 	             8	          64	        1 x 1,875 GB
---------------------------  -----------  ------------  ---------------------
i4i.4xlarge	             16	          128           1 x 3,750 GB
---------------------------  -----------  ------------  ---------------------
i4i.8xlarge	             32	          256           2 x 3,750 GB
---------------------------  -----------  ------------  ---------------------
i4i.16xlarge	             64	          512	        4 x 3,750 GB
---------------------------  -----------  ------------  ---------------------
i4i.32xlarge	             128	        1,024	      8 x 3,750 GB
---------------------------  -----------  ------------  ---------------------
i4i.metal	             128	         1,024	      8 x 3,750 GB
===========================  ===========  ============  =====================

All i4i instances have the following specs:

* 3.5 GHz all-core turbo Intel® Xeon® Scalable (Ice Lake) processors
* 40 Gbps bandwidth to EBS in the largest size and up to 10 Gbps in the four smallest sizes (twice that of i3 instances. Up to 75 Gbps networking bandwidth (three times more than I3 instances).
* AWS Nitro SSD storage


See  `Amazon EC2 I4i Instances <https://aws.amazon.com/ec2/instance-types/i4i/>`_ for specification details. 

See `ScyllaDB on the New AWS EC2 I4i Instances: Twice the Throughput & Lower Latency <https://www.scylladb.com/2022/05/09/scylladb-on-the-new-aws-ec2-i4i-instances-twice-the-throughput-lower-latency/>`_ to 
learn more about using ScyllaDB with i4i instances.


Im4gn and Is4gen instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ScyllaDB supports Arm-based Im4gn and Is4gen instances. See  `Amazon EC2 Im4gn and Is4gen instances <https://aws.amazon.com/ec2/instance-types/i4g/>`_ for specification details. 

Google Compute Engine (GCE)
-----------------------------------

Pick a zone where Haswell CPUs are found. Local SSD performance offers, according to Google, less than 1 ms of latency and up to 680,000 read IOPS and 360,000 write IOPS.
Image with NVMe disk interface is recommended, CentOS 7 for ScyllaDB Enterprise 2020.1 and older, and Ubuntu 20 for 2021.1 and later.  
(`More info <https://cloud.google.com/compute/docs/disks/local-ssd>`_)

Recommended instances types are `n1-highmem <https://cloud.google.com/compute/docs/general-purpose-machines#n1_machines>`_ and `n2-highmem <https://cloud.google.com/compute/docs/general-purpose-machines#n2_machines>`_

.. list-table::
   :widths: 30 20 20 30
   :header-rows: 1

   * - Model
     - vCPU
     - Mem (GB)
     - Storage (GB)
   * - n1-highmem-2
     - 2
     - 13
     - 375
   * - n1-highmem-4
     - 4
     - 26
     - 750
   * - n1-highmem-8
     - 8
     - 52
     - 1,500
   * - n1-highmem-16
     - 16
     - 104
     - 3,000
   * - n1-highmem-32
     - 32
     - 208
     - 6,000
   * - n1-highmem-64
     - 64
     - 416
     - 9,000

.. list-table::
   :widths: 30 20 20 30
   :header-rows: 1

   * - Model
     - vCPU
     - Mem (GB)
     - Storage (GB)
   * - n2-highmem-2
     - 2
     - 16
     - 375
   * - n2-highmem-4
     - 4
     - 32
     - 750
   * - n2-highmem-8
     - 8
     - 64
     - 1500
   * - n2-highmem-16
     - 16
     - 128
     - 3,000
   * - n2-highmem-32
     - 32
     - 256
     - 6,000
   * - n2-highmem-48
     - 48
     - 384
     - 9,000
   * - n2-highmem-64
     - 64
     - 512
     - 9,000
   * - n2-highmem-80
     - 80
     - 640
     - 9,000


Storage: each instance can support  maximum of 24 local SSD of 375 GB partitions each for a total of `9 TB per instance <https://cloud.google.com/compute/docs/disks>`_       

.. _system-requirements-azure:

Microsoft Azure
---------------

The `Lsv3-series <https://learn.microsoft.com/en-us/azure/virtual-machines/lsv3-series/>`_  of Azure Virtual Machines (Azure VMs) features high-throughput, low latency, directly mapped local NVMe storage. These VMs run on the 3rd Generation Intel® Xeon® Platinum 8370C (Ice Lake) processor in a hyper-threaded configuration.


.. list-table::
   :widths: 30 20 20 30
   :header-rows: 1

   * - Model
     - vCPU
     - Mem (GB)
     - Storage
   * - Standard_L8s_v3
     - 8
     - 64
     - 1 x 1.92 TB
   * - Standard_L16s_v3
     - 16
     - 128
     - 2 x 1.92 TB
   * - Standard_L32s_v3
     - 32
     - 256
     - 4 x 1.92 TB
   * - Standard_L48s_v3
     - 48
     - 384
     - 6 x 1.92 TB     
   * - Standard_L64s_v3
     - 64
     - 512
     - 8 x  1.92 TB
   * - Standard_L80s_v3
     - 80
     - 640
     - 10 x 1.92 TB
       
More on Azure Lsv3 instances `here <https://learn.microsoft.com/en-us/azure/virtual-machines/lsv3-series/>`_

Oracle Cloud Infrastructure (OCI)
----------------------------------------

An OCPU is defined as the CPU capacity equivalent of one physical core of an Intel Xeon processor with hyperthreading enabled. 
For Intel Xeon processors, each OCPU corresponds to two hardware execution threads, known as vCPUs.


.. list-table::
   :widths: 30 20 20 30
   :header-rows: 1

   * - Model
     - OCPU
     - Mem (GB)
     - Storage
   * - VM.DenseIO2.8
     - 8
     - 120
     - 6.4 TB
   * - VM.DenseIO2.16
     - 16
     - 240
     - 12.8 TB
   * - VM.DenseIO2.24
     - 24
     - 320 
     - 25.6 TB
   * - BM.DenseIO2.52 
     - 52 
     - 768 
     - 51.2 TB
   * - BM.HPC2.36 
     - 36 
     - 384 
     - 6.7 TB

.. include:: /rst_include/advance-index.rst

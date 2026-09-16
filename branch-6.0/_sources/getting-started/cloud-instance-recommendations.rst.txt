========================================================
Cloud Instance Recommendations for AWS, GCP, and Azure
========================================================

.. meta::
   :title:
   :description: Cloud Instance Recommendations for AWS, GCP, and Azure
   :keywords: ScyllaDB Cloud deloyment, AWS, GCE, Azure, ScyllaDB image, cloud instance, cloud support

You can run your ScyllaDB workloads on AWS, GCE, and Azure using a ScyllaDB image. This page includes the recommended instance types to be used with ScyllaDB.

.. TO DO: Add a link to the installation section for cloud deployments - when the page is added.

.. note:: 

    The following recommendations apply to self-managed deployments. See the `ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/>`_ to learn about ScyllaDB's fully managed database-as-a-service.

.. _system-requirements-aws:

Amazon Web Services (AWS)
-----------------------------

* The recommended instance types are :ref:`i3en <system-requirements-i3en-instances>`, and :ref:`i4i <system-requirements-i4i-instances>`.
* We recommend using enhanced networking that exposes the physical network cards to the VM.

.. note::

  Some of the ScyllaDB configuration features rely on querying instance metadata. 
  Disabling access to instance metadata will impact using Ec2 Snitches and tuning performance.
  See `AWS - Configure the instance metadata options <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-options.html>`_ for more information.

.. _system-requirements-i3en-instances:

i3en instances
^^^^^^^^^^^^^^
i3en instances have up to 4x the networking bandwidth of i3 instances, enabling up to 100 Gbps of sustained network bandwidth. 

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

.. _system-requirements-gcp:

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


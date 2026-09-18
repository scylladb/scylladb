=============================
Maximizing Scylla Performance
=============================

The purpose of this guide is to provide an overview of the best practices for maximizing the performance of Scylla, the next-generation NoSQL database.
Even though Scylla auto-tunes for optimal performance, users still need to apply best practices in order to get the most out of their Scylla deployments.



Performance Tips Summary
------------------------
If you are not planning to read this document fully, then here are the most important parts of this guide:

* use the best hardware you can reasonably afford
* install Scylla Monitoring Stack
* run scylla_setup script
* use Cassandra stress test
* expect to get at least 12.5K operations per second (OPS) per physical core for simple operations on selected hardware

Scylla Design Advantages
------------------------

Scylla is different from any other NoSQL database. It achieves the highest levels of performance and takes full control of the hardware by utilizing all of the server cores in order to provide strict SLAs for low-latency operations.
If you run Scylla in an over-committed environment, performance won't just be linearly slower &emdash; it will tank completely.

This is because Scylla has a reactor design that runs on all the (configured) cores and a scheduler that assumes a 0.5 ms tick.
Scylla does everything it can to control queues in userspace and not in the OS/drives.
Thus, it assumes the bandwidth that was measured by ``scylla_setup``.

It is not that difficult to get the best performance out of Scylla. Mostly, it is automatically tuned as long as you do not work against the system.
The remainder of this document contains the best practices to follow to make sure that Scylla keeps tuning itself and that your performance has maximum results.

Install Scylla Monitoring Stack
-------------------------------

Install and use the `Scylla Monitoring Stack <http://monitoring.docs.scylladb.com/>`_; it gives excellent additional value beyond performance.
If you don’t know what your bottleneck is, you have not configured your system correctly. The Scylla monitoring stack dashboards will help you sort this out.

With the recent addition of the `Scylla Advisor <http://monitoring.docs.scylladb.com/stable/advisor.html>`_ to the Scylla Monitoring Stack, it is even easier to find potential issues.

Install Scylla Manager
----------------------

Install and use `Scylla Manager <https://manager.docs.scylladb.com>` together with the `Scylla Monitoring Stack <http://monitoring.docs.scylladb.com/>`_.
Scylla Manager provides automated backups and repairs of your database.
Scylla Manager can manage multiple Scylla clusters and run cluster-wide tasks in a controlled and predictable way.
For example, with Scylla Manager you can control the intensity of a repair, increasing it to speed up the process, or lower the intensity to ensure it minimizes impact on ongoing operations.

Run scylla_setup
----------------

Before running Scylla, it is critical that the scylla_setup script has been executed.
Scylla doesn't require manual optimization &emdash; it is the task of the scylla_setup script to determine the optimal configuration.
But, if ``scylla_setup`` has not run, the system won’t be configured optimally. Refer to the :doc:`System Configuration </getting-started/system-configuration/>` guide for details.

Benchmarking Best Practices
---------------------------
Use a Representative Environment
================================

* Execute benchmarks on an environment that reflects your production.
* Benchmarking on the wrong environment can easily lead to an order of magnitude performance difference.
  For example, on a laptop, you could do 20K OPS while on a dedicated server you could easily do 200K OPS.
  So unless you have your production system running on a laptop, do not benchmark on a laptop.
* It is recommended to automate benchmarking with tools such as Terraform/Ansible to ensure repetitive tests run smoothly.
* If you are using shared hardware in a containerized/virtualized environment, note that a single guest can increase latency in the other guests.
* Make sure that you do not underprovision your load generators to prevent the load generators from becoming the bottleneck.

Use a Representative Data Model
===============================

Tools, such as :doc:`cassandra-stress </operating-scylla/admin-tools/cassandra-stress/>`, use a default data model that does not reflect the actions that you will perform in production.
For example, the cassandra-stress default data model has a replication factor set to 1 and uses the LOCAL_ONE as a consistency level.

Although cassandra_stress is convenient to get some initial performance impressions, it is critical to benchmark the same/similar data model that is used in production.
Therefore we recommend that you use a custom data model. For more information refer to the user mode section.

Use Representative Datasets
===========================

If you run the benchmark with a dataset that is smaller than your production data, you may have misleading or incorrect results due to the reduced number of I/O operations.
Therefore, it is critical to configure the size of the dataset to reflect your production dataset size.

Use a Representative Load
=========================

Run the benchmark using a load that represents, as closely as possible, the load you anticipate will be used in production.
This includes the queries submitted by the load generator.
The read/write ratio is important due to the overhead of compaction and finding the right data on disk.

Proper Warmup and Duration
==========================

When benchmarking, it is important that the system is given time to warm up.
This allows the database to fill the cache.
In addition, it is critical to run the benchmarks long enough so that at least one compaction is triggered.

Latency Tests vs. Throughput Tests
==================================

When performing a load test you need to differentiate between a latency test and a throughput test.
With a throughput test, you measure the maximum throughput by sending a new request as soon as the previous request completes.
With a latency test, you pin the throughput at a fixed rate.
In both cases, latency is measured.

Most engineers will start with a throughput test, but often a latency test is a better choice because the desired throughput is known e.g. 1M op/s.
Especially if your production depends on meeting the needs of the  SLA For example, the 99.99 percentile should have a latency less than 10ms.

Coordinated Omission
====================

A common problem when measuring latencies is the coordinated omission problem that causes the worst latencies to be omitted from the metrics.
As a result, it renders the higher percentiles useless.
A tool such as cassandra-stress prevents coordinated omissions from occurring.
For more information, read this `article <http://highscalability.com/blog/2015/10/5/your-load-generator-is-probably-lying-to-you-take-the-red-pi.html>`_.

Don’t Average Percentiles
=========================

Another typical problem with benchmarks is that when a load is generated by multiple load generators, the percentiles are averaged.
The correct way to determine the percentiles over multiple load generators is to merge the latency distribution of each load generator and then to determine the percentiles.
If this isn’t an option, then the next best alternative is to take the maximum (the p99, for example) of each of the load generators.
The actual p99 will be equal or smaller than the maximum p99. For more information on percentiles, read this `blog <http://pveentjer.blogspot.com/2017/08/percentiles-and-mean.html>`_.

Use Proven Benchmark Tools
==========================

Instead of rolling out custom benchmarks, use proven tools like cassandra-stress.
It is very flexible and takes care of coordinated omission.
Yahoo! Cloud Serving Benchmark (YCSB) is also an option, but needs to be configured correctly to prevent coordinated omission.
TLP-stress is not recommended because it suffers from coordinated omission.
When benchmarking make sure that cassandra-stress that is part of the Scylla distribution is used because it contains the shard aware drivers.

Use the Same Benchmark Tool
===========================

When benchmarking with different tools, it is very easy to run into an apples vs. oranges comparison.
When comparing products, use the same benchmark tool if possible.

Reproducible Results
====================

Make sure that the outcomes of the benchmark are reproducible; so execute your tests at least twice.
If the outcomes are different, then the benchmark results are unreliable.
One potential cause could be that the old data set of a previous benchmark has not been cleaned and this can make a performance difference for writes.

Query Recommendations
---------------------

Correct Data Modeling
=====================

The key to a well-performing system is using a properly defined data model.
A poorly structured data model can easily lead to an order of magnitude performance difference compared to that of a proper model.

A few of the most important tips:

* Choose the right partition key and clustering keys. Reduce or even eliminate the amount of data that needs to be scanned.
* Add indexes where appropriate.
* Partitions that are accessed more than others (hot partitions) should be avoided because it causes load imbalances between CPUs and nodes.
* :doc:`Large partitions </troubleshooting/large-partition-table/>`, :doc:`large rows and large cells </troubleshooting/large-rows-large-cells-tables/>` should be avoided because it can cause high latencies.

Use Prepared Statements
=======================

Prepared statements provide better performance because: parsing is done once, token/shard aware routing and less data is sent.
Apart from performance improvements, prepared statements also increase security because it prevents CQL injection.
Read more about `Stop Wasting Scylla’s CPU Time by Not Being Prepared <https://www.scylladb.com/2017/12/13/prepared-statements-scylla/>`_.

Use Paged Queries
=================

It is best to run queries that return a small number of rows.
However, if a query can return many rows, then the unpaged query can lead to a huge memory bubble. This will eventually cause Scylla to kill the query.
With a paged query, the execution collects a page's worth of data and new pages are retrieved on demand, leading to smaller memory bubbles.
Read about `More Efficient Query Paging <https://www.scylladb.com/2018/07/13/efficient-query-paging/>`_.

Use Workload Prioritization
===========================

In a typical application there are operational workloads that require low latency.
Sometimes these run in parallel with analytic workloads that process high volumes of data and do not require low latency.
With workload prioritization, one can prevent that the analytic workloads lead to an unwanted high latency on operational workload.
`Workload prioritization <https://enterprise.docs.scylladb.com/stable/using-scylla/workload-prioritization.html>`_ is only available with `Scylla Enterprise <https://enterprise.docs.scylladb.com/>`_.

Bypass Cache
============

There are certain workloads, e.g. analytical workloads, that scan through all data.
By default ScyllaDB will try to use cache, but since the data won’t be used again, it leads to cache pollution: i.e. good data gets pushes out of the cache and replaced by useless data,

As a consequence it can lead to bad latency on operational workloads due to increased rate of cache misses.
To prevent this problem, queries from analytical workloads can bypass the cache using the ‘bypass cache’ option.

See :ref:`Bypass Cache <bypass-cache>` for more information.

Batching
========

Multiple CQL queries to the same partition can be batched into a single query.
Imagine a query where the round trip time is 0.9 ms and the service time is  0.1 ms.
Without :ref:`batching <batch_statement>` the total latency would be 10x(0.9+0.1)=10.0 ms.
But if you created a batch of 10 instructions, the total time would be 0.9+10*0.1=1.9 ms.
This is 19% of the latency compared to no batching.

Driver Guidelines
-----------------

Use the :doc:`Scylla drivers </using-scylla/drivers/index>` that are available for Java, Python, Go, and C/C++.
They provide much better performance than third-party drivers because they are shard aware &emdash; they can route requests to the right CPU core (shard).
When the driver starts, it gets the topology of the cluster and therefore it knows exactly which CPU core should get a request.
Our latest shard-aware drivers also improve the efficiency of our Change Data Capture (CDC) feature.
If the Scylla drivers are not an option, make sure that at least a token aware driver is used so that one round trip is removed.

Check if there are sufficient connections created by the client, otherwise performance could suffer. The general rule is between 1-3 connections per Scylla CPU per node.

Hardware Guidelines
-------------------

CPU Core Count guidelines
=========================

Scylla, by default, will make use of all of its CPUs cores and is designed to perform well on powerful machines and as a consequence fewer machines are needed.
The recommended minimum number of CPU cores per node for operational workloads is 20.

The rule of thumb is that a single physical CPU can process 12.5 K queries per second with a payload of up to 1 KB.
If a single node should process 400K queries per second, at least 32 physical CPUs or 64 hyper-threaded cores are required.
In cloud environments hyper-threaded cores are often called virtual CPUs (vCPUs) or just cores.
So it is important to determine if a virtual CPU is the same as a physical CPU or if it is a hyper threaded CPU.

Scylla relies on temporarily spinning the CPU instead of blocking and waiting for data to arrive. This is done to reduce latency due to reduced context switching.
The drawback is that the CPUs are 100% utilized and you might falsely conclude that Scylla can’t keep up with the load.
Read more about :doc:`Scylla System Requirements </getting-started/system-requirements>`.

Memory Guidelines
=================
During startup, Scylla claims nearly all of the available memory for itself.
This is done for caching purposes to reduce the number of I/O operations.
So the more memory available, the better the performance.

Scylla recommends at least 2 GB of memory per core and a minimum of 16 GB of memory for a system (pick the highest value).
This means if you have a 64 core system, you should have at least 2x64=128 GB of memory.

The max recommended ratio of storage/memory for good performance is 30:1.
So for a system with 128 GB of memory, the recommended upper bound on the storage capacity is 3.8 TB per node of data.
To store 6 TB of data per node, the minimum recommended amount of memory is 200 GB.

Read more about  :doc:`Scylla System Requirements </getting-started/system-requirements>` or :doc:`Starting Scylla in a Shared Environment </getting-started/scylla-in-a-shared-environment/>`.


Storage Guidelines
==================

Scylla utilizes the full potential of modern NVMe SSDs; so the faster drive, the better the performance.
If there is more than one SSD, it is recommended to use them as RAID 0 for the best performance.
This is configured during ``scylla_setup`` and Scylla will create the RAID device automatically.
If there is limited SSD capacity, the commit log should be placed on the SSD.

The recommended file system is XFS because of its asynchronous appending write support and is the primary file system ScyllaDB is tested with.

As SSD’s wear out over time, it is recommended to re-run the iotune tool every few months. This helps Scylla’s IO scheduler to make the best performing choices.

Read more about :doc:`Scylla System Requirements </getting-started/system-requirements>`.

Networking Guidelines
=====================

For operational workloads the minimum recommended network bandwidth is 10 Gbps.
The scylla_setup script takes care of optimizing the kernel parameters, IRQ handling etc.

Read more about :ref:`Scylla Network Requirements <system-requirements-network>`.

Cloud Compute Instance Recommendations
--------------------------------------

Scylla is designed to utilize all hardware resources. Bare metal instances are preferred for best performance.

Read more about :doc:`Starting Scylla in a Shared Environment </getting-started/scylla-in-a-shared-environment/>`.

Image Guidelines
================

Use the Scylla provided AMI on AWS EC2 or the Google Cloud Platform (CGP) image, if possible.
They have already been correctly configured for use in those public cloud environments.

AWS
===

AWS EC2 i3, i3en, i4i and c5d bare metal instances are **highly recommended** because they are optimized for high I/O.

Read more about :ref:`Scylla Supported Platforms <system-requirements-supported-platforms>`.

If bare metal isn’t an option, use Nitro based instances and run with ‘host’ as tenancy policy to prevent the instance being shared with other VM’s.
If Nitro isn’t possible, then use instance storage over EBS.
If instance store is not an option, use an io2 IOPS provisioned SSD for best performance.
If there is limited support for instance storage, place the commit log there.
There is a new instance type available called `r5b <https://aws.amazon.com/blogs/aws/new-amazon-ec2-r5b-instances-providing-3x-higher-ebs-performance/>`_ that has high EBS performance.

GCP
===

For GCP we recommend n1/n2-highmem with local SSDs.

Read more at: https://docs.scylladb.com/getting-started/system-requirements/#google-compute-engine-gce

Azure
=====

For Azure we recommend the Lsv2 series. They feature high throughput and low latency and have local NVMe storage.
Read more about :ref:`Azure Requirements <system-requirements-azure>`.

Docker
======

When running in Docker platform, please use CPU pinning and host networking for best performance.
Read more about `The Cost of Containerization for Your Scylla <https://www.scylladb.com/2018/08/09/cost-containerization-scylla/>`_.

Kubernetes
==========

Just as with Docker, on a Kubernetes environment CPU pinning should be used as well.
In this case the pod should be pinned to a CPU so that no sharing takes place.

Read more about `Exploring Scylla on Kubernetes <https://www.scylladb.com/2018/03/29/scylla-kubernetes-overview/>`_.

Data Compaction
---------------

When records get updated or deleted, the old data eventually needs to be deleted. This is done using compaction.
The compaction settings can make a huge difference.

* Use the following :ref:`Compaction Strategy Matrix <CSM1>` to use the correct compaction strategy for your workload.
* ICS is an incremental compaction strategy that combines the low space amplification of LCS with the low write amplification of STCS. It is **only** available with Scylla Enterprise.
* If you have time series data, the TWCS should be used.

Read more about :doc:`Compaction Strategies </architecture/compaction/compaction-strategies>`

Consistency Level
-----------------

The consistency level determines how many nodes the coordinator should wait for, for the read or write is considered a success.
The consistency level is determined by the application based on the requirement for consistency, availability and performance.
The higher the consistency, the lower the availability and the performance.

For single data center setups a frequently used consistency level for both reads and writes is QUORUM.
It gives a nice balance between consistency and availability/performance.
For multi datacenter setups it is best to use LOCAL_QUORUM.

Read more about :doc:`Fault Tolerance </architecture/architecture-fault-tolerance/>`

Replication Factor
------------------

The default replication factor is set to 3 and in most cases this is a sensible default because it provides a good balance between performance and availability.
Keep in mind that a write will always be sent to all replicas, no matter the consistency level.

Asynchronous Requests
---------------------
Use asynchronous requests can help to increase the throughput of the system.
If the latency would be 1 ms, then 1 thread at most could do 1000 QPS. But if the service time an operation takes 100 us, with pipelining the throughput could increase to 10.000 QPS.

To prevent overload due to asynchronous requests, the drivers limit the number of pending requests to prevent overloading the server.

Read more about `Maximizing Performance via Concurrency While Minimizing Timeouts in Distributed Databases <https://www.scylladb.com/2019/11/20/maximizing-performance-via-concurrency-while-minimizing-timeouts-in-distributed-databases/>`_ for more information.

Conclusion
----------

Maximizing Scylla performance does require some effort even though Scylla will do its best to reduce the amount of configuration.
If the best practices are correctly applied, then most common performance problems will be prevented.

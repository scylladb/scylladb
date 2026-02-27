=====================
Benchmarking ScyllaDB
=====================

We provide Terraform and Ansible templates you can reuse to spin up a high-load 
test environment in your AWS account. 

Check out the repo here: `scylladb/1m-ops-demo <https://github.com/scylladb/1m-ops-demo>`_.

You can use this project as a starting point for your own demo, proof of concept, or benchmarking purposes.


ScyllaDB Cloud example
=======================

`This Terraform example <https://github.com/scylladb/1m-ops-demo/tree/main/scylladb-cloud>`_ 
shows you how to set up loader machines in AWS and run a high-throughput 
workload (configured for ~1M ops/sec, which you can customize) 
against a ScyllaDB Cloud cluster.

Terraform configuration files and instructions are available here:

.. raw:: html

   <p><iframe width="560" height="315" src="https://www.youtube.com/embed/SXwNVrU93IM?si=9MyoJwi3J4OtZ7ZN" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe></p>

ScyllaDB Enterprise (self-hosted on AWS) example
================================================

`This Terraform + Ansible example <https://github.com/scylladb/1m-ops-demo/tree/main/scylladb-enterprise>`_ 
shows you how to provision a self-hosted ScyllaDB Enterprise cluster on AWS, 
set up ScyllaDB Monitoring, and run a high-throughput workload from dedicated 
loader machines.

Terraform configuration files and instructions are available here:

DEMO UI
=======

`The DEMO UI <https://github.com/scylladb/1m-ops-demo/blob/main/README.md>`_ 
provides a guided way to spin up environments using the same Terraform and 
Ansible configuration that's available in the repo. It’s handy when you want 
a quick, simple experience.

The DEMO UI supports:

- ScyllaDB Cloud at 1M operations/second (requires an AWS account and a ScyllaDB Cloud account)
- ScyllaDB Enterprise at 1M operations/second (requires an AWS account)
- Scaling a cluster from 3 to 6 nodes (requires an AWS account)

.. raw:: html

   <p><iframe width="560" height="315" src="https://www.youtube.com/embed/-nPO9KeNydM?si=wkCRK5ZJ0cfz9h4u" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe></p>


More resources
==============

- `Best Practices for Benchmarking ScyllaDB <https://www.scylladb.com/2021/03/04/best-practices-for-benchmarking-scylla/>`_
- `How to Test and Benchmark Database Clusters <https://www.scylladb.com/2020/11/04/how-to-test-and-benchmark-database-clusters/>`_


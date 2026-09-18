======================================================
Integrate Scylla with the JanusGraph Graph Data System
======================================================

A graph data system (or graph database) is a database that uses a graph structure with nodes and edges to represent data. Edges represent relationships between nodes, and these relationships allow the data to be linked and for the graph to be visualized. It’s possible to use different storage mechanisms for the underlying data, and this choice affects the performance, scalability, ease of maintenance, and cost.

Some common use cases for graph databases are knowledge graphs, recommendation applications, social networks, and fraud detection.

`JanusGraph <https://janusgraph.org/>`_ is a scalable open-source graph database optimized for storing and querying graphs containing hundreds of billions of vertices and edges distributed across a multi-machine cluster. It stores graphs in adjacency list format, which means that a graph is stored as a collection of vertices with their edges and properties.

The data storage layer for JanusGraph is pluggable - you can choose from several storage systems, including ScyllaDB. 

In the ScyllaDB University lesson, `A Graph Data System Powered by ScyllaDB and JanusGraph - Part 1 <https://university.scylladb.com/courses/the-mutant-monitoring-system-training-course/lessons/a-graph-data-system-powered-by-scylladb-and-janusgraph/>`_ , you can learn more about using JanusGraph with ScyllaDB as the underlying data storage layer, and see a hands-on, step-by-step example. Another lesson, `ScyllaDB and JanusGraph - Part 2 <https://university.scylladb.com/courses/the-mutant-monitoring-system-training-course/lessons/a-graph-data-system-powered-by-scylladb-and-janusgraph-part-2/>`_ , covers the JanusGraph data model, how data is persisted using Scylla as a backend for JanusGraph, and an example of persistence in case of server failure. 

The list below contains integration projects using Scylla with JanusGraph. If you have tested your application with Scylla and want to publish the results, contact us using the `community forum <https://forum.scylladb.com>`_.

* `QOMPLX: Using Scylla with JanusGraph for Cybersecurity <https://www.scylladb.com/2021/03/11/qomplx-using-scylla-with-janusgraph-for-cybersecurity/>`_

* `Zeotap: A Graph of Twenty Billion IDs Built on Scylla and JanusGraph <https://www.scylladb.com/2020/05/14/zeotap-a-graph-of-twenty-billion-ids-built-on-scylla-and-janusgraph/>`_

* `Powering a Graph Data System with Scylla + JanusGraph <https://www.scylladb.com/2019/05/14/powering-a-graph-data-system-with-scylla-janusgraph/>`_

* `Scylla Shines in IBM’s Performance Tests for JanusGraph <https://www.scylladb.com/users/case-study-scylla-shines-in-ibms-performance-tests-for-janusgraph/>`_

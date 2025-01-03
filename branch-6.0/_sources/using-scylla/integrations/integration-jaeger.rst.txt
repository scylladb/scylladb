===================================
Integrate Scylla with Jaeger Server
===================================

`Jaeger Server <https://www.jaegertracing.io>`_ is an open-source distributed tracing system, originally developed by Uber Technologies,
aimed at monitoring and troubleshooting the performance of microservices-based applications.
It provides end-to-end visibility into the interactions among different components in a distributed architecture,
which is invaluable when dealing with complex, large-scale applications.

Now using an updated architecture, Jaeger relies on direct communication between applications and the Jaeger Collector,
which processes and stores trace data in backend storage systems such as Cassandra, Elasticsearch, or Kafka.
The Jaeger Query service offers a web-based UI and API for users to explore, visualize, and analyze trace data.
Jaeger also supports integration with other observability tools like Prometheus and Grafana,
making it a popular choice for monitoring modern distributed applications.

Jaeger Server `can also be run <https://github.com/jaegertracing/jaeger/tree/main/plugin/storage/scylladb>`_ with ScyllaDB as the storage backend, thanks to ScyllaDB's compatibility with Cassandra.
As a drop-in replacement for Cassandra, ScyllaDB implements the same protocol and provides a high-performance,
low-latency alternative. This compatibility allows Jaeger users to easily switch to ScyllaDB without making significant changes to their setup.

Using ScyllaDB as the storage backend for Jaeger Server can offer additional benefits,
such as improved performance, scalability, and resource efficiency.
This makes Jaeger even more effective for monitoring and troubleshooting distributed applications,
especially in high-traffic, demanding environments where a high-performance storage solution is critical.
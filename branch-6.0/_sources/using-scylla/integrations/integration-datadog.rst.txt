==============================
Integrate Scylla with DataDog
==============================

Datadog is a popular SaaS monitoring service. The default `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/stable/>`_ for Scylla is based on Prometheus and Grafana. You can export metrics from this stack and into DataDog, using it to monitor Scylla.

The way to do so is running a DataDog Agent to pull metrics from Prometheus and push it to DataDog as follows:

.. image:: images/datadog-arch.png
   :align: left
   :alt: scylla and datadog solution

If you are a Scylla Cloud user, you can export your cluster metrics to your own Prometheus and use the same method to export the metrics from Prometheus to DataDog, effectively monitoring your Scylla Cloud cluster with DataDog.


The list below contains integration projects using DataDog to monitor Scylla. If you have monitored Scylla with DataDog and want to publish the results, contact us using the `community forum <https://forum.scylladb.com>`_.

Additional Topics
-----------------

* `Monitoring Scylla with Datadog: A Tale about Datadog – Prometheus integration <https://www.scylladb.com/2019/10/02/monitoring-scylla-with-datadog-a-tale-about-datadog-prometheus-integration/>`_
* `Scylla Integration Page on Datadog's website <https://docs.datadoghq.com/integrations/scylla/>`_ 
* `Datadog Blog <https://www.datadoghq.com/blog/monitor-scylla-with-datadog/>`_

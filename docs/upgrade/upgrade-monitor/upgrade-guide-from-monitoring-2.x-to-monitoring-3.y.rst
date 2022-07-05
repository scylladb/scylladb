==============================================================
Upgrade Guide - Scylla Monitoring 2.x to Scylla Monitoring 3.x
==============================================================

This document is a step by step procedure for upgrading |mon_root| from version 2.x to 3.x


Switching from Scylla Monitoring 2.x to Scylla Monitoring 3.x is not fully backward compatible.
The changes affect dashboards' names and metrics.

Upgrade Procedure
=================

1. Validate node_exporter version
---------------------------------

Scylla Monitoring uses the node_exporter utility to collect OS-related metrics. By default, Scylla will install node_exporter version 0.17.
If you upgrade in the past from the older Scylla version (before Scylla 2.3), you should verify that you are running the correct node_exporter.
You can do that by running `node_exporter --version` on the machines running Scylla.

If you are running an older version of `node_exporter` you can use the helper script `node_exporter_install --force` that shipped with Scylla to force upgrade.

2. Install the new monitoring stack
-----------------------------------

#. Download the 3.x version from the `release <https://github.com/scylladb/scylla-monitoring/releases>`_ page.
#. Unzip it into a **different** directory.
#. Copy the targets files from the old stack to the new one, located on the ``prometheus/`` sub-directory:

   - scylla_servers.yml (for example ``cp /path/to/old/monitor/prometheus/scylla_servers.yml prometheus/``)
   - scylla_manager_servers.yml

    .. note::
       The targets files are no longer part of the release, make sure to copy them or the monitoring stack will not start.

#. Stop the old monitoring stack

    .. code-block:: bash

       ./kill-all.sh

#. Start the new monitoring stack

    .. code-block:: bash

       ./start-all.sh -d /prometheus-data-path

Validate the upgrade
--------------------
You should be able to see the graphs on the new stack. Make sure you see the graphs' history.

Because of the metrics, name change graphs will display correctly but with a different color as of the time of the upgrade.
This is only true for the transition phase.

Rollback
========
To rollback, simply kill the new stack, change to the old monitoring stack directory, and start it.


Related Links
=============

* |mon_root|
* :doc:`Upgrade</upgrade/index>`

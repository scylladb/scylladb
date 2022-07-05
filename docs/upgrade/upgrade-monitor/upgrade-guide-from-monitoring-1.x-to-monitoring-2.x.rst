==============================================================
Upgrade Guide - Scylla Monitoring 1.x to Scylla Monitoring 2.x
==============================================================

This document is a step by step procedure for upgrading |mon_root| from version 1.x to 2.x


Scylla monitoring stack uses `Prometheus <https://prometheus.io>`_ as its metrics database. The main differences between Scylla Monitoring 1.x and 2.x are moving from Prometheus version 1.x to Prometheus 2.x.
Since Prometheus is not backward compatible between these two versions, the upgrade procedure consists of running the two monitoring stack, old and new, in **parallel**, allowing the new stack to read metrics from the old one. This procedure will enable you to migrate your Scylla monitoring stack without losing historical metric data in the process.
Once the Prometheus retention time has passed, you can safely remove the old stack.

Upgrade Procedure
=================

1. Upgrade to the latest 1.x version
------------------------------------
Before starting the upgrade procedure, make sure you are running the `latest 1.x version <https://github.com/scylladb/scylla-monitoring/releases/>`_

2. Install the new monitoring stack
-----------------------------------

#. Download the 2.x version from the `release <https://github.com/scylladb/scylla-monitoring/releases>`_ page.
#. Unzip it into a **different** directory. This is important, as Prometheus is not backward compatible and would not be able to use the old data.
#. You can use the server definitions from the old monitoring stack by copying the target files from the old stack to the new one, located on the ``prometheus/`` directory:

   - scylla_servers.yml
   - scylla_manager_servers.yml
   - node_exporter_servers.yml
    
#. Start the new monitoring stack. If you are using Docker, make sure you are using ``-g`` ``-p`` and ``-m`` to specify different ports than the old monitoring stack. For example:

.. code-block:: bash

   ./start-all.sh -g 3001 -p 9091 -m 9094 -d /new-prometheus-data-path

.. note::
   Make sure to use the ``-d`` option, letting Prometheus keep its data **outside** the Docker container. Fail to do so will result in loss of historical data in case you restart the monitoring stack.

While the **old** monitoring stack keeps working, you can take the **new** stack up and down to make sure everything works correctly. Note that if you are using non-default ports setting, you should use the same for stopping the stack:

.. code-block:: bash

   ./kill-all.sh -g 3001 -p 9091 -m 9094


Validation
^^^^^^^^^^
Make sure the new monitoring stack is working before moving to 2.x as your main system. See that the graphs return data, and nodes are reachable.

3. Alerting Rules
-----------------
Note that alerting rules moved to a yml file format. Make sure that all defined rules are taken.

4. Moving to Prometheus 2.x
---------------------------
Monitoring stack Version 2.0 upgrade the Prometheus version from 1.8 to 2.3. This upgrade is not backward compatible.
Prometheus Migration is cover `here <https://Prometheus.io/docs/Prometheus/latest/migration/>`_.
Note that when using the docker containers, besides the data migration, the docker permissions were changed. This means that the permissions of the data directory will no longer work.

Do the following to allow the Prometheus in the new monitoring stack to read historical metrics from the old Prometheus:

a. Set the old system
^^^^^^^^^^^^^^^^^^^^^
The following steps will stop the **old** monitoring stack from reading new metrics while exposing an API for the **new** monitoring stack to read historical metrics from.

* In the **old** Prometheus `prometheus.yml.template` file, remove the ``alerting``, ``scrape_configs``, and ``rule_files`` sections, keeping only the ``external_labels`` section.
* Restart the **old** montioring stack with, ``kill-all.sh`` followed by ``start-all.sh`` with command line flag ``-b "-web.listen-address=:9111"``.

.. note::
   After this phase, the **old** monitoring stack will not be updated with new metrics. It will only serve as a data source of historical data for the **new** stack

b. Set the new system
^^^^^^^^^^^^^^^^^^^^^
The following step will allow the **new** monitoring system to read historical metrics from the old system.

* In the Prometheus `prometheus.yml.template` file add the following at the end:

.. code-block:: bash
                
   remote_read:
     - url: "http://{ip}:9111/api/v1/read"

Where {ip} is the ip of the old system.

* restart the **new** stack

Validate the upgrade
--------------------
You should be able to see the graphs on the new stack. Make sure you see the graphs' history.
By default, the Prometheus retention period is 15 days, so after that period, it is safe to take down the old system and remove the `remote_read` from the new Prometheus configuration.

Rollback
========
In the upgrade procedure, you set up a second monitoring stack. The old monitoring stack continues to work in parallel. To rollback, add back the Prometheus targets in the old system, and take down the new system.


Related Links
=============

* |mon_root|
* :doc:`Upgrade</upgrade/index>`
* `Prometheus Migration <https://Prometheus.io/docs/Prometheus/latest/migration/>`_

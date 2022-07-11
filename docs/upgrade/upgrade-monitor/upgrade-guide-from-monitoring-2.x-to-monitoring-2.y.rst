==============================================================
Upgrade Guide - Scylla Monitoring 2.x to Scylla Monitoring 2.y
==============================================================

This document is a step by step procedure for upgrading  |mon_root| from version 2.x to 2.y, for example, between 2.0 to 2.1.



      
Upgrade Procedure
=================

We recommend installing the new release next to the old one, running both in parallel, and making sure it is working as expected before uninstalling the old version.

Change to the directory you want to install the new Monitoring stack.
Download the latest release:
You can download the .zip or the .tar.

Install 2.y (The new version)
-----------------------------

.. code-block:: bash

                wget -L https://github.com/scylladb/scylla-monitoring/archive/scylla-monitoring-2.y.zip
                unzip scylla-monitoring-2.y.zip
                cd scylla-monitoring-scylla-monitoring-2.y/

Replace “y” with the new minor release number, for example, 2.1.zip

Setting the server's files
--------------------------

Copy the ``scylla_servers.yml`` and ``node_exporter_servers.yml`` from the version that is already installed.

.. code-block:: bash

                cp /path/to/monitoring/2.x/prometheus/scylla_servers.yml prometheus/
                cp /path/to/monitoring/2.x/prometheus/node_exporter_servers.yml prometheus/

Validate the new version is running the correct version
-------------------------------------------------------

Starting from Scylla-Monitoring version 2.2, you can run:

.. code-block:: bash

                ./start-all.sh --version
                
To validate the Scylla-Monitoring version.
 

Validate the version installed correctly
----------------------------------------

To validate that the Monitoring stack starts correctly, first in parallel to the current (2.x) stack.

.. code-block:: bash

                ./start-all.sh -p 9091 -g 3001 -m 9095

Browse to ``http://{ip}:9091``
And check the Grafana dashboard

Note that we are using different ports numbers for Grafana, Prometheus, and the Alertmanager.

.. caution::

   Important: do not use the local dir flag when testing!

When you are satisfied with the data in the dashboard, you can shut down the containers.

.. caution::

   Important: Do not kill the 2.x version that is currently running.

Use the following command to kill the containers:

.. code-block:: bash

                ./kill-all.sh -p 9091 -g 3001 -m 9095

You can start and stop the new 2.y version while testing.

Move to version 2.y (the new version)
-------------------------------------

Note: migrating will cause a few seconds of blackout in the system.

We assume that you are using external volume to store the metrics data.

Kill all containers
^^^^^^^^^^^^^^^^^^^

Follow the instruction on how to kill the 2.y version when in testing mode.

To kill the 2.x version containers, run:

.. code-block:: bash

                ./kill-all.sh

Start version 2.y in normal mode


From the new root of the `scylla-monitoring-scylla-monitoring-2.y` run

.. code-block:: bash

                ./start-all.sh -d /path/to/data/dir


Point your browser to ``http://{ip}:3000`` and see that the data is there.

Rollback to version 2.x
-----------------------


To rollback during the testing mode, kill the 2.y containers as explained above, and the system will continue to operate normally.

To rollback to version 2.x after you completed the moving to version 2.y (as shown above).
Run:

.. code-block:: bash

                ./kill-all.sh
                cd /path/to/scylla-grafana-2.x/
                ./start-all.sh -d /path/to/data/dir

Related Links
=============

* |mon_root|
* :doc:`Upgrade</upgrade/index>`

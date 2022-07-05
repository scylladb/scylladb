==============================================================
Upgrade Guide - Scylla Monitoring 3.x to Scylla Monitoring 3.y
==============================================================

This document is a step by step procedure for upgrading :doc:`Scylla Monitoring Stack </operating-scylla/monitoring/3.0/index>` from version 3.x to 3.y, for example, between 3.0 to 3.0.1.



Upgrade Procedure
=================

We recommend installing the new release next to the old one, running both in parallel, and making sure it is working as expected before uninstalling the old version.

Change to the directory you want to install the new Monitoring stack.
Download the latest release:
You can download the .zip or the .tar.

Install 3.y (The new version)
-----------------------------

.. code-block:: bash

                wget -L https://github.com/scylladb/scylla-monitoring/archive/scylla-monitoring-3.y.zip
                unzip scylla-monitoring-3.y.zip
                cd scylla-monitoring-scylla-monitoring-3.y/

Replace “y” with the new minor release number, for example, 3.0.1.zip

Setting the server's files
--------------------------

Copy the ``scylla_servers.yml`` and ``scylla_manager_servers.yml`` from the version that is already installed.

.. code-block:: bash

                cp /path/to/monitoring/3.x/prometheus/scylla_servers.yml prometheus/
                cp /path/to/monitoring/3.x/prometheus/scylla_manager_servers.yml.yml prometheus/

Validate the new version is running the correct version
-------------------------------------------------------

Starting from Scylla-Monitoring version 2.2, you can run:

.. code-block:: bash

                ./start-all.sh --version

To validate the Scylla-Monitoring version.


Validate the version installed correctly
----------------------------------------

To validate that the Monitoring stack starts correctly, first in parallel to the current (3.x) stack.

.. code-block:: bash

                ./start-all.sh -p 9091 -g 3001 -m 9095

Browse to ``http://{ip}:9091``
And check the Grafana dashboard

Note that we are using different port numbers for Grafana, Prometheus, and the Alertmanager.

.. caution::

   Important: do not use the local dir flag when testing!

When you are satisfied with the data in the dashboard, you can shut down the containers.

.. caution::

   Important: Do not kill the 3.x version that is currently running.

Killing the new 3.y Monitoring stack in testing mode
----------------------------------------------------

Use the following command to kill the containers:

.. code-block:: bash

                ./kill-all.sh -p 9091 -g 3001 -m 9095

You can start and stop the new 3.y version while testing.

Move to version 3.y (the new version)
-------------------------------------

Note: migrating will cause a few seconds of blackout in the system.

We assume that you are using external volume to store the metrics data.

Kill all containers
^^^^^^^^^^^^^^^^^^^

At this point you have two monitoring stacks running side by side, you should kill both before
continuing.

Kill the newer version that runs in testing mode by following the instructions on how to `Killing the new 3.y Monitoring stack in testing mode`_
in the previous section

kill the older 3.x version containers by running:

.. code-block:: bash

                ./kill-all.sh

Start version 3.y in normal mode


From the new root of the `scylla-monitoring-scylla-monitoring-3.y` run

.. code-block:: bash

                ./start-all.sh -d /path/to/data/dir


Point your browser to ``http://{ip}:3000`` and see that the data is there.

Rollback to version 3.x
-----------------------


To rollback during the testing mode, follow `Killing the new 3.y Monitoring stack in testing mode`_ as explained previously
and the system will continue to operate normally.

To rollback to version 3.x after you completed the moving to version 3.y (as shown above).
Run:

.. code-block:: bash

                ./kill-all.sh
                cd /path/to/scylla-grafana-3.x/
                ./start-all.sh -d /path/to/data/dir

Related Links
=============

* :doc:`Scylla Monitoring </operating-scylla/monitoring/index>`
* :doc:`Upgrade</upgrade/index>`

ScyllaDB will not Start
=========================

Problem
^^^^^^^

The scylla process stopped hours ago and it won’t start

How to Verify
^^^^^^^^^^^^^

Possible cause: The ScyllaDB process is managed by systemd, and systemd expects it to be able to fully start within a timeout. If this timeout is reached, systemd will kill the ScyllaDB process and try to start it again. If that is the case, you will see the following message in the ScyllaDB logs:

.. code-block:: shell

   systemd[1]: scylla-server.service start operation timed out. Terminating.


The timeout in systemd should be enough to always start the server. However, there may be extreme cases in which it is not enough.

Solution
^^^^^^^^

1. Locate the directory with the systemd files where the scylla-server.service resides. 

For Centos operating systems it is expected to be under ``/usr/lib/systemd/system/scylla-server.service`` 

For Ubuntu operating systems it is expected to be under ``/etc/systemd/system/scylla-server.service.d``

2. Create the following directory (if it does not exist)

Centos

.. code-block:: shell

   sudo mkdir /usr/lib/systemd/system/scylla-server.service

Ubuntu

.. code-block:: shell

   sudo mkdir /etc/systemd/system/scylla-server.service.d


3. Create a file inside that directory named ``10-timeout.conf``, with the following contents:

.. code-block:: shell

   [Service]
   TimeoutStartSec=9000​

6. Reload the systemd Daemon for the new configurations to take in effect.

.. code-block:: shell

   systemctl daemon-reload


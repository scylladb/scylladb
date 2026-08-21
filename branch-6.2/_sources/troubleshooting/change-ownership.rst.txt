ScyllaDB Fails to Start Due to Wrong Ownership Problems
========================================================

In cases where a ScyllaDB node fails to start because there is improper ownership, the following steps will help.

Phenomena
^^^^^^^^^

ScyllaDB node fails to start.

In cases where the ScyllaDB node fails to start, check ScyllaDB :doc:`logs </getting-started/logging/>`. If you see the following error message:
Could not access ``<PATH>: Permission denied std::system_error (error system:13, Permission denied)``.

For example:

.. code-block:: shell

   Jul 01 07:31:48 ip-172-16-12-198 scylla[12189]:  [shard 0] init - Could not access /var/lib/scylla/commitlog: std::system_error (error system:13, Permission denied)
   Jul 01 07:31:48 ip-172-16-12-198 scylla[12189]:  [shard 0] init - Could not access /var/lib/scylla/data: std::system_error (error system:13, Permission denied)
   Jul 01 07:31:48 ip-172-16-12-198 scylla[12189]:  [shard 0] seastar - Exiting on unhandled exception: std::system_error (error system:13, Permission denied)
   
Problem
^^^^^^^

The data directories ``/var/lib/scylla/data`` and ``/var/lib/scylla/commitlog`` exist but are not owned by the ScyllaDB user.

For example:

.. code-block:: shell

   [centos@ip-172-16-12-132 scylla]$ ls /var/lib/scylla/data
   total 4
   drwxr-xr-x 2 root root 4096 Jun 18 09:37 commitlog
   drwxr-xr-x 7 root root   97 Jun 18 09:37 data

In this example, the user root is the owner of the directories.


Solution
^^^^^^^^

1. Change the data directory ownership.

.. code-block:: shell

   sudo chown scylla:scylla /var/lib/scylla/data

.. code-block:: shell

   sudo chown scylla:scylla /var/lib/scylla/commitlog

2. Verify that the change completed successfully

.. code-block:: shell

   [centos@ip-172-16-12-132 scylla]$ ls /var/lib/scylla/data
   total 4
   drwxr-xr-x 2 scylla scylla 4096 Jun 18 09:37 commitlog
   drwxr-xr-x 7 scylla scylla   97 Jun 18 09:37 data

3. Start ScyllaDB node.

.. include:: /rst_include/scylla-commands-start-index.rst

4. Verify ScyllaDB node is working 

.. include:: /rst_include/scylla-commands-status-index.rst

.. include:: /troubleshooting/_common/ts-return.rst

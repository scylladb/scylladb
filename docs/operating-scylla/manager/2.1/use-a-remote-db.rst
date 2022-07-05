========================================
Use a remote database for Scylla Manager
========================================

.. include:: /operating-scylla/manager/_common/note-versions.rst


When you install Scylla Manager, it installs a local instance of Scylla to use as it's a database.
You are not required to use the local instance and can use Scylla Manager with a remote database.


**Requirements**

* Scylla cluster to be used as Scylla Manager data store.
* Package ``scylla-manager`` installed.

Remove local Scylla instance
============================

The ``scylla-manager`` package is a meta package that pulls both Scylla and Scylla Manager packages.
If you do not intend to use the local Scylla instance, you may remove it.

**Procedure**

1. Remove ``scylla-enterprise`` package.

.. code-block:: none

   sudo yum remove scylla-enterprise -y

2. Remove related packages. This would also remove the Scylla Manager.

.. code-block:: none

   sudo yum autoremove -y

3. Install the Scylla Manager client and server packages.

.. code-block:: none

   sudo yum install scylla-manager-client scylla-manager-server -y

Edit Scylla Manager configuration
=================================

Scylla Manager configuration file ``/etc/scylla-manager/scylla-manager.yaml`` contains a database configuration section.

.. code-block:: yaml

   # Scylla Manager database, used to store management data.
   database:
     hosts:
       - 127.0.0.1
   # Enable or disable client/server encryption.
   #  ssl: false
   #
   # Database credentials.
   #  user: user
   #  password: password
   #
   # Local datacenter name, specify if using a remote, multi-dc cluster.
   #  local_dc:
   #
   # Database connection timeout.
   #  timeout: 600ms
   #
   # Keyspace for management data, for create statement see /etc/scylla-manager/create_keyspace.cql.tpl.
   #  keyspace: scylla_manager
   #  replication_factor: 1


Using an editor, open the file and change relevant parameters.

**Procedure**

1. Edit the ``hosts`` parameter, change the IP address to the IP address or addresses of the remote cluster.

2. If client/server encryption is enabled, uncomment and set the ``ssl`` parameter to ``true``.
   Additional SSL configuration options can be set in the ``ssl`` configuration section.

   .. code-block:: yaml

      # Optional custom client/server encryption options.
      #ssl:
      # CA certificate used to validate server cert. If not set will use he host's root CA set.
      #  cert_file:
      #
      # Verify the hostname and server cert.
      #  validate: true
      #
      # Client certificate and key in PEM format. It has to be provided when
      # client_encryption_options.require_client_auth=true is set on server.
      #  user_cert_file:
      #  user_key_file

3. If authentication is needed, uncomment and edit the ``user`` and ``password`` parameters.

4. If the remote cluster contains more than one node:

   * If it's a single DC deployment, uncomment and edit the ``replication_factor`` parameter to match the required replication factor.
      Note that this would use a simple replication strategy (SimpleStrategy).
      If you want to use a different replication strategy, create ``scylla_manager`` keyspace (or another matching the ``keyspace`` parameter) yourself.
      Refer to :doc:`Scylla Architecture - Fault Tolerance </architecture/architecture-fault-tolerance>` for more information on replication.

   * If it's a multi DC deployment, create ``scylla_manager`` keyspace (or other matching the ``keyspace`` parameter) yourself.
      Uncomment and edit the ``local_dc`` parameter to specify the local datacenter.

Sample configuration of Scylla Manager working with a remote cluster with authentication and replication factor 3 could look like this.

.. code-block:: yaml

   database:
     hosts:
       - 198.100.51.11
       - 198.100.51.12
     user: user
     password: password
     replication_factor: 3

Setup Scylla Manager
====================

Continue with :ref:`setup script <install-run-the-scylla-manager-setup-script>`.

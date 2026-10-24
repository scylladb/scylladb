ScyllaDB Security Checklist
=============================
The ScyllaDB Security checklist is a list of security recommendations that should be implemented to protect your ScyllaDB cluster.


Enable Authentication
~~~~~~~~~~~~~~~~~~~~~

:doc:`Authentication </operating-scylla/security/authentication/>` is a security step to verify the identity of a client. When enabled, ScyllaDB requires all clients to authenticate themselves to determine their access to the cluster.


Enable Authorization
~~~~~~~~~~~~~~~~~~~~~

:doc:`Authorization </operating-scylla/security/enable-authorization/>` is a security step to verify the granted permissions of a client. When enabled, ScyllaDB will check all clients for their access permissions to the cluster objects(keyspaces, tables).


Role Base Access
~~~~~~~~~~~~~~~~

Role-Based Access Control (:doc:`RBAC</operating-scylla/security/rbac-usecase/>`), a method of reducing lists of authorized users to a few roles assigned to multiple users. RBAC is sometimes referred to as role-based security. It is recommended to:

* Set :ref:`roles <rbac-usecase-grant-roles-and-permissions>` per keyspace/table.

* Use the `principle of least privilege`_ per keyspace/table. :ref:`Start <rbac-usecase-use-case>` by granting no permissions to all roles, then grant read access only to roles who need it, write access to roles who need to write, etc. It's better to have more roles, each with fewer permissions.

..  _`principle of least privilege`: https://en.wikipedia.org/wiki/Principle_of_least_privilege#Details

Encryption on Transit, Client to Node and Node to Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Encryption on Transit protects your communication against a 3rd interception on the network connection.
Configure ScyllaDB to use TLS/SSL for all the connections. Use TLS/SSL to encrypt communication between ScyllaDB nodes and client applications.

.. only:: enterprise

    Starting with version 2023.1.1, you can run ScyllaDB Enterprise on FIPS-enabled Ubuntu, 
    which uses FIPS 140-2 certified libraries (such as OpenSSL, GnuTLS, and more) and Linux 
    kernel in FIPS mode.

* :doc:`Encryption Data in Transit Client to Node </operating-scylla/security/client-node-encryption>`

* :doc:`Encryption Data in Transit Node to Node </operating-scylla/security/node-node-encryption>`

Encryption at Rest :label-tip:`ScyllaDB Enterprise`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Encryption at Rest is available in `ScyllaDB Enterprise <https://enterprise.docs.scylladb.com/>`_.

Encryption at Rest protects the privacy of your user's data, reduces the risk of data breaches, and helps meet regulatory requirements. 
In particular, it provides an additional level of protection for your data persisted in storage or backup.

See `Encryption at Rest <https://enterprise.docs.scylladb.com/stable/operating-scylla/security/encryption-at-rest.html>`_ for details.

Reduce the Network Exposure
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ensure that ScyllaDB runs in a trusted network environment.
A best practice is to maintain a list of ports used by ScyllaDB and to monitor them to ensure that only trusted clients access those network interfaces and ports.
The diagram below shows a single datacenter cluster deployment, with the list of ports used for each connection type. You should periodically check to make sure that only these ports are open and that they are open to relevant IPs only.
Most of the ports' settings are configurable in the scylla.yaml file.
Also, see the list of ports used by ScyllaDB.

.. image:: Scylla-Ports2.png

The ScyllaDB ports are detailed in the table below. For ScyllaDB Manager ports, see the `ScyllaDB Manager Documentation <https://manager.docs.scylladb.com>`_.

.. include:: /operating-scylla/_common/networking-ports.rst




Audit System Activity
~~~~~~~~~~~~~~~~~~~~~

Auditing is available in `ScyllaDB Enterprise <https://enterprise.docs.scylladb.com/>`_.

Using the `auditing feature <https://enterprise.docs.scylladb.com/stable/operating-scylla/security/auditing.html>`_ allows the administrator to know “who did / looked at / changed what and when.” It also allows logging some or all the activities a user performs on the ScyllaDB cluster.

General Recommendations
~~~~~~~~~~~~~~~~~~~~~~~

* Update your cluster with the latest ScyllaDB version.
* Make sure to update your Operating System, and libraries are up to date.

* cluster_name - ``grep cluster_name /etc/scylla/scylla.yaml``
* seeds - ``grep seeds: /etc/scylla/scylla.yaml``
* endpoint_snitch - ``grep endpoint_snitch /etc/scylla/scylla.yaml``
* Scylla version - ``scylla --version``
* Authenticator - ``grep authenticator /etc/scylla/scylla.yaml``

.. Note:: 

   If ``authenticator`` is set to ``PasswordAuthenticator`` - increase the ``system_auth`` table replication factor.

   For example

   ``ALTER KEYSPACE system_auth WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : <new_replication_factor>};``

   It is recommended to set ``system_auth`` replication factor to the number of nodes in each DC.

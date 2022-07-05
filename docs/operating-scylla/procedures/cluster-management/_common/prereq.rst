* cluster_name - ``cat /etc/scylla/scylla.yaml | grep cluster_name``
* seeds - ``cat /etc/scylla/scylla.yaml | grep seeds:``
* endpoint_snitch - ``cat /etc/scylla/scylla.yaml | grep endpoint_snitch``
* Scylla version - ``scylla --version``
* Authentication status - ``cat /etc/scylla/scylla.yaml | grep authenticator``

.. Note:: 

   If ``authenticator`` is set to ``PasswordAuthenticator`` - increase the ``system_auth`` table replication factor.

   For example

   ``ALTER KEYSPACE system_auth WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : <new_replication_factor>};``

   It is recommended to set ``system_auth`` replication factor to the number of nodes in each DC.

Schema Mismatch
===============

Issue
^^^^^

A cqlsh operation failed with ``schema version mismatch detected`` error message.


For example

.. code-block:: shell

   OperationTimedOut: errors={'10.1.1.54': 'Request timed out while waiting for schema agreement. See Session.execute[_async](timeout) and Cluster.max_schema_agreement_wait.'}, last_host=10.1.1.54 
   Warning: schema version mismatch detected; check the schema versions of your nodes in system.local and system.peers.

Problem
^^^^^^^

One or more Scylla nodes have a schema mismatch.

How to Verify
^^^^^^^^^^^^^

Run :doc:`nodetool describecluster </operating-scylla/nodetool-commands/describecluster/>`


In the following example, one node, **172.17.0.3** has a different schema than the other two nodes. As a temporary state, this is fine, but if this state persists, it will fail CQL operations.

.. code-block:: cql

   Cluster Information:
   	   Name: Test Cluster
	   Snitch: org.apache.cassandra.locator.SimpleSnitch
	   DynamicEndPointSnitch: disabled
	   Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
	   Schema versions:
		   f04247d2-e2a6-3785-9fb8-8a57c7bdb25c: [172.17.0.1, 172.17.0.2]
                   b1c9af1d-4f90-39fb-a869-95eeb3da96af: [172.17.0.3]


Solution
^^^^^^^^

1. Wait for five minutes and run ``nodetool describecluster`` to verify that the schema is synced.

2. If there is still a mismatch error message execute a `rolling restart`_.

3. Verify schema is synced

For example:

.. code-block:: cql

   Cluster Information:
   	   Name: Test Cluster
	   Snitch: org.apache.cassandra.locator.SimpleSnitch
	   DynamicEndPointSnitch: disabled
	   Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
	   Schema versions:
		   1fd57629-6bae-3f23-97d1-ffa4208cc372: [172.17.0.1, 172.17.0.2, 172.17.0.3]

..  _`rolling restart`: /operating-scylla/procedures/config-change/rolling_restart/

Clients Table
==============

This document describes how to work with Scylla's client table, which provides real-time information on CQL clients **currently** connected to the Scylla cluster.

Viewing - List Active CQL connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: cql

   SELECT address, port FROM system.clients;

Example output:

.. code-block:: cql

  address    | port
  ------------+-------
  172.17.0.2 | 33296
  172.17.0.2 | 33298


Significant columns in the Client table:
  
================================================  =================================================================================
Parameter                                         Description
================================================  =================================================================================
address (PK)                                      Client's IP address
------------------------------------------------  ---------------------------------------------------------------------------------
port (CK)                                         Client's outgoing port number
------------------------------------------------  ---------------------------------------------------------------------------------
username                                          Username - when Authentication is used
------------------------------------------------  ---------------------------------------------------------------------------------
shard_id                                          Scylla node shard handing the connection
================================================  =================================================================================


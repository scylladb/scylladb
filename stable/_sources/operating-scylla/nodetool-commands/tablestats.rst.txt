Nodetool tablestats
====================
Provides statistics about one or more tables.

Syntax
-------
.. code-block:: console

   nodetool tablestats [<keyspace.cfname>...]

Typically, the keyspace and cfname are separated by a dot character. However, if either of them contains a dot, you can 
use a forward slash character (``/``) as a delimiter. For example:

.. code-block:: console

   nodetool tablestats my.keyspace/my.cfname

In addition, when you specify the keyspace without any cfname (to obtain statistics for all the tables in that keyspace), 
and the keyspace name contains a dot, you need to add ``/`` to the keyspace name to indicate that the entire string before 
the slash refers to a keyspace. For example:

.. code-block:: console

   nodetool tablestats my.keyspace/

If you don't add ``/``, nodetool will assume that "my" is the keyspace and "keyspace" is the cfname.

Example
--------

.. code-block:: shell

   nodetool tablestats

.. include:: /operating-scylla/nodetool-commands/_common/stats-output.rst

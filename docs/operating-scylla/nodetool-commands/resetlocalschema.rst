============================
Nodetool resetlocalschema
============================

This command reloads in-memory schema objects from disk. It recalculates 
the per-node and per-table schema versions. This can help you solve some 
cases of schema version disagreement that would otherwise require a node restart.

.. note::
    
    This command cannot be used to hot-update the node's schema from manually 
    placed schema SStables. If the schema on disk doesn't match the schema in 
    memory, such reload will likely fail.

Syntax
-------

.. code-block:: console

    nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
                [(-pp | --print-port)] [(-pw <password> | --password <password>)]
                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
                [(-u <username> | --username <username>)] resetlocalschema


Options
--------

* ``-h <host>`` or ``--host <host>`` - Node hostname or IP address.
* For other options, see :ref:`Nodetool generic options <nodetool-generic-options>`.




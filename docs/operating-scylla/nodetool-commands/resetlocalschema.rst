============================
Nodetool resetlocalschema
============================

This command is no longer supported. Schema version recalculation was removed
from ScyllaDB, and the command fails with
``Schema version recalculation is no longer supported``.

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




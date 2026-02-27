Nodetool drain
==============
**drain** - Flushes all memtables from a node to the SSTables that are on the disk. Scylla stops listening for connections from the client and other nodes. You need to restart Scylla after running this command. This command is usually executed before upgrading a node to a new version or before any maintenance action is performed. When you want to simply flush memtables to disk, use the :doc:`nodetool flush </operating-scylla/nodetool-commands/flush/>` command.

For example:

``nodetool drain``

See also

:doc:`Nodetool flush </operating-scylla/nodetool-commands/flush/>`

.. include:: nodetool-index.rst

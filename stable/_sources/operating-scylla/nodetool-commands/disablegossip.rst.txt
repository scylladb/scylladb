Nodetool disablegossip
======================
**disablegossip** - Disable the gossip protocol.

For example:

.. code-block:: shell

   nodetool disablegossip

To verify the gossip status, run ``nodetool statusgossip``. The result will be: ``not running``

See also

* :doc:`enablegossip </operating-scylla/nodetool-commands/enablegossip>`
* :doc:`statusgossip </operating-scylla/nodetool-commands/statusgossip>`

.. include:: nodetool-index.rst

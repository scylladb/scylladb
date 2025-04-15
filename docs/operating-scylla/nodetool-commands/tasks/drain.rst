Nodetool tasks drain
====================
**tasks drain** - Unregisters all finished local tasks from the module.
If a module is not specified, finished tasks in all modules are unregistered.

Syntax
-------
.. code-block:: console

   nodetool tasks drain [--module <module>]

Options
-------

* ``--module`` - if set, only the specified module is drained.

For example:

.. code-block:: shell

   > nodetool tasks drain --module repair

=====================
Nodetool statusbinary
=====================
**statusbinary** - Provides the status of native transport - CQL (binary protocol) In case that you don’t want to use CQL you can disable it using the :doc:`disablebinary <disablebinary>` command. By default CQL is active. 

For example:

::

    nodetool statusbinary

The result can be one of the following: ``running`` or ``not running``.

See also

* :doc:`enablebinary <enablebinary>`
* :doc:`disablebinary <disablebinary>`

.. include:: nodetool-index.rst

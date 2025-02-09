ScyllaDB Memory Usage
=====================

ScyllaDB memory usage might be larger than the data set used.

For example:

``The data size is 19GB, but ScyllaDB uses 220G memory.``


ScyllaDB uses available memory to cache your data. ScyllaDB knows how to dynamically manage memory for optimal performance, for example, if many clients connect to ScyllaDB, it will evict some data from the cache to make room for these connections, when the connection count drops again, this memory is returned to the cache.

To limit the memory usage you can start scylla with ``--memory`` parameter.
Alternatively, you can specify the amount of memory ScyllaDB should leave to the OS with ``--reserve-memory`` parameter. Keep in mind that the amount of memory left to the operating system needs to suffice external scylla modules.

On Ubuntu, edit the ``/etc/default/scylla-server``.

On Red Hat / CentOS, edit the ``/etc/sysconfig/scylla-server``.



For example:

.. code-block:: yaml 

   SCYLLA_ARGS="--log-to-syslog 1 --log-to-stdout 0 --default-log-level info --collectd-address=127.0.0.1:25826 --collectd=1 --collectd-poll-period 3000 --network-stack posix --memory 2G --reserve-memory 2G


:doc:`Knowledge Base </kb/index>`


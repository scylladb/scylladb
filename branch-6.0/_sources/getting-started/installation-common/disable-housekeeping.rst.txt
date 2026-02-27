
============================================
ScyllaDB Housekeeping and how to disable it
============================================

It is always recommended to run the latest version of Scylla Open Source or Scylla Enterprise. 
The latest stable release version is always available from the `Download Center <https://www.scylladb.com/download/>`_.

When you install Scylla, it installs by default two services: **scylla-housekeeping-restart** and **scylla-housekeeping-daily**. These services check for the latest Scylla version and prompt the user if they are using a version that is older than what is publicly available.
Information about your Scylla deployment, including the Scylla version currently used, as well as unique user and server identifiers, are collected by a centralized service.

To disable these service, update file ``/etc/scylla.d/housekeeping.cfg`` as follow: ``check-version: False``

See also:

* `ScyllaDB privacy <https://www.scylladb.com/privacy/>`_
* :doc:`Getting Started </getting-started/index>`





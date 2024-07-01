Error message "File system on /var/lib/scylla is not qualified for seastar AIO" on startup
==========================================================================================

During first startup, you may see the error message:

.. code-block:: sh

   File system on /var/lib/scylla is not qualified for seastar AIO

There can be two causes for this error:

* You are not using XFS on your data directory

  * Remedy: format the data directory with XFS

* You are using XFS, but the kernel version is too old; see below for affected kernels

  * Remedy: upgrade your kernel

ScyllaDB requires using the XFS filesystem, since it is the only Linux filesystem with good Asynchronous I/O support. In addition, Linux kernels before 3.15 did not have good asynchronous append support, which is required by ScyllaDB.

If you are using Red Hat Enterprise Linux or CentOS, use version 7.2 or higher of the operating system. These versions contain a kernel that provides the necessary support.

If you are using Ubuntu 14.04, install kernel package ``linux-image-generic-lts-vivid``. This package contains a kernel that provides the necessary support

.. include:: /troubleshooting/_common/ts-return.rst

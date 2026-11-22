Error message "File system on /var/lib/scylla is not qualified for seastar AIO" on startup
==========================================================================================

Old versions of ScyllaDB printed this error message during the first startup. Current versions print a different message, from the same check:

.. code-block:: sh

   Linux AIO is not supported by filesystem at /var/lib/scylla

Before it measures the disk, ``scylla_io_setup`` runs the ``iotune`` tool, which writes to a file in the data directory with asynchronous direct I/O, and checks that the file system only starts these writes, without making the writing thread wait. If the writes wait, ScyllaDB would stall on them, so ``scylla_io_setup`` stops. XFS and ext4 pass this check, but some file systems fail it: for example, btrfs does.

* Remedy: place the data directory on XFS.

XFS is the recommended file system for ScyllaDB's data in production. ScyllaDB is tested primarily on XFS, which handles asynchronous appending writes better than ext4 and btrfs. Linux kernels before 3.15 did not have good asynchronous append support on XFS either, but all the Linux distributions that ScyllaDB supports have newer kernels.

.. include:: /troubleshooting/_common/ts-return.rst

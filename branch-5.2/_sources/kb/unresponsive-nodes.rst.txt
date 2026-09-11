==============================
Scylla Nodes are Unresponsive
==============================

**Topic: Performance Analysis**

**Issue:**

Scylla nodes are unresponsive. They are shown as down, and I can't even establish new SSH connections to the cluster. The existing connections are slow.

**Environment: All**



Root Cause
----------

When Scylla is reporting itself as down, this may mean a Scylla-specific issue. But when the node as a whole starts reporting slowness and even establishing SSH connections is hard, that usually indicates a node level issue.

The most common cause is due to swap. There are two main situations we need to consider:

* The system has swap configured. If the system needs to swap pages, it may swap the Scylla memory, and future access to that memory will be slow.

* The system does not have swap configured. In that case the kernel may go on a loop trying to free pages without being able to so, becoming a CPU-hog which eventually stalls the Scylla and other processes from executing.



Resolution
----------

1. Ideally, a healthy system should not swap. Scylla pre-allocates 93% of the memory by default, and never uses more than that. It leaves the remaining 7% of the memory for other tasks including the Operating System. Check with the ``top`` utility if there are other processes running which are consuming a lot of memory.

   * If there are other processes running but they are not essential, we recommend moving them to other machines.
   * If there are other processes running and they are essential, the default reservation may not be enough.  Change the reservation following the steps below.


   Swap can be set up in several ways. One way to set up swap is detailed in the KB Article :doc:`How to Setup a Swap Space </kb/set-up-swap>`.


2. Having swap enabled and not using it is better than needing swap and not having it. Configure a file or partition to be used as swap for production deployments.

Change memory reservation
-------------------------

Add ``--reserve-memory [memory]`` to the scylla command line at:

* ``/etc/sysconfig/scylla-server`` (RHEL variants) or 
* ``/etc/defaults/scylla-server`` (Debian variants)

For example ``--reserve-memory 10G`` (will reserve 10G)


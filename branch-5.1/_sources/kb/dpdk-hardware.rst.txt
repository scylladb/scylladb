DPDK mode
=========
**Topic:	Planning and setup**

**Learn:	How to select networking hardware for Scylla**

**Audience:	Scylla administrators**

Scylla is designed to use the Seastar framework, which uses the Data Plane Development Kit (DPDK) to drive NIC hardware directly, instead of relying on the kernel’s network stack. This provides an enormous performance boost for Scylla. Scylla and DPDK also rely on the Linux “hugepages” feature to minimize overhead on memory allocations. DPDK is supported on a variety of high-performance network devices.

.. role:: raw-html(raw)
   :format: html

+------+---------------------------------------------------+--------------------------------------+
|Brand |Device                                             | Status                               |
+======+===================================================+======================================+
|Intel |ixgbe (82598..82599, X540, X550)                   | :raw-html:`<span class="icon-yes"/>` |
+------+---------------------------------------------------+--------------------------------------+
|Intel |i40e (X710, XL710)                                 | :raw-html:`<span class="icon-yes"/>` |
+------+---------------------------------------------------+--------------------------------------+

Scylla RPM packages are built with DPDK support, but the package defaults to POSIX networking mode (see Administration Guide). To enable DPDK, edit ``/etc/sysconfig/scylla-server`` and edit the following lines:

.. code-block:: ini

   # choose following mode: virtio, dpdk, posix
   NETWORK_MODE=posix

   # Ethernet device driver (dpdk)
   ETHDRV=

Reference
---------
`DPDK: Supported NICs <http://dpdk.org/doc/nics>`_

:doc:`Knowledge Base </kb/index>`

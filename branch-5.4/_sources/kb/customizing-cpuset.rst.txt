
==========================
Customizing CPUSET
==========================

CPUSET in the ``/etc/scylla.d/cpuset.conf`` file is automatically configured by running the ``scylla_setup`` 
(or ``scylla_cpuset_setup``) script. If you want to customize CPUSET, you must modify both the ``cpuset.conf`` 
and ``perftune.yaml`` files.

Note that the ``scylla-server`` service will generate ``/etc/scylla.d/perftune.yaml`` *only if* the file doesn't exist.

Examples
----------
Here are a few examples of possible configurations. In the examples, ``eth5`` is used as a NIC name and ``/var/lib/scylla`` as a data directory.

Example 1
^^^^^^^^^^

* 16 CPUs system.
* You want to run Scylla on CPUs 3, 4, 5, and have IRQs handled on the same CPUs, while allowing other apps on the same machine to benefit 
  from XFS/RFS/RPS from ``eth5``.

**cpuset.conf:**

.. code-block:: none

    CPUSET="--cpuset 3,4,5 "

**perftune.yaml:**

.. code-block:: none

    cpu_mask: '0xffff'
    dir:
    - /var/lib/scylla
    irq_cpu_mask: '0x38'
    nic:
    - eth5
    tune:
    - net
    - disks


Example 2
^^^^^^^^^^

* 16 CPUs system.
* You want to run Scylla on CPUs 3, 4, 5, and have IRQs handled on the same CPUs, and Scylla is going to be the only application 
  that will use ``eth5``.

**cpuset.conf:**

.. code-block:: none
    
    CPUSET="--cpuset 3,4,5 "


**perftune.yaml:**

.. code-block:: none

    cpu_mask: '0x38'
    dir:
    - /var/lib/scylla
    irq_cpu_mask: '0x38'
    nic:
    - eth5
    tune:
    - net
    - disks

Example 3
^^^^^^^^^^

* 16 CPUs system.
* You want to run Scylla on CPUs 3, 4, 5, and IRQs handled on CPUs 6,7,8, while allowing other apps on the same machine to benefit 
  from XFS/RFS/RPS from ``eth5``.


**cpuset.conf:**

.. code-block:: none
    
    CPUSET="--cpuset 3,4,5 "

**perftune.yaml:**

.. code-block:: none

    cpu_mask: '0xffff'
    dir:
    - /var/lib/scylla
    irq_cpu_mask: '0x1c0'
    nic:
    - eth5
    tune:
    - net
    - disks

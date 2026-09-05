============================================
Increase Scylla resource limits over systemd
============================================

**Topic: Increasing resource limits when Scylla runs and is managed via systemd**

**Audience: Scylla administrators**



Issue
-----

Updates to ``/etc/security/limits.d/scylla.conf`` do not have any effect. After a cluster rolling restart is completed, the Scylla limits listed under ``/proc/<PID>/limits`` are still the same or lower than what has been configured.

Root Cause
----------

When running under systemd, Scylla enforces the **LimitNOFILE** and **LimitNPROC** values under ``/lib/systemd/system/scylla-server.service``, where:

**LimitNOFILE** - Maximum number of file descriptors allowed to be opened simultaneously (defaults to 800000)

**LimitNPROC** - Maximum number of processes allowed to run in parallel (defaults to 8096)

Even though Scylla's provided defaults are suitable for most workloads, there may be situations on which these values may need to be overridden.

Before you start
----------------

The Linux kernel imposes an upper limit on the maximum number of file-handles that a process may allocate, which takes precedence over systemd. Such limit may be persistently increased by tuning the ``fs.nr_open`` parameter in the ``/etc/sysctl.conf`` file.

The ``fs.nr_open`` parameter default value is 1048576 (1024*1024) and it must be increased whenever it is required to overcome such limit.

As a rule of thumb, always ensure that the value of ``fs.nr_open`` is **equal or greater than** the maximum number of file-handles that Scylla may be able to consume.

1. To check the value of ``fs.nr_open`` run:

.. code-block:: shell

   sysctl fs.nr_open
   fs.nr_open = 1048576

2. Edit the file ``/etc/sysctl.conf`` as root, and increase the value of ``fs.nr_open``:

.. code-block:: shell

   sudo vi /etc/sysctl.conf
   fs.nr_open=5000000

3. Save the changes and apply the new setting to the running configuration:

.. code-block:: shell

   sudo sysctl -p



Solution
--------

1. To override Scylla limits on systemd, run:

.. code-block:: shell

   sudo systemctl edit scylla-server.service

2. Within the opened text editor, add the following lines and adjust the parameters as needed, e.g.:

**Warning**: Avoid setting the value of such fields to `Infinity` as this can lead to unpredictable behaviors. When adjusting the value of **LimitNOFILE** always set it to a value which is **equal or lower than** the value of ``fs.nr_open``

.. code-block:: shell

    [Service]
    LimitNOFILE=5000000

3. Restart Scylla:

.. code-block:: shell

    sudo systemctl restart scylla-server.service

This will create a configuration file named ``override.conf`` under the ``/etc/systemd/system/scylla-server.service.d`` folder. Whenever editing this file by hand manually, remember to run ``sudo systemctl daemon-reload`` before restarting Scylla, so that systemd reloads the changes.

4. To check the updated limits allowed by the Scylla process run:

.. code-block:: shell

   cat /proc/$(pidof scylla)/limits

References
----------

* `The Linux Kernel Documentation for /proc/sys/fs/*` <https://www.kernel.org/doc/Documentation/sysctl/fs.txt>
* `systemd.exec(5) manpage`

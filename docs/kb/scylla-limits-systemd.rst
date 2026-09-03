==============================================
Increase ScyllaDB resource limits over systemd
==============================================

**Topic: Increasing resource limits when ScyllaDB runs and is managed via systemd**

**Audience: ScyllaDB administrators**



Issue
-----

Updates to ``/etc/security/limits.d/scylla.conf`` do not have any effect. After a cluster rolling restart is completed, the ScyllaDB limits listed under ``/proc/<PID>/limits`` are still the same or lower than what has been configured.

Root Cause
----------

When running under systemd, ScyllaDB uses the resource limits set in ``/lib/systemd/system/scylla-server.service``, such as **LimitNPROC** — the maximum number of processes allowed to run in parallel (defaults to 8096). Limits configured through ``/etc/security/limits.d`` do not apply to systemd services.

Even though ScyllaDB's provided defaults are suitable for most workloads, there may be situations on which these values may need to be overridden.

Solution
--------

1. To override ScyllaDB limits on systemd, run:

.. code-block:: shell

   sudo systemctl edit scylla-server.service

2. Within the opened text editor, add the following lines and adjust the parameters as needed, e.g.:

.. code-block:: shell

    [Service]
    LimitNPROC=16384

3. Restart ScyllaDB:

.. code-block:: shell

    sudo systemctl restart scylla-server.service

This will create a configuration file named ``override.conf`` under the ``/etc/systemd/system/scylla-server.service.d`` folder. Whenever editing this file by hand manually, remember to run ``sudo systemctl daemon-reload`` before restarting ScyllaDB, so that systemd reloads the changes.

4. To check the updated limits allowed by the ScyllaDB process run:

.. code-block:: shell

   cat /proc/$(pidof scylla)/limits

References
----------

* `The Linux Kernel Documentation for /proc/sys/fs/*` <https://www.kernel.org/doc/Documentation/sysctl/fs.txt>
* `systemd.exec(5) manpage`

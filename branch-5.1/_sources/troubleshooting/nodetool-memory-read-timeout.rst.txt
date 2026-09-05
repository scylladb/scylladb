Nodetool fails with SocketTimeoutException 'Read timed out' 
===========================================================

This troubleshooting article describes what to do when Nodetool fails with a 'Read timed out' error.

Problem
^^^^^^^

When running any Nodetool command, users may see the following error:

.. code-block:: none

   Failed to connect to '127.0.0.1:7199' - SocketTimeoutException: 'Read timed out' 

Analysis
^^^^^^^^
Nodetool is a Java based application which requires memory. Scylla by default consumes 93% of the node’s RAM (for MemTables + Cache) and leaves 7% for other applications, such as nodetool.

If cases where this is not enough memory (e.g. small instances with ~64GB RAM or lower), Nodetool may not be able to run due to insufficient memory. In this case an out of memory (OOM) error may appear and scylla-jmx will not run.


Example
-------

The error you will see is similar to:

.. code-block:: none

   OpenJDK 64-Bit Server VM warning: INFO: os::commit_memory(0x00000005c0000000, 
   671088640, 0) failed; error='Cannot allocate memory' (err no=12) 


In order to check if the issue is scylla-jmx, use the following command (systemd-based Linux distribution) to check the status of the service:

.. code-block:: none

   sudo systemctl status scylla-jmx

If the service is running you will see something similar to:

.. code-block:: none

   sudo service scylla-jmx status
   ● scylla-jmx.service - Scylla JMX
      Loaded: loaded (/lib/systemd/system/scylla-jmx.service; disabled; vendor preset: enabled)
      Active: active (running) since Wed 2018-07-18 20:59:08 UTC; 3s ago
    Main PID: 256050 (scylla-jmx)
       Tasks: 27
      Memory: 119.5M
         CPU: 1.959s
      CGroup: /system.slice/scylla-jmx.service
              └─256050 /usr/lib/scylla/jmx/symlinks/scylla-jmx -Xmx384m -XX:+UseSerialGC -Dcom.sun.management.jmxremote.auth

If it isn't, you will see an error similar to:

.. code-block:: none

   sudo systemctl status scylla-jmx
   ● scylla-jmx.service - Scylla JMX
     Loaded: loaded (/usr/lib/systemd/system/scylla-jmx.service; disabled; vendor preset: disabled)
     Active: failed (Result: exit-code) since Thu 2018-05-10 10:34:15 EDT; 3min 47s ago
     Process: 1417 ExecStart=/usr/lib/scylla/jmx/scylla-jmx $SCYLLA_JMX_PORT $SCYLLA_API_PORT $SCYLLA_API_ADDR $SCYLLA_JMX_ADDR
     $SCYLLA_JMX_FILE $SCYLLA_JMX_LOCAL $SCYLLA_JMX_REMOTE $SCYLLA_JMX_DEBUG (code=exited, status=127)
     Main PID: 1417 (code=exited, status=127)

or 

.. code-block:: none

   sudo service scylla-jmx status
   ● scylla-jmx.service
     Loaded: not-found (Reason: No such file or directory)
     Active: failed (Result: exit-code) since Wed 2018-07-18 20:38:58 UTC; 12min ago
     Main PID: 141256 (code=exited, status=143)

You will need to restart the service or change the RAM allocation as per the Solution_ below. 

Solution
^^^^^^^^

There are two ways to fix this problem, one is faster but may not permanently fix the issue and the other solution is more robust. 

**The immediate solution**

.. code-block:: none

   service scylla-jmx restart 

.. note:: This is not a permanent fix as the problem might manifest again at a later time.

**The more robust solution**

1. Take the size of your node’s RAM, calculate 7% of that size, increase it by another 40%, and use this new size as your RAM requirement. 

   For example: on a GCP n1-highmem-8 instance (52GB RAM)

   * 7% would be ~3.6GB. 
   * Increasing it by ~40% means you need to increase your RAM ~5GB.
2. Open one of the following files (as per your OS platform):

   * Ubuntu: ``/etc/default/scylla-server``. 
   * Red Hat/ CentOS: ``/etc/sysconfig/scylla-server`` 
3. In the file you are editing, add to the ``SCYLLA_ARGS`` statement ``--reserve-memory 5G`` (the amount you calculated above). Save and exit.
4. Restart Scylla server 

.. code-block:: none

   sudo systemctl restart scylla-server


.. note:: If the initial calculation and reserve memory is not enough and problem persists and/or reappears, repeat the procedure from step 2 and increase the RAM in 1GB increments.


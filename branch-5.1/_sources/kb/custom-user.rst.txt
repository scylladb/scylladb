Run Scylla and supporting services as a custom user:group
=========================================================
**Topic: Planning and setup**
By default, Scylla runs as user ``scylla`` in group ``scylla``. The following procedure will allow you to use a custom user and group to run Scylla.
1. Create the new user and update file permissions

.. code-block:: sh
   
   useradd test
   groupadd test
   usermod test -G test
   chown -R test:test /var/lib/scylla
   
2. Edit ``/etc/sysconfig/scylla-server`` and change the USER and GROUP

.. code-block:: sh

   USER=test
   GROUP=test

3. Edit ``/etc/systemd/system/multi-user.target.wants/scylla-server.service`` 

.. code-block:: sh

   User=test

4. Edit ``/etc/systemd/system/multi-user.target.wants/node-exporter.service``

.. code-block:: sh
   
   User=test
   Group=test

5. Edit /usr/lib/systemd/system/scylla-jmx.service

.. code-block:: sh
   
   User=test
   Group=test

6. Reload the daemon settings and start Scylla and node_exporter

.. code-block:: sh
   
   systemctl daemon-reload
   systemctl start scylla-server
   systemctl start node-exporter

At this point, all  services should be started as test:test user:

.. code-block:: sh
   
   test      8760     1 11 14:42 ?        00:00:01 /usr/bin/scylla --log-to-syslog 1 --log-to-std ...
   test      8765     1 12 14:42 ?        00:00:01 /opt/scylladb/jmx/symlinks/scylla-jmx -Xmx256m ...
   test     13638     1  0 14:30 ?        00:00:00 /usr/bin/node_exporter --collector.interrupts

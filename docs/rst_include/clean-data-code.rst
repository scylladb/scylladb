
.. code-block:: sh

   sudo rm -rf /var/lib/scylla/data
   sudo find /var/lib/scylla/commitlog -type f -delete
   sudo find /var/lib/scylla/hints -type f -delete
   sudo find /var/lib/scylla/view_hints -type f -delete

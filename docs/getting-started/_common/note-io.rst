.. note::

   It's important to keep I/O scheduler configuration in sync on nodes with the same hardware. 
   That's why we recommend skipping running scylla_io_setup when provisioning a new node with exactly the same hardware setup as existing nodes in the cluster. 

   Instead, we recommend to copy the following files from an existing node to the new node after running scylla_setup and restart scylla-server service (if it is already running):
     * /etc/scylla.d/io.conf
     * /etc/scylla.d/io_properties.yaml

   Using different I/O scheduler configuration may result in unnecessary bottlenecks.

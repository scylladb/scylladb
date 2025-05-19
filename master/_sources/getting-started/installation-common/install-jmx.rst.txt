
======================================
Install scylla-jmx Package
======================================

scylla-jmx is an optional package and is not installed by default.
If you need JMX server, you can still install it from scylla-jmx GitHub page.

.. tabs::

   .. group-tab:: Debian/Ubuntu
        #. Download .deb package from scylla-jmx page.

            Access to https://github.com/scylladb/scylla-jmx, select latest
            release from "releases", download a file end with ".deb".

        #. (Optional) Transfer the downloaded package to the install node.

            If the pc from which you downloaded the package is different from
            the node where you install scylladb, you will need to transfer
            the files to the node.

        #. Install scylla-jmx package.

            .. code-block:: console
    
               sudo apt install -y ./scylla-jmx_<version>_all.deb


   .. group-tab:: Centos/RHEL

        #. Download .rpm package from scylla-jmx page.

            Access to https://github.com/scylladb/scylla-jmx, select latest
            release from "releases", download a file end with ".rpm".

        #. (Optional) Transfer the downloaded package to the install node.

            If the pc from which you downloaded the package is different from
            the node where you install scylladb, you will need to transfer
            the files to the node.

        #. Install scylla-jmx package.

            .. code-block:: console
    
               sudo yum install -y ./scylla-jmx-<version>.noarch.rpm


   .. group-tab:: Install without root privileges

        #. Download .tar.gz package from scylla-jmx page.

            Access to https://github.com/scylladb/scylla-jmx, select latest
            release from "releases", download a file end with ".tar.gz".

        #. (Optional) Transfer the downloaded package to the install node.

            If the pc from which you downloaded the package is different from
            the node where you install scylladb, you will need to transfer
            the files to the node.

        #. Install scylla-jmx package.

            .. code:: console
    
                tar xpf scylla-jmx-<version>.noarch.tar.gz
                cd scylla-jmx
                ./install.sh --nonroot

Next Steps
-----------

* :doc:`Configure ScyllaDB </getting-started/system-configuration>`
* Manage your clusters with `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_
* Monitor your cluster and data with `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_
* Get familiar with ScyllaDB’s :doc:`command line reference guide </operating-scylla/nodetool>`.
* Learn about ScyllaDB at `ScyllaDB University <https://university.scylladb.com/>`_

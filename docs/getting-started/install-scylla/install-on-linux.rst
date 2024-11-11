.. The |RHEL_EPEL| variable needs to be adjuster per release, depending on support for RHEL.
.. 5.2 supports Rocky/RHEL 8 only
.. 5.4 supports Rocky/RHEL 8 and 9
.. |RHEL_EPEL_8| replace:: https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
.. |RHEL_EPEL_9| replace:: https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm

======================================
Install ScyllaDB Linux Packages
======================================

We recommend installing ScyllaDB using :doc:`ScyllaDB Web Installer for Linux </getting-started/installation-common/scylla-web-installer/>`,
a platform-agnostic installation script, to install ScyllaDB on any supported Linux platform.
Alternatively, you can install ScyllaDB using Linux packages. 

This article will help you install ScyllaDB on Linux using platform-specific packages.

Prerequisites
----------------

* Ubuntu, Debian, CentOS, or RHEL (see :doc:`OS Support by Platform and Version </getting-started/os-support>`
  for details about supported versions and architecture)
* Root or ``sudo`` access to the system
* Open :ref:`ports used by ScyllaDB <networking-ports>`
* (CentOS and RHEL only) Removing Automatic Bug Reporting Tool (ABRT) if installed before installing ScyllaDB, 
  as it may conflict with ScyllaDB coredump configuration:

  .. code-block:: console

     sudo yum remove -y abrt

.. The last requirement may need to be removed. See https://github.com/scylladb/scylladb/issues/14488.

Install ScyllaDB
--------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        #. Install a repo file and add the ScyllaDB APT repository to your system.

            .. code-block:: console
    
               sudo mkdir -p /etc/apt/keyrings


            .. code-block:: console
    
               sudo gpg --homedir /tmp --no-default-keyring --keyring /etc/apt/keyrings/scylladb.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys a43e06657bac99e3

            .. code-block:: console
               :substitutions:
    
               sudo wget -O /etc/apt/sources.list.d/scylla.list http://downloads.scylladb.com/deb/debian/|UBUNTU_SCYLLADB_LIST|


        #. Install ScyllaDB packages.

            .. code-block:: console
    
               sudo apt-get update
               sudo apt-get install -y scylla 

            Running the command installs the latest official version of ScyllaDB Open Source.
            To install a specific patch version, list all the available patch versions:
          
            .. code-block:: console
    
               apt-cache madison scylla

            Then install the selected patch version:

            .. code-block:: console
    
               apt-get install scylla{,-server,-tools,-tools-core,-kernel-conf,-node-exporter,-conf,-python3}=<your patch version>
          
            The following example shows installing ScyllaDB 5.2.3.

            .. code-block:: console
               :class: hide-copy-button
    
               apt-cache madison scylla
               scylla | 5.2.3-0.20230608.ea08d409f155-1 | https://downloads.scylladb.com/downloads/scylla/deb/debian-ubuntu/scylladb-5.2 stable/main amd64 Packages
               scylla | 5.2.2-0.20230521.9dd70a58c3f9-1 | https://downloads.scylladb.com/downloads/scylla/deb/debian-ubuntu/scylladb-5.2 stable/main amd64 Packages
               scylla | 5.2.1-0.20230508.f1c45553bc29-1 | https://downloads.scylladb.com/downloads/scylla/deb/debian-ubuntu/scylladb-5.2 stable/main amd64 Packages
               scylla | 5.2.0-0.20230427.429b696bbc1b-1 | https://downloads.scylladb.com/downloads/scylla/deb/debian-ubuntu/scylladb-5.2 stable/main amd64 Packages

            .. code-block:: console
               :class: hide-copy-button
    
               apt-get install scylla{,-server,-tools,-tools-core,-kernel-conf,-node-exporter,-conf,-python3}=5.2.3-0.20230608.ea08d409f155-1


        #. (Ubuntu only) Set Java 11.

            .. code-block:: console
    
               sudo apt-get update
               sudo apt-get install -y openjdk-11-jre-headless
               sudo update-java-alternatives --jre-headless -s java-1.11.0-openjdk-amd64


   .. group-tab:: Centos/RHEL

        #. Install the EPEL repository.

           CentOS:

            .. code-block:: console
    
               sudo yum install epel-release


           Rocky/RHEL 8

            .. code-block:: console
               :substitutions:
    
               sudo yum -y install |RHEL_EPEL_8|


           Rocky/RHEL 9

            .. code-block:: console
               :substitutions:
    
               sudo yum -y install |RHEL_EPEL_9|

        #. Add the ScyllaDB RPM repository to your system.

            .. code-block:: console
               :substitutions:
    
               sudo curl -o /etc/yum.repos.d/scylla.repo -L http://downloads.scylladb.com/rpm/centos/|CENTOS_SCYLLADB_REPO|

        #. Install ScyllaDB packages.

            .. code-block:: console
    
               sudo yum install scylla

            Running the command installs the latest official version of ScyllaDB Open Source.
            Alternatively, you can to install a specific patch version:

            .. code-block:: console
    
               sudo yum install scylla-<your patch version>

            Example: The following example shows the command to install ScyllaDB 5.2.3.

            .. code-block:: console
               :class: hide-copy-button
    
               sudo yum install scylla-5.2.3

(Optional) Install scylla-jmx
-------------------------------

    scylla-jmx is an optional package and is not installed by default.
    If you need JMX server, see :doc:`Install scylla-jmx Package </getting-started/installation-common/install-jmx>`.



.. include:: /getting-started/_common/setup-after-install.rst

Next Steps
------------

* :doc:`Configure ScyllaDB </getting-started/system-configuration>`
* Manage your clusters with `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_
* Monitor your cluster and data with `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_
* Get familiar with ScyllaDB’s :doc:`command line reference guide </operating-scylla/nodetool>`.
* Learn about ScyllaDB at `ScyllaDB University <https://university.scylladb.com/>`_

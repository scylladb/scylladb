=======================================================
Scylla Manager Upgrade - Scylla Manager 2.x.a to 2.y.b
=======================================================

This document describes upgrade guide between two following *Minor* or *Patch* releases of Scylla Manager 2.x.y

.. toctree::
   :maxdepth: 2

Applicable versions
===================

This guide covers upgrading Scylla Manager version 2.x.a to version 2.y.b, on the following platforms:

- Red Hat Enterprise Linux, version 7
- CentOS, version 7
- Debian, version 9
- Ubuntu, versions 16.04, 18.04

Upgrade Procedure
=================

.. note:: In Scylla Manager 2.x.a new component called Scylla Manager Agent is introduced which is running on each scylla node in the cluster as a sidecar. Upgrading this component means commands have to be executed for each node separately.

Upgrade procedure for the Scylla Manager includes upgrade of three components server, client, and the agent. Entire cluster shutdown is NOT needed. Scylla will be running while the manager components are upgraded. Overview of the required steps:

- Stop all Scylla Manager tasks (or wait for them to finish)
- Stop the Scylla Manager Server 2.x.a
- Stop the Scylla Manager Agent 2.x.a on all nodes
- Upgrade the Scylla Manager Server and Client to 2.y.b
- Upgrade the Scylla Manager Agent to 2.y.b on all nodes
- Run `scyllamgr_agent_setup` script on all nodes
- Reconcile configuration files
- Start the Scylla Manager Agent 2.y.b on all nodes
- Start the Scylla Manager Server 2.y.b
- Validate status of the cluster

Upgrade steps
=============

Stop all Scylla Manager tasks (or wait for them to finish)
----------------------------------------------------------

**On the Manager Server** check current status of the manager tasks:

.. code:: sh

    sctool task list -c <cluster>

None of the listed tasks should have status in RUNNING.

Stop the Scylla Manager Server 2.x.a
------------------------------------

**On the Manager Server** instruct Systemd to stop the server process:

.. code:: sh

    sudo systemctl stop scylla-manager

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager

It should have a status of *“Active: inactive (dead)”*.

Stop the Scylla Manager Agent 2.x.a on all nodes
------------------------------------------------

**On each scylla node** in the cluster run:

.. code:: sh

    sudo systemctl stop scylla-manager-agent

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: inactive (dead)”*.

Upgrade the Scylla Manager Server and Client to 2.y.b
-----------------------------------------------------

**Before you Begin**

Confirm that the settings for ``scylla-manager.repo`` are correct.

**On the Manager Server** display the contents of the scylla-manager.repo and confirm the version displayed is the version you want to upgrade to. This example uses Scylla Manager 2.1, but your display should show the repo you have installed. 

CentOS, Red Hat:

.. code:: sh

   cat /etc/yum.repos.d/scylla-manager.repo

   [scylla-manager-2.1] name=Scylla Manager for Centos - $basearch baseurl=http://downloads.scylladb.com/downloads/scylla-manager/rpm/centos/scylladb-manager-2.1/$basearch/ enabled=1 gpgcheck=0


Debian, Ubuntu:

.. code:: sh

   cat /etc/apt/sources.list/scylla-manager.repo

   [scylla-manager-2.1] name=Scylla Manager for Centos - $basearch baseurl=http://downloads.scylladb.com/downloads/scylla-manager/rpm/centos/scylladb-manager-2.1/$basearch/ enabled=1 gpgcheck=0


**On the Manager Server** instruct package manager to update server and the client:

CentOS, Red Hat:

.. code:: sh

    sudo yum update scylla-manager-server scylla-manager-client -y


Debian, Ubuntu:

.. code:: sh

    sudo apt-get update
    sudo apt-get install scylla-manager-server scylla-manager-client -y

.. note:: When using apt-get, if a previous version of the Scylla Manager package had a modified configuration file, you will be asked what to do with this file during the installation process. In order to keep both files for reconciliation (covered later in the procedure), select the "keep your currently-installed version" option when prompted. 

Upgrade the Scylla Manager Agent to 2.y.b on all nodes
------------------------------------------------------

**On each scylla node** instruct package manager to update the agent:

CentOS, Red Hat:

.. code:: sh

    sudo yum update scylla-manager-agent -y

Debian, Ubuntu:

.. code:: sh

    sudo apt-get update
    sudo apt-get install scylla-manager-agent -y

.. note:: With apt-get, if a previous version of the package had a modified configuration file, you will be asked during installation what to do with it. Please select "keep your currently-installed version" option to keep both previous and new default configuration file for later reconciliation.

Run `scyllamgr_agent_setup` script on all nodes
-----------------------------------------------

.. note:: Script mentioned in this section is added in version 2.0.2 so it won't be available for earlier versions.

This step requires sudo rights:

.. code:: sh

    $ sudo scyllamgr_agent_setup
    Do you want to create scylla-helper.slice if it does not exist?
    Yes - limit Scylla Manager Agent and other helper programs memory. No - skip this step.
    [YES/no] YES
    Do you want the Scylla Manager Agent service to automatically start when the node boots?
    Yes - automatically start Scylla Manager Agent when the node boots. No - skip this step.
    [YES/no] YES

First step relates to limiting resources that are available to the agent and second
instructs systemd to run agent on node restart.

Reconcile configuration files
-----------------------------

Upgrades can create changes to the structure and values of the default yaml configuration file. If the previous version's configuration file was modified with custom values, this could result in a conflict. The upgrade procedure can't resolve this without help from an administrator. If you followed instructions from the upgrade packages sections of this document, and you elected to save both the new and old configuration files, the new version of the configuration file is saved in the same directory as the old one with an added extension suffix for both server and agent. These files are stored in the `/etc/scylla-manager` directory.

On a CentOS configuration, a conflict looks like:

.. code:: sh

    # On the Scylla Manager node
    /etc/scylla-manager/scylla-manager.yaml # old file containing custom values
    /etc/scylla-manager/scylla-manager.yaml.rpmnew # new default file from new version
    # On all Scylla nodes
    /etc/scylla-manager-agent/scylla-manager-agent.yaml # old file containing custom values
    /etc/scylla-manager-agent/scylla-manager-agent.yaml.rpmnew # new default file from new version

On an Ubuntu configuration, a conflict looks like:

.. code:: sh

    # On the Scylla Manager node
    /etc/scylla-manager/scylla-manager.yaml # old file containing custom values
    /etc/scylla-manager/scylla-manager.yaml.dpkg-dist # new default file from new version
    # On all Scylla nodes
    /etc/scylla-manager-agent/scylla-manager-agent.yaml # old file containing custom values
    /etc/scylla-manager-agent/scylla-manager-agent.yaml.dpkg-dist # new default file from new version

It is required to manually inspect both files and reconcile old values with the new configuration. Remember to carry over any custom values like database credentials, backup, repair, and any other configuration. This can be done by manually updating values in the new config file and then renaming files:

For CentOS:

.. code:: sh

    # On the Scylla Manager node
    cd /etc/scylla-manager/
    mv scylla-manager.yaml scylla-manager.yaml.old  #renames the old config file as old
    mv scylla-manager.yaml.rpmnew scylla-manager.yaml
    # On all Scylla nodes
    cd /etc/scylla-manager-agent/
    mv scylla-manager-agent.yaml scylla-manager-agent.yaml.old
    mv scylla-manager-agent.yaml.rpmnew scylla-manager-agent.yaml

For Ubuntu:

.. code:: sh

    # On the Scylla Manager node
    cd /etc/scylla-manager/
    mv scylla-manager.yaml scylla-manager.yaml.old
    mv scylla-manager.yaml.dpkg-dist scylla-manager.yaml
    # On all Scylla nodes
    cd /etc/scylla-manager-agent/
    mv scylla-manager-agent.yaml scylla-manager-agent.yaml.old
    mv scylla-manager-agent.yaml.dpkg-dist scylla-manager-agent.yaml

**Guide to important configuration changes across versions:**

Scylla Manager 2.2.x

- Default ports changed. They are placed into a lower port bracket because higher bracket is reserved by the OS for incoming connections and this is to avoid potential conflicts.
    - ``http`` old port was ``56080`` new port is ``5080``
    - ``https`` old port was ``56443`` new port is ``5443``
    - ``prometheus`` old port was ``56090`` new port is ``5090``
    - ``debug`` old port waas ``56112`` new port is ``5112``
- Repair changes:
    - ``poll_interval`` was changed from ``200ms`` to ``50ms``.
    - ``segments_per_repair``, ``shard_parallel_max``, ``shard_failed_segments_max``, and ``error_backoff`` were removed.
    - ``graceful_stop_timeout`` and ``force_repair_type`` were added.

Scylla Manager Agent 2.2.x

- Default ports changed. They are placed into a lower port bracket because higher bracket is reserved by the OS for incoming connections and this is to avoid potential conflicts.
    - ``prometheus`` old port was ``56090`` new port is ``5090``
    - ``debug`` old port waas ``56112`` new port is ``5112``

Start the Scylla Manager Agent 2.y.b on all nodes
-------------------------------------------------

**On each scylla node** instruct Systemd to start the agent process:

.. code:: sh

    sudo systemctl start scylla-manager-agent

Ensure that it is running with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: active (running)”*.

Start the Scylla Manager Server 2.y.b
-------------------------------------

**On the Manager Server** instruct Systemd to start the server process:

.. code:: sh

    sudo systemctl daemon-reload
    sudo systemctl start scylla-manager

Ensure that it is started with:

.. code:: sh

    sudo systemctl status scylla-manager

It should have a status of *“Active: active (running)”*.

Validate status of the cluster
------------------------------

**On the Manager Server** check the version of the client and the server:

.. code:: sh

    sctool version
    Client version: 2.y.b-0.20200123.7cf18f6b
    Server version: 2.y.b-0.20200123.7cf18f6b

Check that cluster is up:

.. code:: sh

    sctool status -c <cluster>

All running nodes should be up.

.. note:: In **Scylla Manager 2.2** the meaning of repair command's ``--intensity`` flag was changed. After starting the upgraded server, all previously scheduled repairs will remain with the original value of ``--intensity`` but that value will be interpreted differently. This parameter is now broken into two new components:

    - ``--intensity`` which now affects the number of repaired segments in a single repair request to the cluster. By default value (0) scylla manager will try to determine the maximum segments possible based on cluster setup.
    - ``--parallel`` which now affects how many repairs will be requested in parallel (given that only nodes that are not busy with the repair can be repaired in parallel). By default value (0) maximum possible parallelism will be applied.

    Based on this breakdown you can update current repair tasks to achieve previous result. Update can be done with:

        sctool repair update --intensity <value> --parallel <value> <type/task-id>

Rollback Procedure
==================

.. note:: Rolling back to 2.x.a is not recommended because 2.y.b contains bug fixes and performance optimizations so you will be going back to a lesser version. This should be only used as a last resort.

Rollback procedure contains the same steps as upgrade but with downgrading the components to older version:

- Stop all Scylla Manager tasks (or wait for them to finish)
- Stop the Scylla Manager Server 2.y.b
- Stop the Scylla Manager Agent 2.y.b on all nodes
- Downgrade the Scylla Manager Server and Client to 2.x.a
- Downgrade the Scylla Manager Agent to 2.x.a on all nodes
- Bring back old configuration (if there was conflict)
- Start the Scylla Manager Agent 2.x.a on all nodes
- Start the Scylla Manager Server 2.x.a
- Validate status of the cluster

Rollback steps
==============

Stop all Scylla Manager tasks (or wait for them to finish)
----------------------------------------------------------

**On the Manager Server** check current status of the manager tasks:

.. code:: sh

    sctool task list -c <cluster>

None of the listed tasks should have status in RUNNING.

Stop the Scylla Manager Server 2.y.b
------------------------------------

**On the Manager Server** instruct Systemd to stop the server process:

.. code:: sh

    sudo systemctl stop scylla-manager

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager

It should have a status of *“Active: inactive (dead)”*.

Stop the Scylla Manager Agent 2.y.b on all nodes
------------------------------------------------

**On each scylla node** in the cluster run:

.. code:: sh

    sudo systemctl stop scylla-manager-agent

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: inactive (dead)”*.

Downgrade the Scylla Manager Server and Client to 2.x.a
-------------------------------------------------------

**On the Manager Server** instruct package manager to downgrade server and the client:

CentOS, Red Hat:

.. code:: sh

    sudo yum downgrade scylla-manager-server-2.x.a* scylla-manager-client-2.x.a* -y

Debian, Ubuntu:

.. code:: sh

    sudo apt-get install scylla-manager-server=2.x.a scylla-manager-client=2.x.a -y

Downgrade the Scylla Manager Agent to 2.x.a on all nodes
--------------------------------------------------------

**On each scylla node** instruct package manager to downgrade the agent:

CentOS, Red Hat:

.. code:: sh

    sudo yum downgrade scylla-manager-agent-2.x.a* -y

Debian, Ubuntu:

.. code:: sh

    sudo apt-get install scylla-manager-agent=2.x.a -y

Revert to the old configuration
----------------------------------------------------

If you followed instructions from the Upgrade Steps section and you had configuration conflict when upgrading, then listing the configuration directory should give you both new and old configuration:

.. code:: sh

    /etc/scylla-manager/scylla-manager.yaml # New version that you want to disable
    /etc/scylla-manager/scylla-manager.yaml.old # Previous version that you want to rollback

To restore the old configuration:

.. code:: sh

    cd /etc/scylla-manager/
    mv scylla-manager.yaml scylla-manager.yaml.new
    mv scylla-manager.yaml.old scylla-manager.yaml

The procedure is the same for the Scylla Manager Agent (on all nodes):

.. code:: sh

    cd /etc/scylla-manager-agent/
    mv scylla-manager-agent.yaml scylla-manager-agent.yaml.new
    mv scylla-manager-agent.yaml.old scylla-manager-agent.yaml

Start the Scylla Manager Agent 2.x.a on all nodes
-------------------------------------------------

On all nodes instruct Systemd to start the agent process:

.. code:: sh

    sudo systemctl start scylla-manager-agent

Ensure that it is running with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: active (running)”*.

Start the Scylla Manager Server 2.x.a
-------------------------------------

**On the Manager Server** instruct Systemd to start the server process:

.. code:: sh

    sudo systemctl stop scylla-manager

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager

It should have a status of *“Active: active (running)”*.

.. note:: In **Scylla Manager 2.2** the meaning of repair command's ``--intensity`` flag was changed. If you want to rollback changes that were broken down to ``--intensity`` and ``--parallel`` you would need to remove or disable task in question and create new with correct values.

    For example:

        sctool task update --enable false <type/task_id>
        sctool repair [repair parameters]

Validate status of the cluster
------------------------------

**On the Manager Server** check the version of the client and the server:

.. code:: sh

    sctool version
    Client version: 2.x.a
    Server version: 2.x.a

Check that cluster is up:

.. code:: sh

    sctool status -c <cluster>

All running nodes should be up.

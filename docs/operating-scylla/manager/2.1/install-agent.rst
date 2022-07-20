=================================
Scylla Manager Agent Installation
=================================

.. include:: /operating-scylla/manager/_common/note-versions.rst

Scylla Manager Agent is an executable, installed on each Scylla node. 
The Server communicates with the Agent over REST/HTTPS. 
The Agent communicates with the local Scylla node over the REST/HTTP.



Install Scylla Manager Agent
----------------------------

Prerequisites
=============

* Scylla cluster running on any :doc:`OS supported by Scylla Manager 2.0 </getting-started/os-support>`
* Traffic on port 10001 unblocked to Scylla nodes from the dedicated host

.. note:: Scylla Manager only works with Scylla clusters that are using the Murmur3 partitioner (Scylla default partitioner). To check your cluster's partitioner, run the cqlsh command ``DESCRIBE CLUSTER``.

Download packages
=================

**Procedure**

Download and install Scylla Manager Agent (from the Scylla Manager Download Page) according to the desired version:

* `Scylla Manager for Open Source <https://www.scylladb.com/download/open-source/scylla-manager/>`_ - Registration Required
* Scylla Enterprise - Login to the `Customer Portal <https://www.scylladb.com/customer-portal/>`_

Configure Scylla Manager Agent
------------------------------
There are three steps you need to complete:

#. `Generate an authentication token`_
#. Place the token parameters from `Configure authentication token parameters`_ in the Agent configuration file
#. `Start Scylla Manager Agent service`_ or restart if already running. Confirm the service starts / restarts and runs without errors


.. _manager-2.1-generate-auth-token:

Generate an authentication token
================================

**Procedure**

#. Generate an authentication token to be used to authenticate Scylla Manager with Scylla nodes. 
   This procedure is done **once** for each cluster. It is recommended to use a different token for each cluster. 

   .. note:: Use the same token on all nodes in the same cluster. 

   From **one node only** Run the token generator script. 

   For example:

   .. code-block:: none

      $ scyllamgr_auth_token_gen
      6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM

   If you want to change the token, you will need to repeat this procedure and place the new token on all nodes. 
   This procedure sets up the Scylla agent on each node. 
   Repeat the procedure for **every** Scylla node in the cluster that you want to be managed under Scylla Manager.


Run `scyllamgr_agent_setup` script
==================================

**Procedure**

#. Run the setup script to setup environment for the agent:

   .. note:: Script requires sudo rights

   .. code-block:: none

      $ sudo scyllamgr_agent_setup
      Do you want to create scylla-helper.slice if it does not exist?
      Yes - limit Scylla Manager Agent and other helper programs memory. No - skip this step.
      [YES/no] YES
      Do you want the Scylla Manager Agent service to automatically start when the node boots?
      Yes - automatically start Scylla Manager Agent when the node boots. No - skip this step.
      [YES/no] YES

   The first step relates to limited resources that are available to the agent, and second instructs systemd to run agent on node restart.

.. _manager-2.1-configure-auth-token:

Configure authentication token parameters
=========================================

**Procedure**

#. Take the authentication token you generated from `Generate an authentication token`_, and place it into ``/etc/scylla-manager-agent/scylla-manager-agent.yaml`` as part of the ``auth_token`` :ref:`section <manger-2.1-agent-configuration-file-auth-token>`.  

   For Example:

   .. code-block:: none

      $ cat /etc/scylla-manager-agent/scylla-manager-agent.yaml
      # Scylla Manager Agent config YAML

      # Specify authentication token, the auth_token needs to be the same for all the
      # nodes in a cluster. Use scyllamgr_auth_token_gen to generate the auth_token
      # value.
      auth_token: 6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM


Start Scylla Manager Agent service
==================================

**Procedure**

#. Start Scylla Manager Agent service

   .. code-block:: none

      $ sudo systemctl start scylla-manager-agent

#. Validate Scylla Manager Agent is running

   .. code-block:: none

      $ sudo systemctl status scylla-manager-agent
      ● scylla-manager-agent.service - Scylla Manager Agent
        Loaded: loaded (/usr/lib/systemd/system/scylla-manager-agent.service; disabled; vendor preset: disabled)
        Active: active (running) since Wed 2019-10-30 10:46:51 UTC; 7s ago
          Main PID: 14670 (scylla-manager-)
          CGroup: /system.slice/scylla-manager-agent.service
                 └─14670 /usr/bin/scylla-manager-agent

#. Enable the Scylla Manager Agent to run when the node starts.

   .. code-block:: none

      $ sudo systemctl enable scylla-manager-agent


#.    Repeat the procedure for **every** Scylla node in the cluster that you want to be managed under Scylla Manager, starting with `Configure authentication token parameters`_. 

.. _manager-2.1-prepare-nodes-for-backup:

Prepare nodes for backup
------------------------

Adding the cluster to Scylla Manager automatically creates a backup task. Validate the connection to your backup location is accessible from Scylla Manager before adding the cluster to avoid errors.  

**Procedure**

#. Create a storage location for the backup. 
   Currently, Scylla Manager 2.1 supports `S3 buckets <https://aws.amazon.com/s3/>`_ created on AWS.
   You can use an S3 bucket that you already created. 
#. Choose how you want to configure access to the S3 Bucket. 
   You can use an IAM role (recommended), or you can add your AWS credentials to the agent configuration file. 
   This method is less secure as you will be propagating each node with this security information, and in cases where you need to change the key, you will have to replace it on each node.  

   * To use an IAM Role:

     #. Create an `IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide//iam-roles-for-amazon-ec2.html>`_ for the S3 bucket which adheres to your company security policy. You can use the role you already created. 
     #. `Attach the IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide//iam-roles-for-amazon-ec2.html#attach-iam-role>`_ to **each EC2 instance (node)** in the cluster.

   * To add your AWS credentials the Scylla Manager Agent configuration file:

     #. Edit the ``/etc/scylla-manager-agent/scylla-manager-agent.yaml`` in the ``S3`` section your authentication information about the S3 bucket. 
        Refer to :ref:`AWS Credentials Configuration <manager-2.1-aws-credentials>` for details. 

#. Validate that the manager has access to the backup location. 
   If there is no response, the S3 bucket is accessible. If not, you will see an error. 

   .. code-block:: none
      
      $ scylla-manager-agent check-location --location s3:<your S3 bucket name>


Register a cluster
------------------

Continue with :doc:`Add a Cluster <add-a-cluster>`.

=============================================================================
Launch ScyllaDB |CURRENT_VERSION| on Oracle Cloud Infrastructure (OCI)
=============================================================================

This article will guide you through self-managed ScyllaDB deployment on Oracle Cloud Infrastructure (OCI)
from the ScyllaDB image published in `Oracle Cloud Marketplace <https://docs.oracle.com/en-us/iaas/Content/Marketplace/Concepts/marketoverview.htm>`_.
For a fully-managed deployment of ScyllaDB as-a-service, see
`ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/>`_.

The ScyllaDB Marketplace listing is a *Bring Your Own License* (BYOL) image listing: OCI charges you for
the compute, storage, and network resources you use, and the ScyllaDB software is covered by your ScyllaDB
license or subscription.

Prerequisites
----------------

* Active OCI account (tenancy) with permissions to launch compute instances, and to subscribe to
  Marketplace images in the target compartment. See
  `Managing Compartments <https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/managingcompartments.htm>`_.
* A `VCN <https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/managingVCNs.htm>`_ with a subnet to
  launch the instance into, and the :ref:`ScyllaDB ports <networking-ports>` allowed by the
  `security rules <https://docs.oracle.com/en-us/iaas/Content/Network/Concepts/securityrules.htm>`_ that
  apply to the instance - either in a security list attached to the subnet, or in a
  `network security group <https://docs.oracle.com/en-us/iaas/Content/Network/Concepts/networksecuritygroups.htm>`_
  (NSG) attached to the instance's VNIC.

  Open each port only to the peers that need it: the inter-node ports to the other nodes of the cluster,
  the CQL ports to your clients, and keep the REST API port closed to anything outside the cluster. You
  also need TCP port 22 open, from your administrators' addresses only, for the SSH step below. See
  :doc:`ScyllaDB Security Checklist </operating-scylla/security/security-checklist/>` for the full set of
  recommendations.
* An SSH key pair. See
  `Managing Key Pairs on Linux Instances <https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/managingkeypairs.htm>`_.
* To use the CLI: the `OCI CLI <https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm>`_,
  installed and configured with ``oci setup config`` (see
  `Configuring the CLI <https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliconfigure.htm>`_).
* ScyllaDB Image requires at least 2 vCPU servers.
* The image is an x86_64 image and supports the Dense I/O shapes, which provide the local NVMe storage that
  ScyllaDB uses for its data directory. On the first boot, the image formats the local NVMe disks with XFS,
  creating a RAID0 array over them if there is more than one.

.. warning::
   The ScyllaDB data directory lives on the instance's local NVMe storage. OCI does not back up local NVMe
   storage, and its contents are lost when the instance is
   `terminated <https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/terminatinginstance.htm>`_ or
   migrated to another host. Keep data that must survive on a multi-node cluster with a replication factor
   greater than one, and back it up - for example with
   `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_.

.. _launch-on-oci-console:

Launching ScyllaDB from the OCI Console
------------------------------------------

#. Choose a shape. See :ref:`Cloud Instance Recommendations for OCI <system-requirements-oci>` for the list
   of recommended shapes.

   Other shapes will work, but with lesser performance. If you choose a shape other than the recommended
   ones, make sure to run the :ref:`scylla_setup <system-configuration-scripts>` script.

#. Sign in to the `OCI Console <https://cloud.oracle.com/>`_ and select the region you want to deploy in.

#. Open the navigation menu, select **Marketplace**, and then select **All Applications**.

#. Search for *ScyllaDB* and select the ScyllaDB listing for the version you want to deploy. Listings are
   named after the ScyllaDB release, for example *ScyllaDB 2026.2*.

   Alternatively, open
   `the ScyllaDB listings in Marketplace <https://cloud.oracle.com/marketplace/apps?searchQuery=ScyllaDB>`_
   directly.

#. On the listing page:

   * **Version** – Select the package version to deploy. The latest version is selected by default.
   * **Compartment** – Select the compartment to launch the instance in.
   * Select the checkbox to accept the Oracle Terms of Use and the ScyllaDB (partner) terms and conditions.

#. Select **Launch Instance**.

   The **Create compute instance** page opens with the ScyllaDB Marketplace image already selected.

#. Configure the instance:

   * **Name** – A name for the instance.
   * **Placement** – Select the availability domain.
   * **Image and shape** – Keep the pre-selected ScyllaDB image, and select **Change shape** to pick
     the shape you chose in the first step. For a flexible shape, set the number of OCPUs and the amount of memory.
   * **Networking** – Select the VCN and subnet. If you keep your security rules in network security
     groups, select **Use network security groups to control traffic** and choose the NSG that allows the
     ScyllaDB ports.

     Assign a public IPv4 address if you need to reach the node from outside the VCN. A public address can
     only be assigned in a public subnet - in a private subnet, reach the node over its private address
     from within the VCN, through a
     `bastion <https://docs.oracle.com/en-us/iaas/Content/Bastion/Concepts/bastionoverview.htm>`_,
     or over a VPN.
   * **Add SSH keys** – Select **Generate a key pair for me** and save the private key, or select
     **Paste public keys** / **Upload public key files (.pub)** to provide your own public key. You need
     this key to connect to the instance.
   * **Advanced options > Management > Initialization script** – Optional. Provide the ScyllaDB
     configuration as user data. See :ref:`Configuring ScyllaDB with user data <launch-on-oci-user-data>`
     below.

     The Console base64-encodes the script for you, so paste or upload it as plain text.

#. Select **Create**.

   The instance is listed as *Provisioning*, and switches to *Running* when it is up. Continue with
   :ref:`Connecting to the instance <launch-on-oci-connect>`.

.. _launch-on-oci-cli:

Launching ScyllaDB with the OCI CLI
--------------------------------------

Launching a Marketplace image with the CLI requires that you first accept the image terms of use, which
creates an image subscription in your compartment. This is the CLI equivalent of the terms-of-use checkbox
in the Console.

The following steps use the ``us-ashburn-1`` region as an example. Replace it with your region, or omit
``--region`` to use the region from your CLI configuration.

#. Choose a shape. See :ref:`Cloud Instance Recommendations for OCI <system-requirements-oci>` for the list
   of recommended shapes.

   Other shapes will work, but with lesser performance. If you choose a shape other than the recommended
   ones, make sure to run the :ref:`scylla_setup <system-configuration-scripts>` script.

#. Find the OCID of the ScyllaDB Marketplace listing:

   .. code-block:: console

        oci marketplace listing list --region us-ashburn-1 --all \
          --query 'data[?contains(name, `Scylla`)].{name:name, id:id}'

   Example output:

   .. code-block:: json

        [
          {
            "id": "ocid1.mktpublisting.oc1.iad.amaaaaaa433yp7qakrqk5u3zu6ekylon4xrrflma3xcjug52xts4svplmjna",
            "name": "ScyllaDB 2026.2"
          }
        ]

   You can also read the OCID from the ``/marketplace/application/<listing OCID>`` part of the listing URL
   in the OCI Console.

#. List the available package versions of that listing:

   .. code-block:: console

        oci marketplace package list --region us-ashburn-1 \
          --listing-id <listing OCID> \
          --query 'data[].{version:"package-version", created:"time-created"}'

   Note the version you want to deploy - usually the most recently created one.

#. Get the package details of that version. The output holds the image OCID and the App Catalog
   identifiers you need in the next steps:

   .. code-block:: console

        oci marketplace package get --region us-ashburn-1 \
          --listing-id <listing OCID> \
          --package-version <package version> \
          --query 'data.{image:"image-id", catalogListing:"app-catalog-listing-id", catalogVersion:"app-catalog-listing-resource-version"}'

   Example output:

   .. code-block:: json

        {
          "catalogListing": "ocid1.appcataloglisting.oc1..aaaaaaaarw5vsfysh36clj4xasz5mhsemzs5wt4eea5tdywzbujopvjzsoxq",
          "catalogVersion": "2026.2.5",
          "image": "ocid1.image.oc1..aaaaaaaashyu5zg4s72zwanu73enkab4mitjkmifebglfvp4qwqdbalxmtgq"
        }

#. Retrieve the terms of use for that version. The response contains the EULA link, the Oracle Terms of Use
   link, a signature, and the time the terms were retrieved - all of which are required to subscribe:

   .. code-block:: console

        oci compute pic agreements get --region us-ashburn-1 \
          --listing-id <app catalog listing OCID> \
          --resource-version <app catalog resource version>

   Review the documents at ``eula-link`` and ``oracle-terms-of-use-link`` before continuing.

#. Subscribe to the image in your compartment, which accepts the terms retrieved in the previous step:

   .. code-block:: console

        oci compute pic subscription create --region us-ashburn-1 \
          --compartment-id <compartment OCID> \
          --listing-id <app catalog listing OCID> \
          --resource-version <app catalog resource version> \
          --signature <signature> \
          --time-retrieved <time-retrieved> \
          --eula-link <eula-link> \
          --oracle-tou-link <oracle-terms-of-use-link>

   The subscription takes some time to propagate. Verify that it is in place before launching:

   .. code-block:: console

        oci compute pic subscription list --region us-ashburn-1 \
          --compartment-id <compartment OCID>

   .. note::
      Launching immediately after creating the subscription can fail, because the new subscription is not
      visible to the Compute service yet. If the next step fails with a not-authorized or not-found error
      on the image, wait a few minutes and retry it - do not re-create the subscription.

#. Launch the instance, using the image OCID from the package details:

   .. code-block:: console

        oci compute instance launch --region us-ashburn-1 \
          --compartment-id <compartment OCID> \
          --availability-domain <availability domain> \
          --shape <shape> \
          --shape-config '{"ocpus": <number of OCPUs>, "memoryInGBs": <memory in GB>}' \
          --subnet-id <subnet OCID> \
          --nsg-ids '["<NSG OCID>"]' \
          --assign-public-ip true \
          --image-id <image OCID> \
          --display-name scylla-node1 \
          --ssh-authorized-keys-file <path to your public SSH key> \
          --user-data-file <path to your user data file> \
          --wait-for-state RUNNING

   For example:

   .. code-block:: console

        oci compute instance launch --region us-ashburn-1 \
          --compartment-id ocid1.compartment.oc1..example \
          --availability-domain ewbj:US-ASHBURN-AD-1 \
          --shape VM.DenseIO.E5.Flex \
          --shape-config '{"ocpus": 8, "memoryInGBs": 96}' \
          --subnet-id ocid1.subnet.oc1.iad.example \
          --nsg-ids '["ocid1.networksecuritygroup.oc1.iad.example"]' \
          --assign-public-ip true \
          --image-id ocid1.image.oc1..aaaaaaaashyu5zg4s72zwanu73enkab4mitjkmifebglfvp4qwqdbalxmtgq \
          --display-name scylla-node1 \
          --ssh-authorized-keys-file ~/.ssh/id_rsa.pub \
          --user-data-file ./scylla-user-data.json \
          --wait-for-state RUNNING

   ``--shape-config`` is required for flexible shapes only. ``--nsg-ids`` is needed only if your security
   rules live in a network security group rather than in the subnet's security list. ``--user-data-file``
   is optional; see :ref:`Configuring ScyllaDB with user data <launch-on-oci-user-data>`. To list the
   availability domains available to you, run ``oci iam availability-domain list``.

   ``--assign-public-ip true`` requires a public subnet, and fails in a subnet that prohibits public IP
   addresses. Omit it to launch into a private subnet, and reach the node over its private address from
   within the VCN, through a
   `bastion <https://docs.oracle.com/en-us/iaas/Content/Bastion/Concepts/bastionoverview.htm>`_,
   or over a VPN.

   .. note::
      ``--user-data-file`` and ``--ssh-authorized-keys-file`` cannot be combined with ``--metadata``
      in the same command - they are convenience wrappers around the ``user_data`` and
      ``ssh_authorized_keys`` fields of the instance metadata.

#. Get the IP addresses of the instance:

   .. code-block:: console

        oci compute instance list-vnics --region us-ashburn-1 \
          --instance-id <instance OCID> \
          --query 'data[].{public:"public-ip", private:"private-ip"}'

.. _launch-on-oci-user-data:

Configuring ScyllaDB with user data
--------------------------------------

The ScyllaDB image reads its configuration from the instance user data (the initialization script in
the Console, or ``--user-data-file`` in the CLI). The most popular options are:

* ``cluster_name`` - The name of the cluster.
* ``seed_provider`` - The IP of the first node. New nodes will use the IP of this seed node to connect to
  the cluster and learn the cluster topology and state. See :doc:`ScyllaDB Seed Nodes </kb/seed-nodes>`.
* ``post_configuration_script`` - A base64 encoded bash script that will be executed after the
  configuration is completed.
* ``start_scylla_on_first_boot`` - Starts ScyllaDB once the configuration is completed.
* ``device_wait_seconds`` - How long to wait for the local NVMe devices to appear before setting up the
  data directory. The default is ``0``, which does not wait at all. Set it to ``300`` so that setup does
  not fail with ``No data disk found, abort setup`` when the devices are not ready yet at first boot.

Example:

.. code-block:: json

    {
        "scylla_yaml": {
            "cluster_name": "test-cluster",
            "seed_provider": [{"class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                               "parameters": [{"seeds": "10.0.219.209"}]}]
        },
        "start_scylla_on_first_boot": true,
        "device_wait_seconds": 300
    }

See :ref:`scylla.yaml <admin-scylla.yaml>` for information about the supported ``scylla_yaml`` options, and
the `ScyllaDB Image documentation <https://github.com/scylladb/scylla-machine-image>`_ for the full
documentation of ScyllaDB image user data.

.. note::
   When launching more than one instance, make sure to correctly set the IP of the first instance with the
   ``seeds`` parameter - either in the user data or after launch.

.. _launch-on-oci-connect:

Connecting to the instance
-----------------------------

Connect to the instance with the private key matching the public key you provided at launch, using the
``scyllaadm`` user:

.. code-block:: console

    ssh -i ~/.ssh/id_rsa scyllaadm@<instance IP>

Use the instance's public IP address if you assigned one. Otherwise, connect to its private IP address
from within the VCN, through a
`bastion <https://docs.oracle.com/en-us/iaas/Content/Bastion/Concepts/bastionoverview.htm>`_, or over
a VPN.

The default file paths:

* The ``scylla.yaml`` file: ``/etc/scylla/scylla.yaml``
* Data: ``/var/lib/scylla/``

To check that the ScyllaDB server is running, run:

.. code-block:: console

    nodetool status

Next Steps
------------------

* :doc:`Configure ScyllaDB </getting-started/system-configuration>`
* Manage your clusters with `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_
* Monitor your cluster and data with `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_
* Get familiar with ScyllaDB's :doc:`command line reference guide </operating-scylla/nodetool>`.
* Learn about ScyllaDB at `ScyllaDB University <https://university.scylladb.com/>`_

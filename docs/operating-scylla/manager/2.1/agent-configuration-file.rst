========================
Agent Configuration File
========================

.. include:: /operating-scylla/manager/_common/note-versions.rst



This document covers the configurations settings you need to consider for the Sylla Manager Agent. 

The Scylla Manager Agent has a single configuration file /etc/scylla-manager-agent/scylla-manager-agent.yaml. 

.. _manger-2.1-agent-configuration-file-auth-token:

Authentication token
====================

.. note:: Completing this section in the scylla-manager-agent.yaml file is mandatory

Scylla Agent uses token authentication in API calls so that the Scylla Manager Server can authenticate itself with the Scylla Manager Agent.
Once you have :ref:`created a token <manager-2.1-generate-auth-token>`, and have configured the :ref:`agent configuration file <manager-2.1-configure-auth-token>` as described.

.. code-block:: none

   # Specify authentication token
   auth_token: 6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM

HTTPS server settings
=====================

In this section, you can specify which address Scylla Agent should listen to.
By default, Scylla Manager Agent pulls these values from the Scylla itself.
Port 10001 is the default port, and traffic to this port should be allowed on the firewall.
You can change the port to a different port by clearing the ``https`` label and adding an IP address and port.
In order to obtain TLS cert and key file, use the ``scyllamgr_ssl_cert_gen`` script.  

.. code-block:: none

   # Bind REST API to the specified TCP address using HTTPS protocol. By default
   # Scylla Manager Agent uses Scylla listen/broadcast address that is read from
   # the Scylla API (see the scylla section).
   #https: 0.0.0.0:10001

   # TLS certificate and key files to use with HTTPS. To regenerate the files use
   # scyllamgr_ssl_cert_gen script shipped with the Scylla Manager Agent.
   #tls_cert_file: /var/lib/scylla-manager/scylla_manager.crt
   #tls_key_file: /var/lib/scylla-manager/scylla_manager.key

Prometheus settings
===================

In this section, you can set the Prometheus settings for the Scylla Manager Agent so that the Agent Manager metrics (from each Sylla node) can be viewed and monitored with Scylla Monitoring. 

.. code-block:: none

   # Bind Prometheus API to the specified TCP address using HTTP protocol.
   # By default it binds to all network interfaces, but you can restrict it
   # by specifying it like this 127:0.0.1:56090 or any other combination
   # of ip and port.
   #prometheus: ':56090'

If you change the Prometheus IP or port, you must adjust the rules in the Prometheus server.

.. code-block:: none

   - targets:
     - IP:56090

Debug endpoint settings
=======================

In this section, you can specify the pprof debug server address.
It allows you to run profiling on demand on a live application.
By default, the server is running on port ``56112``.

.. code-block:: none

   debug: 127.0.0.1:56112

CPU pinning settings
====================

In this section, you can set the ``cpu`` setting, which dictates the CPU to run Scylla Manager Agent on. 
By default, the agent reads the Scylla configuration from ``/etc/scylla.d/cpuset.conf`` and tries to find a core that is not used by Scylla. 
If that's not possible, you can specify the core on which to run the Scylla Manager Agent.

.. code-block:: none

   cpu: 0

Log level settings
==================

In this section, you can set the Log level settings which specify log output and level. Available log levels are ``error``, ``info`` and ``debug``.

.. code-block:: none

   logger:
     level: info

Scylla API settings
===================

In this section, you can set the Scylla API settings. Scylla Manager Agent pulls all needed configuration options from the ``scylla.yaml`` file. In order to do this, Scylla Manager Agent needs to know where the Scylla API is exposed. You should copy the ``api_address`` and ``api_port`` values from ``/etc/scylla/scylla.yaml`` and add them here:

.. code-block:: none

   #scylla:
   #  api_address: 0.0.0.0
   #  api_port: 10000

Backup S3 settings
==================

In this section, you configure the AWS credentials (if required) for the backup location.

IAM Role
--------

.. note:: If you are setting an IAM role in AWS, you do not need to change this section.  

.. _manager-2.1-aws-credentials:

AWS credentials
---------------

.. note:: Completing this section in the scylla-manager-agent.yaml file is mandatory if you are not using an IAM role. Make sure you understand the security ramifications of placing AWS credentials into the yaml file. 

Fill in the information below with your AWS Credentials information.
If you do not know where your keys are located, read the `AWS Security Blogs <https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/>`_ or `documentation <https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys>`_ for information.

.. code-block:: none

   s3:
   # S3 credentials, it's recommended to use IAM roles if possible, otherwise set
   # your AWS Access Key ID and AWS Secret Access Key (password) here.
     access_key_id: <your access key id>
     secret_access_key: <your secret access key>

MinIO and other AWS S3 alternatives
-----------------------------------

Backup can work with MinIO and other AWS S3 compatible providers.
The available options are:

* Alibaba,
* Ceph,
* DigitalOcean,
* IBMCOS,
* Minio,
* Wasabi,
* Dreamhost,
* Netease.

To configure S3 with a 3rd-party provider, in addition to credentials, one needs to specify ``provider`` parameter with one of the above options.
If the service is self-hosted, it's also needed to specify ``endpoint`` with its base URL address.

.. code-block:: none

   s3:
   # S3 credentials, it's recommended to use IAM roles if possible, otherwise set
   # your AWS Access Key ID and AWS Secret Access Key (password) here.
     access_key_id: <your access key id>
     secret_access_key: <your secret access key>
   # Provider of the S3 service. By default, this is AWS. There are multiple S3
   # API compatible providers that can be used instead. Due to minor differences
   # between them we require that exact provider is specified here for full
   # compatibility. Supported and tested options are: AWS and Minio.
   # The available providers are: Alibaba, AWS, Ceph, DigitalOcean, IBMCOS, Minio,
   # Wasabi, Dreamhost, Netease.
     provider: Minio
   #
   # Endpoint for S3 API, only relevant when using S3 compatible API.
     endpoint: <your MinIO instance URL>

Advanced settings
-----------------

.. code-block:: none

   #s3:
   # The server-side encryption algorithm used when storing this object in S3.
   # If using KMS ID you must provide the ARN of Key.
   #  server_side_encryption:
   #  sse_kms_key_id:
   #
   # Number of files uploaded concurrently, by default it's 2.
   #  upload_concurrency: 2
   #
   # Maximum size (in bytes) of the body of a single request to S3 when uploading big files.
   # Big files are cut into chunks. This value allows specifying how much data
   # single request to S3 can carry. Bigger value allows reducing the number of requests
   # needed to upload files, increasing it may help with 5xx responses returned by S3.
   # Default value is 50M, and the string representation of the value can be provided, for
   # e.g. 1M, 1G, off.
   #  chunk_size: 50M
   #
   # AWS S3 Transfer acceleration
   # https://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration-examples.html
   #  use_accelerate_endpoint: false

Additional resources
====================

Scylla Manager :doc:`Configuration file <configuration-file>`

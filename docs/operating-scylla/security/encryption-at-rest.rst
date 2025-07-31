==================
Encryption at Rest
==================

Introduction
----------------------

ScyllaDB protects your sensitive data with data-at-rest encryption. It protects
the privacy of your users' data, reduces the risk of data breaches, and helps
meet regulatory requirements. In particular, it provides an additional level of
protection for your data persisted in storage or its backups.

When ScyllaDB's Encryption at Rest is used together with Encryption in Transit
(:doc:`Node to Node </operating-scylla/security/node-node-encryption>` and
:doc:`Client to Node </operating-scylla/security/client-node-encryption>`), you
benefit from end to end data encryption.

About Encryption at Rest
-----------------------------

The following can be encrypted:

* ScyllaDB persistent tables (SSTables):

  - User tables
  - System tables: ``system.batchlog``, ``system.paxos``, ``system.dicts``

* System level data:

  - Commit logs
  - Hints

Encryption at Rest can be configured at table granularity, so you can choose to
encrypt only sensitive tables. For both system and table data, you can use
different block cipher algorithms that are supported by `OpenSSL
<https://www.openssl.org/>`_, in different modes of operation.

.. note:: SSTables of a particular table may be encrypted with different block
   ciphers, modes of operation, and keys; or not be encrypted at all - at the
   same time.

When is Data Encrypted?
========================

As SSTables are immutable, they are encrypted only once, as a result of memtable
flush, compaction, or upgrade (with :doc:`Nodetool upgradesstables
</operating-scylla/nodetool-commands/upgradesstables>`). Similarly, system
artifacts are encrypted when they are written. Encryption configuration takes
effect at the time of write, and is persisted in the given SSTable's metadata
file (``Scylla.db``), or the given system artifact's (e.g. commit log's)
metadata file, respectively.

SSTables of a table, on a particular node, are encrypted using the most recently
configured algorithm and key that apply to that table and node. When you enable
encryption for an existing table, only subsequently written SSTables are
encrypted accordingly. Old SSTables that existed before (re)configuring
encryption are not updated. Those SSTables remain encrypted (or not encrypted at
all) according to the configuration that was in effect when they were written.

When is Data Decrypted?
========================

When ScyllaDB reads an encrypted SSTable from disk, it fetches the
:ref:`algorithm descriptor <ear-cipher-algorithms>` (identifying block cipher,
key size, and mode of operation) from the SSTable's metadata file
(``Scylla.db``). ScyllaDB extracts the key from the key provider (which is
recorded in the same SSTable metadata file), and then decrypts the other SSTable
components with the key.

When ScyllaDB reads an encrypted system artifact, such as a commit log, it
fetches the algorithm descriptor and the key provider from the commit log
segment's metadata file. ScyllaDB extracts the key from the key provider, and
then decrypts the system artifact with the key.


Data Classes for Encryption
---------------------------

ScyllaDB supports encryption for both system and user data. Encryption behaves
similarly for both data classes, but it is configured differently for each
class:

* Encryption for system and user data must be configured separately in
  ``scylla.yaml``, through the ``system_info_encryption`` and
  ``user_info_encryption`` sections, respectively. This separation allows to
  select different configuration properties for each data class.

* Encryption for user data can be configured at two levels:

  - Node-level configuration: Using the ``user_info_encryption`` section in
    ``scylla.yaml``.

  - Table-level configuration: Using the ``scylla_encryption_options`` in the
    table schema.

  The table-level options are both finer-grained and coarser-grained than the
  SSTable encryption defaults in ``scylla.yaml``:

  - They are finer-grained because they override any possible defaults in
    ``scylla.yaml`` for a particular table.

  - They are coarser-grained because they apply to all nodes in the cluster that
    write out new SSTables for the table, while the options configured in
    ``scylla.yaml`` apply only to a particular node.

.. _ear-cipher-algorithms:

Cipher Algorithm Descriptors
----------------------------

The following block cipher, mode of operation, and key size combinations are
available for use with ScyllaDB using `OpenSSL <https://www.openssl.org/>`_.

.. list-table::
   :widths: 70 30
   :header-rows: 1

   * - ``cipher_algorithm``
     - ``secret_key_strength``
   * - ``AES/CBC/PKCS5Padding`` (**default**)
     - ``128`` (**default**), ``192``, or ``256``
   * - ``AES/ECB/PKCS5Padding``
     - ``128``, ``192``, or ``256``
   * - ``Blowfish/CBC/PKCS5Padding``
     - .. https://github.com/scylladb/scylla-enterprise/issues/4848

       ``128``

The ECB (Electronic Code Book) mode of operation is less secure than CBC (Cipher
Block Chaining).

The Blowfish block cipher's block size is 64 bits, while AES has a 128-bit block
size. Therefore, Blowfish may leak information when encrypting more than a few
GB of data with the same key.

The default algorithm (``AES/CBC/PKCS5Padding`` with key strength ``128``) is
strongly recommended.

.. _ear-key-providers:

Key Providers
----------------------

When encrypting the system tables or SSTables, you need to state which provider
is holding your keys. You can use the following options:

.. list-table::
   :widths: 33 33 33
   :header-rows: 1

   * - Key Provider Name
     - ``key_provider`` value
     - Description
   * - Local Key Provider
     - ``LocalFileSystemKeyProviderFactory`` (**default**)
     - Stores the key on the same machine as the data.
   * - Replicated Key Provider
     - ``ReplicatedKeyProviderFactory``
     - Stores table keys in a ScyllaDB table where the table itself is encrypted
       using the system key.
   * - KMIP Key Provider
     - ``KmipKeyProviderFactory``
     - Stores key(s) in an external Key Management Server using the Key
       Management Interoperability Protocol (KMIP).
   * - KMS Key Provider
     - ``KmsKeyProviderFactory``
     - Uses key(s) provided by the AWS KMS service.
   * - GCP Key Provider
     - ``GcpKeyProviderFactory``
     - Uses key(s) provided by the GCP KMS service.
   * - Azure Key Provider
     - ``AzureKeyProviderFactory``
     - Uses key(s) provided by the Azure Key Vault service.

Each available key provider is further described at a high level in the
following sections. Instructions on how to configure them are provided in later
sections.

Local Key Provider
====================

.. note::

   The Local Key Provider is less safe than other options and as such it is not
   recommended for production use. It is the default key provider for the
   node-local encryption configuration in ``scylla.yaml`` because it does not
   require any external resources. In production environments, it is recommended
   to use an external KMS instead.

The Local Key Provider is the default key provider for the node-local encryption
configuration in ScyllaDB (``user_info_encryption`` and ``system_info_encryption``
in ``scylla.yaml``). It stores the encryption keys locally on disk in a text file.
The location of this file is specified in ``scylla.yaml``, or in the table schema.
The user has the option to generate the key(s) themselves, or let ScyllaDB
generate the key(s) for them.

.. caution::

   Care should be taken so that no unauthorized person can access the key data
   from the file system. Make sure that the owner of this file is the ``scylla``
   user and that the file is **not** readable by **other users**, or accessible
   by **other roles**.

You should also consider keeping the key directory on a network drive (using TLS
for the file sharing) to avoid having keys and data on the same storage media,
in case your storage is stolen or discarded.

Replicated Key Provider
=========================

.. note::

   The Replicated Key Provider is not recommended for production use because it
   does not support key rotation. For compatibility with DataStax Cassandra, it
   is the default key provider for per-table encryption setup. In production
   environments, an external KMS should be used instead.

The Replicated Key Provider is the default key provider for per-table encryption
setup in ScyllaDB (``scylla_encryption_options`` in table schema). It stores and
distributes the encryption keys across every node in the cluster through a
special ScyllaDB system table (``system_replicated_keys.encrypted_keys``). The
Replicated Key Provider requires two additional keys to operate:

* A system key - used to encrypt the data in the system table. The system key
  can be either a local key, or a KMIP key.

* A local secret key - used as a fallback key, in case the Replicated Key
  Provider fails to retrieve a key from the system table.

The Replicated Key Provider has some limitations:

* It cannot be selected as a node's default encryption option for system or user
  data in ``scylla.yaml``. It can only be configured per-table, through the
  ``scylla_encryption_options`` in the table schema.

* It does not support key rotation.

KMIP Key Provider
===================

The KMIP Key Provider stores the encryption keys in an external Key Management
Service (KMS) that supports the Key Management Interoperability Protocol (KMIP).
The user has the option to generate the key(s) themselves, or let ScyllaDB
generate the key(s) for them. When creating a key or locating an existing key,
the key provider extracts the raw key material from the server and stores it in
a loading cache in memory.

Key rotation is supported, but the rotation must be performed by the user on the
KMIP server level.

.. _ear-key-providers-kms:

KMS Key Provider
==================

The KMS Key Provider uses keys stored in the AWS Key Management Service (KMS).
These keys are expected to be pre-created and managed entirely by the user - the
KMS Key Provider does not create any KMS keys by itself. The user needs to
specify a KMS key in the ``scylla.yaml`` configuration file, or in the table
schema, along with credentials to access this key.

The encryption of user or system data does not happen with KMS keys directly.
Instead, the KMS Key Provider generates another set of keys - the data
encryption keys - and wraps them with the KMS keys to securely store them on
disk along with the encrypted data. The data encryption keys remain encrypted on
disk at all times and they are temporarily stored unencrypted in memory while in
use.

The KMS Key Provider creates data encryption keys via the KMS's
``GenerateDataKey`` operation and decrypts wrapped keys via the ``Decrypt``
operation. It also maintains data encryption keys in a loading cache to reuse
them for the encryption of multiple artifacts. Once a key expires from the
cache, it generates a new one via the KMS.

KMS key rotation is supported, but the rotation must be performed by the user.

.. _ear-key-providers-gcp:

GCP Key Provider
==================

The GCP Key Provider uses keys stored in the Google Cloud Key Management Service
(KMS). The operating principle is similar to the :ref:`KMS Key Provider
<ear-key-providers-kms>`, with the only difference that it generates the data
encryption keys locally. For key wrapping, it relies on the KMS's ``encrypt``
and ``decrypt`` operations.

Azure Key Provider
==================

The Azure Key Provider uses keys stored in the Azure Key Vault service. It
follows the same approach as the :ref:`GCP Key Provider <ear-key-providers-gcp>`,
namely key wrapping with locally generated data encryption keys. Key wrapping is
implemented through Azure Key Vault's ``wrapkey`` and ``unwrapkey`` operations.

.. _ear-create-encryption-key:

Create Encryption Keys
-----------------------------

Depending on your key provider, you will either have the option to allow
ScyllaDB to generate an encryption key, or you will have to provide one:

* Local Key Provider - you can provide your own keys, otherwise ScyllaDB will generate them for you
* Replicated Key Provider - you must generate a system key yourself
* KMIP Key Provider - you can provide your own keys, otherwise ScyllaDB will generate them for you
* KMS Key Provider - you must generate a key yourself in AWS
* GCP Key Provider - you must generate a key yourself in GCP
* Azure Key Provider - you must generate a key yourself in Azure

To create your own key, select one of the following options, depending on your
desired key provider:

.. tabs::

   .. group-tab:: Local Key Provider

      The following procedure describes how you can use ScyllaDB's key generator
      script to generate local keys for your cluster.

      **Procedure**

      #. Select a local directory for storing your keys.

         For example:

         .. code-block:: none

            SCYLLA_LOCAL_KEYS_DIR=/etc/scylla/encryption_keys

      #. Create the directory if it doesn't exist, along with a sub-directory
         for your system keys.

         .. code-block:: none

            sudo mkdir -p ${SCYLLA_LOCAL_KEYS_DIR?}/system_keys

      #. Set the ``scylla`` user as the owner of the directory. Provide
         ``scylla`` user with Read, Write, and Execute permissions over this
         directory.

         .. code-block:: none

            sudo chown -R scylla:scylla ${SCYLLA_LOCAL_KEYS_DIR?}
            sudo chmod -R 700 ${SCYLLA_LOCAL_KEYS_DIR?}

      #. Create a key using the local file key generator script, making sure
         that the key file owner is ``scylla`` and not another user. Run the
         following command:

         .. code-block:: none

          sudo -u scylla /usr/bin/scylla local-file-key-generator <op> [options] [key-path]

         Where:

         * ``<op>`` is either ``generate`` or ``append``. ``generate`` creates a
           new key file with the generated key, while ``append`` appends a new
           key of the required type to an existing key file.

         * ``[options]`` can be any combination of the following:

           * ``-a,--alg <arg>`` - the encryption algorithm (e.g., AES) you want to use to encrypt the key
           * ``-h,--help`` - displays the help menu
           * ``-l,--length <arg>`` - the length of the encryption key in bits (i.e. 128, 256)
           * ``-b,--block-mode <arg>`` - the encryption algorithm block mode (i.e. CBC, EBC)
           * ``-p,--padding <arg>`` - the encryption algorithm padding method (i.e. PKCS5)

         * ``[key-path]`` - is the directory you want to place the key into
            (e.g., ``/etc/scylla/encryption_keys``). This is optional for
            ``append`` if the file already exists.

         For example:

         To create a secret key and a system key in a different location:

         .. code-block:: none

            sudo -u scylla /usr/bin/scylla local-file-key-generator generate -a AES -b CBC -p PKCS5 -l 128 ${SCYLLA_LOCAL_KEYS_DIR?}/secret_key
            sudo -u scylla /usr/bin/scylla local-file-key-generator generate -a AES -b CBC -p PKCS5 -l 128 ${SCYLLA_LOCAL_KEYS_DIR?}/system_keys/system_key

      #. Verify that the script has successfully created the key in your
         selected location.

         To display the secret key parameters:

         .. code-block:: none

            sudo -u scylla cat ${SCYLLA_LOCAL_KEYS_DIR?}/secret_key

         To display the system key parameters:

         .. code-block:: none

            sudo -u scylla cat ${SCYLLA_LOCAL_KEYS_DIR?}/system_keys/system_key

         Both commands should return a single line with the following format:

         .. code-block:: none

            <cipher_algorithm>:<secret_key_length>:<base64encoded_key>

         For example:

         .. code-block:: none

            AES/CBC/PKCS5Padding:128:8stVxW5ypYhNxsnRVS1A6suKhk0sG4Tj

      #. Securely copy the key file, using ``scp`` or similar, to the same path
         on all nodes in the cluster. Make sure the key on each target node is
         moved to the same location as the source directory and that the target
         directory has the same permissions as the source directory.

   .. group-tab:: Replicated Key Provider

      The Replicated Key Provider requires two additional keys:

      * A system key - used to encrypt the data in the system table.
      * A local secret key - used as a fallback key, in case the Replicated Key
        Provider fails to retrieve a key from the system table.

      The system key can be either a local key, or a KMIP key. In either case,
      you have to provide your own key (Scylla will not generate it). Check the
      instructions in the relevant tabs on how to create such keys.

      The local secret key can be created using the same method as the system
      key, but it is also possible to let ScyllaDB generate it for you, in the
      path that you specified in the ``scylla_encryption_options``.

      .. note::

         You cannot use the same key for both the system key and the local
         secret key. They must be different keys.

   .. group-tab:: KMIP Key Provider

      The KMIP Key Provider will first try to discover existing keys in the KMIP
      server, and if it does not find any, it will generate new keys. The key
      discovery is based on attributes. If you wish to provide your own keys,
      make sure to set the following attributes so that the KMIP Key Provider
      can locate them:

      * Object type: symmetric key
      * Algorithm: your desired algorithm (e.g., AES)
      * Key size: your desired key size (e.g., 128 bits)
      * State: active
      * Cryptographic Usage Mask: Encrypt, Decrypt
      * (optional) Key namespace: a namespace for your key(s), - must match the
        ``key_namespace`` option that you will specify in the
        ``scylla_encryption_options`` in the table schema

   .. group-tab:: KMS Key Provider

      To use the KMS Key Provider, you must generate a key yourself in your AWS
      account. The key must be a symmetric key, using the ``SYMMETRIC_DEFAULT``
      key spec. If your key is regional, consider creating it in the same region
      as your ScyllaDB cluster.

      For step-by-step instructions, refer to the `AWS documentation
      <https://docs.aws.amazon.com/kms/latest/developerguide/create-symmetric-cmk.html>`_.

   .. group-tab:: GCP Key Provider

      To use the GCP Key Provider, you must generate a key ring and a key in
      your GCP account. The key must be a symmetric key with the
      ``ENCRYPT_DECRYPT`` purpose. Consider creating your key ring in the same
      location as your ScyllaDB cluster.

      For step-by-step instructions, refer to the GCP documentation for `creating
      a key ring <https://cloud.google.com/kms/docs/create-key-ring>`_ and
      `creating a key <https://cloud.google.com/kms/docs/create-key>`_.

   .. group-tab:: Azure Key Provider

      To use the Azure Key Provider, you must create a key vault and a key in
      your Azure account. The key must be an RSA key with either software or HSM
      protection, and it must permit the ``wrapkey`` and ``unwrapkey`` operations.
      Consider creating your vault in the same location as your ScyllaDB cluster.

      For step-by-step instructions, refer to the Azure documentation for `creating
      a key vault <https://docs.microsoft.com/en-us/azure/key-vault/general/quick-create-portal>`_
      and `generating a key <https://learn.microsoft.com/en-us/azure/key-vault/keys/quick-create-portal>`_.

.. _encryption-at-rest-set-kmip:

Set the KMIP Host
----------------------

If you are using :term:`KMIP <Key Management Interoperability Protocol (KMIP)>`
to encrypt tables or system information, add the KMIP server information to the
``scylla.yaml`` configuration file.

#. Edit the ``scylla.yaml`` file located in ``/etc/scylla/`` and add the
   following in KMIP host(s) section:

   .. code-block:: yaml

       kmip_hosts:
         <name>:
             hosts: <address1[:port]> [, <address2[:port]>...]
             certificate: <identifying certificate> (optional)
             keyfile: <identifying key> (optional; it is required if "certificate" is set)
             truststore: <truststore for SSL connection> (optional)
             priority_string: <kmip tls priority string>
             username: <login> (optional)
             password: <password> (optional)
             max_command_retries: <int> (optional; default 3)
             key_cache_expiry: <key cache expiry period>
             key_cache_refresh: <key cache refresh/prune period>
      #   <name>:

   Where:

   * ``<name>`` - The cluster name.
   * ``hosts`` - The list of hosts specified by IP and port for the KMIP server.
     The KMIP connection management only supports failover, so all requests go
     through a single KMIP server. There is no load balancing, as currently no
     KMIP servers support read replication or other strategies for availability.
     Hosts are tried in the order they appear, and the next one in the list is
     tried if the previous one fails. The default number of retries is three,
     but you can customize it with ``max_command_retries``.
   * ``certificate`` - The name of the certificate and path used to identify yourself to the KMIP server.
   * ``keyfile`` - The name of the key used to identify yourself to the KMIP server. It is generated together with the certificate.
   * ``truststore`` - The location and key for the truststore to present to the KMIP server.
   * ``priority_string`` - The KMIP TLS priority string.
   * ``username`` - The KMIP server user name.
   * ``password`` - The KMIP server password.
   * ``max_command_retries`` - The number of attempts to connect to the KMIP server before trying the next host in the list.
   * ``key_cache_expiry`` - Key cache expiry period, after which keys will be re-requested from server. Default is 30s.
   * ``key_cache_refresh`` - Key cache refresh period - the frequency at which cache is checked for expired entries. Default is 100s.

   Example:

   .. code-block:: yaml

       kmip_hosts:
         my-kmip1:
             hosts: 127.0.0.1:5696
             certificate: /etc/kmip/cert.pem
             keyfile: /etc/kmip/key.pem

#. Save the file.
#. Drain the node with :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>`.
#. Restart the scylla-server service.

   .. include:: /rst_include/scylla-commands-restart-index.rst

.. _encryption-at-rest-set-kms:

Set the KMS Host
----------------------

If you are using AWS KMS to encrypt tables or system information, you need to
add the KMS host information to the ``scylla.yaml`` configuration file.

**Before you Begin**

Before configuring AWS KMS integration, ensure you have:

* An AWS KMS key in your AWS account. Follow the procedure in :ref:`Create
  Encryption Keys <ear-create-encryption-key>` to create it.
* Credentials to authenticate ScyllaDB against the KMS key. Choose one of:

  - **EC2 instance role** (recommended for production) - ScyllaDB will use the IAM role attached to the EC2 instance.
  - **IAM role** (recommended for production) - ScyllaDB will use the provided credentials to assume an IAM role. It allows to grant temporary, limited access to resources.
  - **IAM user** - You will need to supply long-term credentials for this user.
  - **AWS profile** - A profile configured via the AWS CLI.

* Sufficient permissions on the KMS key to perform the ``DescribeKey``,
  ``GenerateDataKey`` and ``Decrypt`` operations on the KMS key.

**Procedure**

#. Edit the ``scylla.yaml`` file located in ``/etc/scylla/`` to add the
   following in KMS host(s) section:

   .. code-block:: yaml

       kms_hosts:
         <name>:
             endpoint: http(s)://<host>(:port) (optional if `aws_region` is specified)
             aws_region: <aws region> (optional if `endpoint` is specified)
             aws_access_key_id: <aws access key id> (optional)
             aws_secret_access_key: <aws secret access key> (optional)
             aws_session_token: <aws session token> (optional)
             aws_profile: <aws credentials profile to use> (optional)
             aws_use_ec2_credentials: (bool : default false)
             aws_use_ec2_region: (bool : default false)
             aws_assume_role_arn: <arn of aws role to assume before call> (optional)
             master_key: <named KMS key for encrypting data keys> (required)
             certificate: <identifying certificate> (optional)
             keyfile: <identifying key> (optional)
             truststore: <truststore for SSL connection> (optional)
             priority_string: <KMS TLS priority string> (optional)
             key_cache_expiry: <key cache expiry period>
             key_cache_refresh: <key cache refresh/prune period>
      #   <name>:

   Where:

   * ``<name>`` - The name to identify the KMS host. You have to provide this name to encrypt a :ref:`new <ear-create-table>` or :ref:`existing
     <ear-alter-table>` table.
   * ``endpoint`` - The explicit KMS host endpoint. If not provided, ``aws_region`` is used for connection.
   * ``aws_region`` - An AWS region. If not provided, ``endpoint`` is used for connection.
   * ``aws_access_key_id`` - AWS access key used for authentication.
   * ``aws_secret_access_key`` - AWS secret access key used for authentication.
   * ``aws_session_token`` - AWS session token. Used for authentication with temporary security credentials, along with access key id and secret access key.
   * ``aws_profile`` - AWS profile to use when reading credentials from the ``~/.aws/credentials`` file. If not provided, the value is read from the
     ``AWS_PROFILE`` environment variable. If neither is defined, it defaults to ``default``.
   * ``aws_use_ec2_credentials`` - If true, KMS queries will use the credentials provided by ec2 instance role metadata as initial access key.
   * ``aws_use_ec2_region`` - If true, KMS queries will use the AWS region indicated by ec2 instance metadata.
   * ``aws_assume_role_arn`` - If set, any KMS query will first attempt to assume this role.
   * ``master_key`` - The ID or alias of your AWS KMS key. The key must be generated with an appropriate access policy so that the AWS user has permissions to
     read the key and encrypt data using that key. This parameter is required.
   * ``certificate`` - The name of the certificate and the path used to identify yourself to the KMS server.
   * ``keyfile`` - The name of the key for the certificate. It is generated together with the certificate.
   * ``truststore`` - The location and key for the truststore to present to the KMS server.
   * ``priority_string`` - The KMS TLS priority string.
   * ``key_cache_expiry`` - Key cache expiry period, after which keys will be re-requested from server. Default is 600s.
   * ``key_cache_refresh`` - Key cache refresh period - the frequency at which cache is checked for expired entries. Default is 1200s.

   Examples:

   .. tabs::

     .. group-tab:: EC2 instance role (recommended)

        .. code-block:: yaml

            kms_hosts:
              my-kms1:
                 aws_use_ec2_credentials: true
                 aws_use_ec2_region: true
                 master_key: myorg/MyKey

     .. group-tab:: IAM role (recommended)

        .. code-block:: yaml

            kms_hosts:
              my-kms1:
                 aws_use_ec2_credentials: true
                 aws_use_ec2_region: true
                 aws_assume_role_arn: arn:aws:iam::123456789012:role/ScyllaDBKMSRole
                 master_key: myorg/MyKey

     .. group-tab:: IAM user

        .. code-block:: yaml

            kms_hosts:
              my-kms1:
                 aws_region: us-east-1
                 aws_access_key_id: THISISANACCESSKEYID
                 aws_secret_access_key: THISISASECRETACCESSKEY
                 master_key: myorg/MyKey

     .. group-tab:: AWS profile

        .. code-block:: yaml

            kms_hosts:
              my-kms1:
                 aws_region: us-east-1
                 aws_profile: user1
                 master_key: myorg/MyKey

   .. note::

      Note that either ``endpoint``, ``aws_region`` or ``aws_use_ec2_region``
      must be set (one of them is required for connection).

   .. note::

      **AWS Credential Resolution**

      If the ``aws_use_ec2_credentials`` option is set to ``false``, and no
      explicit credentials have been provided (i.e., any of the
      ``aws_access_key_id`` and ``aws_secret_access_key`` options is missing),
      ScyllaDB will attempt to automatically resolve credentials in the
      following order:

      1. **Environment Variables**: Checks the ``AWS_ACCESS_KEY_ID`` and
         ``AWS_SECRET_ACCESS_KEY`` environment variables. If set, ScyllaDB uses
         these credentials directly.

      2. **AWS Profile**: Reads credentials from the AWS credentials file
         (``~/.aws/credentials``) using:

         * The profile specified by the ``aws_profile`` option in ``scylla.yaml``.
         * If ``aws_profile`` is not set, uses the profile from the ``AWS_PROFILE``
           environment variable.
         * If neither is defined, defaults to the ``default`` profile.

#. Save the file.
#. Drain the node with :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>`.
#. Restart the scylla-server service.

   .. include:: /rst_include/scylla-commands-restart-index.rst

.. _encryption-at-rest-set-gcp:

Set the GCP Host
----------------------

If you are using GCP Cloud KMS to encrypt tables or system information, you need
to add the GCP host information to the ``scylla.yaml`` configuration file.

**Before you Begin**

Before configuring GCP KMS integration, ensure you have:

* A GCP KMS key ring and key in your GCP account. Follow the procedure in
  :ref:`Create Encryption Keys <ear-create-encryption-key>` to create them.

* Credentials to authenticate ScyllaDB against the KMS key. Choose one of:

  - **Attached service account** (recommended for production) - ScyllaDB will use the service account attached to the VM instance.
  - **Impersonated service account** (recommended for production) - ScyllaDB will use the provided credentials to impersonate a service account. It allows to
    grant temporary, limited access to resources.
  - **Application default credentials** - `A credential file <https://cloud.google.com/docs/authentication/application-default-credentials>`_ configured via the
    gcloud CLI.

* The ``roles/cloudkms.cryptoKeyEncrypterDecrypter`` role assigned to the identity of your choice.

**Procedure**

#. Edit the ``scylla.yaml`` file located in ``/etc/scylla/`` to add the
   following in KMS host(s) section:

   .. code-block:: yaml

       gcp_hosts:
         <name>:
             gcp_project_id: <gcp project>
             gcp_location: <gcp location>
             gcp_credentials_file: <(service) account json key file - authentication>
             gcp_impersonate_service_account: <service account to impersonate>
             master_key: <keyring>/<keyname> - named GCP key for encrypting data keys (required)
             certificate: <identifying certificate> (optional)
             keyfile: <identifying key> (optional)
             truststore: <truststore for SSL connection> (optional)
             priority_string: <KMS TLS priority string> (optional)
             key_cache_expiry: <key cache expiry period>
             key_cache_refresh: <key cache refresh/prune period>
      #   <name>:

   Where:

   * ``<name>`` - The name to identify the GCP host. You have to provide this name to encrypt a :ref:`new <ear-create-table>` or :ref:`existing
     <ear-alter-table>` table.
   * ``gcp_project_id`` - The GCP project from which to retrieve key information.
   * ``gcp_location`` - A GCP project location.
   * ``gcp_credentials_file`` - GCP credentials file used for authentication. If not specified, the provider reads it from your GCP credentials.
   * ``gcp_impersonate_service_account`` - An optional service account to impersonate when issuing key query calls.
   * ``master_key`` - The <keyring>/<keyname> of your GCP KMS key. The key must be generated with an appropriate IAM policy so that the identity has permissions
     to encrypt and decrypt data using that key. This parameter is required.
   * ``certificate`` - The name of the certificate and the path used to identify yourself to the KMS server.
   * ``keyfile`` - The name of the key for the certificate. It is generated together with the certificate.
   * ``truststore`` - The location and key for the truststore to present to the KMS server.
   * ``priority_string`` - The KMS TLS priority string.
   * ``key_cache_expiry`` - Key cache expiry period, after which keys will be re-requested from server. Default is 600s.
   * ``key_cache_refresh`` - Key cache refresh period - the frequency at which cache is checked for expired entries. Default is 1200s.

   Examples:

   .. tabs::

     .. group-tab:: Attached service account (recommended)

        .. code-block:: yaml

            gcp_hosts:
              my-gcp1:
                  gcp_project_id: myproject
                  gcp_location: global
                  master_key: mykeyring/mykey

     .. group-tab:: Impersonated service account (recommended)

        .. code-block:: yaml

            gcp_hosts:
              my-gcp1:
                  gcp_project_id: myproject
                  gcp_location: global
                  gcp_impersonate_service_account: my-service-account@myproject.iam.gserviceaccount.com
                  master_key: mykeyring/mykey

     .. group-tab:: Application default credentials

        .. code-block:: yaml

            gcp_hosts:
              my-gcp1:
                  gcp_project_id: myproject
                  gcp_location: global
                  gcp_credentials_file: /home/user/.config/gcloud/application_default_credentials.json
                  master_key: mykeyring/mykey

   .. note::

      **GCP Credential Resolution**

      If the ``gcp_credentials_file`` option is not specified in the
      configuration,  ScyllaDB follows Google's Application Default Credentials
      (ADC) pattern to automatically resolve credentials in the following order:

      1. **Environment Variable**: Checks the ``GOOGLE_APPLICATION_CREDENTIALS``
         environment variable. If set, ScyllaDB loads credentials from the
         specified file path.

      2. **Well-Known File Locations**: Looks for credentials in standard gcloud
         SDK locations:

         * ``~/{CLOUDSDK_CONFIG}/application_default_credentials.json`` (if ``CLOUDSDK_CONFIG``  environment variable is set)
         * ``~/.config/gcloud/application_default_credentials.json`` (default location)

      3. **Metadata Server**: Attempts to detect if running on a Google Compute
         Engine (GCE) instance by:

         * Querying the metadata server (``metadata.google.internal`` or the host
           specified in ``GCE_METADATA_HOST`` environment variable).
         * Checking for "Google" in ``/sys/class/dmi/id/product_name``.

         If running on GCE, it uses the metadata server to obtain credentials
         for the service account attached to the VM instance.

#. Save the file.
#. Drain the node with :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>`.
#. Restart the scylla-server service.

   .. include:: /rst_include/scylla-commands-restart-index.rst

.. _encryption-at-rest-set-azure:

Set the Azure Host
----------------------

If you are using Azure Key Vault to encrypt tables or system information, you
need to add the Azure information to the ``scylla.yaml`` configuration file.

**Before you Begin**

Before configuring Azure Key Vault integration, ensure you have:

* A key vault and a key in your Azure account. Follow the procedure in
  :ref:`Create Encryption Keys <ear-create-encryption-key>` to create them.

* An Azure identity to authenticate ScyllaDB against the Vault key. Choose one of:

  * **Managed Identity** (recommended for production) - No credentials are needed. ScyllaDB will automatically detect and use the default managed identity of the Azure VM.
  * **Service Principal** - Requires providing a tenant ID, client ID, and a secret or certificate.

* Sufficient permissions to perform the ``wrapkey`` and ``unwrapkey`` operations
  on the key. For example, if your key vault uses RBAC, you can assign the
  built-in "Key Vault Crypto User" role to your identity.

.. note::
   ScyllaDB can also authenticate indirectly through a preconfigured Azure CLI,
   but this is offered only for testing purposes.

**Procedure**

#. Edit the ``scylla.yaml`` file located in ``/etc/scylla/`` to add the
   following in the Azure host(s) section:

   .. code-block:: yaml

      azure_hosts:
        <name>:
            azure_tenant_id: <tenant ID> (optional)
            azure_client_id: <ID of the service principal> (optional)
            azure_client_secret: <secret of the service principal> (optional)
            azure_client_certificate_path: <path to PEM-encoded certificate and private key of the service principal> (optional)
            master_key: <vaultname>/<keyname> - named Vault key for encrypting data keys (required)
            truststore: <PEM file with CA certificates for TLS> (optional)
            priority_string: <TLS priority string> (optional)
            key_cache_expiry: <key cache expiry period> (optional)
            key_cache_refresh: <key cache refresh/prune period> (optional)
      #   <name>:

   Where:

   * ``<name>`` - The name to identify the Azure host. You have to provide this name to encrypt a :ref:`new <ear-create-table>` or :ref:`existing <ear-alter-table>` table.
   * ``azure_tenant_id`` - The ID of the Azure tenant. Required only for authentication with a service principal.
   * ``azure_client_id`` - The client ID of your service principal. Required only for authentication with a service principal.
   * ``azure_client_secret`` - The secret of your service principal. Required only for authentication with a service principal.
   * ``azure_client_certificate_path`` - The path to the PEM-encoded certificate and private key file of your service principal. Can be used instead of a secret.
   * ``master_key`` - The <vaultname>/<keyname> of your key. This parameter is required.
   * ``truststore`` - Path to a PEM file with CA certificates to validate the server's TLS certificate.
   * ``priority_string`` - The TLS priority string for TLS handshakes.
   * ``key_cache_expiry`` - Key cache expiry period, after which keys will be re-requested from Azure Key Vault. Default is 600s.
   * ``key_cache_refresh`` - Key cache refresh period - the frequency at which the cache is checked for expired entries. Default is 1200s.

   Example:

   .. tabs::

      .. group-tab:: Managed Identity (recommended)

         .. code-block:: yaml

            azure_hosts:
              my-azure1:
                  master_key: mykeyvault/mykey

      .. group-tab:: Service Principal with Secret

         .. code-block:: yaml

            azure_hosts:
              my-azure1:
                  azure_tenant_id: 00000000-1111-2222-3333-444444444444
                  azure_client_id: 55555555-6666-7777-8888-999999999999
                  azure_client_secret: mysecret
                  master_key: mykeyvault/mykey

      .. group-tab:: Service Principal with Certificate

         .. code-block:: yaml

            azure_hosts:
              my-azure1:
                  azure_tenant_id: 00000000-1111-2222-3333-444444444444
                  azure_client_id: 55555555-6666-7777-8888-999999999999
                  azure_client_certificate_path: /path/to/certificate.pem
                  master_key: mykeyvault/mykey

   .. note::

      **Azure Credential Resolution**

      If no credentials are provided in the configuration file, ScyllaDB will
      attempt to automatically resolve credentials in the following order:

      1. **Environment Variables**: Checks the ``AZURE_TENANT_ID``,
         ``AZURE_CLIENT_ID``, ``AZURE_CLIENT_SECRET``, and
         ``AZURE_CLIENT_CERTIFICATE_PATH`` environment variables. If set,
         ScyllaDB uses these credentials directly. If both ``AZURE_CLIENT_SECRET``
         and ``AZURE_CLIENT_CERTIFICATE_PATH`` are set, the secret takes
         precedence.

      2. **Azure CLI**: If the Azure CLI is installed and configured, ScyllaDB
         will be using it to generate access tokens.

      3. **Azure Instance Metadata Service (IMDS)**: If running on an Azure VM
         with an assigned managed identity, ScyllaDB will be querying the IMDS
         for access tokens.

#. Save the file.
#. Drain the node with :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>`
#. Restart the scylla-server service.

.. include:: /rst_include/scylla-commands-restart-index.rst

Encrypt Tables
-----------------------------

ScyllaDB allows you to enable or disable default encryption of tables. When
enabled, tables will be encrypted by default using the configuration provided
for the ``user_info_encryption`` option in the ``scylla.yaml`` file.

You can override the default configuration when you CREATE TABLE or ALTER TABLE
with ``scylla_encryption_options``. See :ref:`Encrypt a Single Table
<ear-create-table>` for details.

**Before you Begin**

Ensure you have an encryption key available:

* If you are using AWS KMS, :ref:`set the KMS Host <encryption-at-rest-set-kms>`.
* If you are using KMIP, :ref:`set the KMIP Host <encryption-at-rest-set-kmip>`.
* If you are using Google GCP KMS, :ref:`set the GCP Host <encryption-at-rest-set-gcp>`.
* If you are using Azure Key Vault, :ref:`set the Azure Host <encryption-at-rest-set-azure>`.
* If you want to create your own key, follow the procedure in :ref:`Create Encryption Keys <ear-create-encryption-key>`.
* If you do not create your own key, use the following procedure for ScyllaDB to create a key for you (the default location ``/etc/scylla/data_encryption_keys``
  may cause permission issues; the following example creates a key in the directory ``/etc/scylla/encryption_keys``):

   .. code-block:: none

      sudo mkdir -p /etc/scylla/encryption_keys
      sudo chown -R scylla:scylla /etc/scylla/encryption_keys
      sudo chmod -R 700 /etc/scylla/encryption_keys

**Procedure**

Edit the ``scylla.yaml`` file located in ``/etc/scylla/`` and configure the
``user_info_encryption`` option:

.. code-block:: yaml

   user_info_encryption:
    enabled: <true|false>
    cipher_algorithm: <hashing algorithm to create the key>
    secret_key_strength: <length of the key>
    key_provider: <your key provider>
    secret_key_file: <key file>
    kmip_host: <your kmip_host>
    kms_host: <your kms_host>
    gcp_host: <your gcp_host>
    azure_host: <your azure_host>

Where:

* ``enabled`` - Enables or disables default table encryption. Required.
* ``cipher_algorithm`` - One of the :ref:`cipher algorithms <ear-cipher-algorithms>`. If not provided, the default will be used.
* ``secret_key_strength`` - The length of the key in bytes ( determined by the :ref:`cipher algorithms <ear-cipher-algorithms>` you choose). If not provided,
  the default will be used.
* ``key_provider`` - The name of the key provider. See :ref:`Key Providers <ear-key-providers>`. Required.
* ``secret_key_file`` - The location of the key created by ScyllaDB (by default ``/etc/scylla/data_encryption_keys``). Required if you use a ScyllaDB-generated
  key.
* ``kmip_host`` - The name of your :ref:`kmip_host <encryption-at-rest-set-kmip>` group. Required if you use KMIP.
* ``kms_host`` - The name of your :ref:`kms_host <encryption-at-rest-set-kms>` group. Required if you use KMS.
* ``gcp_host`` - The name of your :ref:`gcp_host <encryption-at-rest-set-gcp>` group. Required if you use GCP.
* ``azure_host`` - The name of your :ref:`azure_host <encryption-at-rest-set-azure>` group. Required if you use Azure.

**Example**

.. code-block:: yaml

   user_info_encryption:
    enabled: true
    cipher_algorithm: AES
    secret_key_strength: 128
    key_provider: LocalFileSystemKeyProviderFactory
    secret_key_file: scylla /etc/scylla/encryption_keys

**Examples for KMS:**

In the following example, the ``master_key`` configured for :ref:`kms_host
<encryption-at-rest-set-kms>` will be used.

.. code-block:: yaml

   user_info_encryption:
    enabled: true
    key_provider: KmsKeyProviderFactory
    kms_host: my-kms1


You can specify a different ``master_key`` than the one configured for :ref:`kms_host
<encryption-at-rest-set-kms>`:

 .. code-block:: yaml

   user_info_encryption:
    enabled: true
    key_provider: KmsKeyProviderFactory
    kms_host: my-kms1
    master_key: myorg/SomeOtherKey

.. _ear-create-table:

Encrypt a Single Table
-----------------------------

This procedure demonstrates how to encrypt a new table.

**Before you Begin**

* Make sure to `Set the KMIP Host`_ if you are using KMIP, or the the :ref:`KMS Host
  <encryption-at-rest-set-kms>` if you are using AWS KMS.

* If you want to make your own key, use the procedure in `Create Encryption Keys`_
  and skip to step 3. If you do not create your own key, ScyllaDB will create
  one for you in the ``secret_key_file`` path. If you are not creating your own
  key, start with step 1.

**Procedure**

#. By default, the encryption key is located in the ``/etc/scylla/`` directory,
   and the file is named ``data_encryption_keys``. If you want to save the key
   in a different directory, create one. This example will create encryption
   keys in a different directory (``/etc/scylla/encryption_keys``, for example),
   which ensures that the owner of this directory is ``scylla`` and not another
   user.

   .. note:: Using the default location results in a known permission issue
      (scylladb/scylla-tools-java#94), so it is recommended to use another
      location as described in the example.

   .. code-block:: none

      sudo mkdir -p /etc/scylla/encryption_keys
      sudo chown -R scylla:scylla /etc/scylla/encryption_keys
      sudo chmod -R 700 /etc/scylla/encryption_keys

#. Create the keyspace if it doesn’t exist.

#. Create the table using the ``CREATE TABLE`` CQL statement, adding any
   :ref:`additional options <create-table-statement>`. To encrypt the table, use
   the options for encryption below, remembering to set the ``secret_key_file <path>``
   to the same directory you created in step 1.

   .. code-block:: cql

      CREATE TABLE <keyspace>.<table_name> (...<columns>...) WITH
        scylla_encryption_options = {
          'cipher_algorithm' : <hash>,
          'secret_key_strength' : <len>,
          'key_provider': <provider>,
          'secret_key_file': <path>
        }
      ;

   Where:

   * ``cipher_algorithm`` -  The hashing algorithm which is to be used to create the key. See `Cipher Algorithms`_ for more information.
   * ``secret_key_strength`` - The length of the key in bytes. This is determined by the cipher you choose. See `Cipher Algorithms`_ for more information.
   * ``key_provider`` is the name or type of key provider. Refer to `Key Providers`_ for more information.
   * ``secret_key_file`` - the location that ScyllaDB will store the key it creates (if one does not exist in this location) or the location of the key. By
     default the location is ``/etc/scylla/data_encryption_keys``.

   **Example:**

   Continuing the example from above, this command will instruct ScyllaDB to
   encrypt the table and will save the key in the location created in step 1.

   .. code-block:: cql

      CREATE TABLE data.atrest (pk text primary key, c0 int) WITH
        scylla_encryption_options = {
          'cipher_algorithm' : 'AES/ECB/PKCS5Padding',
          'secret_key_strength' : 128,
          'key_provider': 'LocalFileSystemKeyProviderFactory',
          'secret_key_file': '/etc/scylla/encryption_keys/data_encryption_keys'
        }
      ;

   **Example for KMS:**

   .. code-block:: cql

      CREATE TABLE myks.mytable (...<columns>...) WITH
        scylla_encryption_options = {
          'cipher_algorithm' :  'AES/CBC/PKCS5Padding',
          'secret_key_strength' : 128,
          'key_provider': 'KmsKeyProviderFactory',
          'kms_host': 'my-kms1'
        }
      ;

   You can skip ``cipher_algorithm`` and ``secret_key_strength`` (the
   :ref:`defaults <ear-cipher-algorithms>` will be used):

   .. code-block:: cql

      CREATE TABLE myks.mytable (...<columns>...) WITH
        scylla_encryption_options = {
          'key_provider': 'KmsKeyProviderFactory',
          'kms_host': 'my-kms1'
        }
      ;

.. _ear-create-table-master-key-override:

   You can specify a different master key than the one configured for ``kms_host``
   in the ``scylla.yaml`` file:

   .. code-block:: cql

      CREATE TABLE myks.mytable (...<columns>...) WITH
        scylla_encryption_options = {
          'key_provider': 'KmsKeyProviderFactory',
          'kms_host': 'my-kms1',
          'master_key':'myorg/SomeOtherKey'
        }
      ;


#. From this point, every new SSTable created for the ``atrest`` table is
   encrypted, using the ``data_encryption_keys`` key located in
   ``/etc/scylla/encryption_keys/``. This table will remain encrypted with this
   key until you either change the key, change the key properties, or disable
   encryption.

#. To ensure all SSTables for this table on every node are encrypted, run the
   :doc:`Nodetool upgradesstables
   </operating-scylla/nodetool-commands/upgradesstables>` command. If not, the
   SSTables remain unencrypted until they are compacted or flushed from
   MemTables.

   For Example:

   .. code-block:: none

      nodetool upgradesstables data atrest

#. Your SSTables are encrypted. If you want to change the key at any point, use
   the `Update Encryption Properties of Existing Tables`_ procedure. Always keep
   your key in a safe location known to you. Do not lose it. See `When a Key is Lost`_.

.. _ear-alter-table:

Update Encryption Properties of Existing Tables
==================================================

You can encrypt any existing table or use this procedure to change the cipher
algorithm, key location or key strength or even disable encryption on a table.

**Procedure**

#. Edit the table properties to enable encryption of one table of your choosing.
   Use the properties explained in `Encrypt a Single Table`_ if needed.

   .. code-block:: cql

      ALTER TABLE <keyspace>.<table_name> (...<columns>...) WITH
        scylla_encryption_options = {
          'cipher_algorithm' : <hash>,
          'secret_key_strength' : <len>,
          'key_provider': <provider>,
          'secret_key_file': <path>
        }
      ;


   **Example:**

   Continuing the example from above, this command will instruct ScyllaDB to
   encrypt the table and will save the key in the location created in step 1.

   .. code-block:: cql

      ALTER TABLE data.atrest (pk text primary key, c0 int) WITH
        scylla_encryption_options = {
          'cipher_algorithm' : 'AES/ECB/PKCS5Padding',
          'secret_key_strength' : 192,
          'key_provider': 'LocalFileSystemKeyProviderFactory',
          'secret_key_file': '/etc/scylla/encryption_keys/data_encryption_keys'
        }
      ;

   **Example for KMS:**

   .. code-block:: cql

      ALTER TABLE myks.mytable (...<columns>...) WITH
        scylla_encryption_options = {
          'cipher_algorithm' :  'AES/CBC/PKCS5Padding',
          'secret_key_strength' : 128,
          'key_provider': 'KmsKeyProviderFactory',
          'kms_host': 'my-kms1'
        }
      ;

#. If you want to make sure that SSTables that existed before this change are
   also encrypted, you can either upgrade them using the ``nodetool upgradesstables``
   command or wait until the next compaction. If you decide to wait, ScyllaDB
   will still be able to read the old unencrypted tables. If you change the key
   or remove encryption, ScyllaDB will still continue to read the old tables as
   long as you still have the key. If your data is encrypted and you do not have
   the key, your data is unreadable.

   * If you decide to upgrade all of your old SSTables run the :doc:`nodetool upgradesstables
     </operating-scylla/nodetool-commands/upgradesstables>` command.

     .. code-block:: none

         nodetool upgradesstables <keyspace> <table>

     For example:

     .. code-block:: none

         nodetool upgradesstables ks test

   * Repeat this command on all nodes as nodetool runs locally.

#. If you want to change the key or disable encryption, repeat the `Update
   Encryption Properties of Existing Tables`_ procedure using the examples below
   as reference.

**Examples**

To encrypt an existing table named test in keyspace ks:

.. code-block:: cql

   ALTER TABLE ks.test WITH
     scylla_encryption_options = {
        'cipher_algorithm' : 'AES/ECB/PKCS5Padding',
        'secret_key_strength' : 128,
        'key_provider': 'LocalFileSystemKeyProviderFactory',
        'secret_key_file': '/etc/scylla/encryption_keys/data_encryption_keys'
     }
   ;


To change the cipher algorithm from AES/ECB/PKCS5Padding to AES/ECB/PKCS5Padding
and to change the key strength from 128 to 192 on an existing table:

.. code-block:: cql

   ALTER TABLE ks.test WITH
     scylla_encryption_options = {
        'cipher_algorithm' : 'AES/ECB/PKCS5Padding',
        'secret_key_strength' : 192,
        'key_provider': 'LocalFileSystemKeyProviderFactory',
        'secret_key_file': '/etc/scylla/encryption_keys/data_encryption_keys'
     }
   ;

To disable encryption on an encrypted table named test in keyspace ks:

.. code-block:: cql

   ALTER TABLE ks.test WITH
      scylla_encryption_options =  { 'key_provider' : 'none’ };


Encrypt System Resources
---------------------------

System encryption is applied to semi-transient on-disk data, such as commit
logs, batch logs, and hinted handoff data. This ensures that all temporarily
stored data is encrypted until fully persisted to final SSTable on disk. Once
this encryption is enabled, it is used for all system data.


**Procedure**

#. Edit the scylla.yaml file - located in /etc/scylla/scylla.yaml and add the
   following:

   .. code-block:: none

      system_info_encryption:
         enabled: <true|false>
         key_provider: (optional) <key provider type>
         system_key_directory: <path to location of system key>

   Where:

   * ``enabled`` can be true or false. True is enabled; false is disabled.

   * ``key_provider`` is the name or type of key provider. Refer to `Key Providers`_ for more information.

   * ``cipher_algorithm`` is one of the supported `Cipher Algorithms`_.

   * ``secret_key_file`` is the name of the key file containing the secret key (key.pem, for example)

   Example:

   .. code-block:: none

      system_info_encryption:
         enabled: True
         cipher_algorithm: AES
         secret_key_strength: 128
         key_provider: LocalFileSystemKeyProviderFactory
         secret_key_file: /path/to/systemKey.pem

   Example for KMIP:

   .. code-block:: none

      system_info_encryption:
         enabled: True
         cipher_algorithm: AES
         secret_key_strength: 128
         key_provider: KmipKeyProviderFactory
         kmip_host:  yourkmipServerIP.com

   Where ``kmip_host`` is the address for your KMIP server.

   Example for KMS:

   .. code-block:: none

      system_info_encryption:
         enabled: True
         cipher_algorithm: AES/CBC/PKCS5Padding
         secret_key_strength: 128
         key_provider: KmsKeyProviderFactory
         kms_host: myScylla

   Where ``kms_host`` is the unique name of the KMS host specified in the scylla.yaml file.

   Example for GCP:

   .. code-block:: none

      system_info_encryption:
         enabled: True
         cipher_algorithm: AES/CBC/PKCS5Padding
         secret_key_strength: 128
         key_provider: GcpKeyProviderFactory
         gcp_host: myScylla

   Where ``gcp_host`` is the unique name of the GCP host specified in the
   scylla.yaml file.

   Example for Azure:

   .. code-block:: none

      system_info_encryption:
         enabled: True
         cipher_algorithm: AES/CBC/PKCS5Padding
         secret_key_strength: 128
         key_provider: AzureKeyProviderFactory
         azure_host: myScylla

   Where ``azure_host`` is the unique name of the Azure host specified in the scylla.yaml file.

#. Do not close the yaml file. Change the system key directory location
   according to your settings.

   * ``system_key_directory`` is the location of the system key you created in `Create Encryption Keys`_.

   .. code-block:: none

      system_key_directory: /etc/scylla/encryption_keys/system_keys

#. Save the file.
#. Drain the node with :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>`
#. Restart the scylla-server service.

   .. include:: /rst_include/scylla-commands-restart-index.rst

   .. wasn't able to test this successfully


When a Key is Lost
----------------------

It is crucial to back up all of your encryption keys in a secure way. Keep a
copy of all keys in a secure location. In the event that you do lose a key, your
data encrypted with that key will be unreadable.

Additional Resources
----------------------

* :doc:`nodetool upgradesstables </operating-scylla/nodetool-commands/upgradesstables>`
* :ref:`CREATE TABLE parameters <create-table-statement>`

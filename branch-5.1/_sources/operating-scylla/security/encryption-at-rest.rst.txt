==================
Encryption at Rest
==================


.. versionadded:: 2019.1.1 Scylla Enterprise
.. versionchanged:: 2019.1.3 Scylla Enterprise

.. include:: /rst_include/enterprise-only-note.rst

Scylla Enterprise protects your sensitive data with data-at-rest encryption. 
It protects the privacy of your user's data, reduces the risk of data breaches, and helps meet regulatory requirements. 
In particular, it provides an additional level of protection for your data persisted in storage or its backups.

When Scylla Enterprise Encryption at Rest is used together with Encryption in Transit (:doc:`Node to Node </operating-scylla/security/node-node-encryption>`  and :doc:`Client to Node </operating-scylla/security/client-node-encryption>`), you benefit from end to end data encryption.

.. note:: Encryption at Rest is available for Scylla Enterprise customers version 2019.1.1 and later. :abbr:`KMIP (Key Management Interoperability Protocol)` support is available for Scylla Enterprise customers version 2019.1.3 and later.

About Encryption at Rest
========================

The following can be encrypted:

* Scylla persistent tables (SSTables)
* System level data, such as:

  - Commit logs
  - Batches
  - hints logs
  - KMIP Password (part of scylla.yaml)

Encryption at Rest works at table level granularity, so you can choose to encrypt only sensitive tables. For both system and table data, you can use different algorithms that are supported by `OpenSSL <https://www.openssl.org/>`_ in a file block encryption scheme. 

.. note:: SSTables of a particular table can have different encryption keys, use different encryption algorithms, or not be encrypted at all - at the same time. 

When is Data Encrypted?
========================

As SSTables are immutable, tables are encrypted only once, as a result of memtable flush, compaction, or upgrade (with :doc:`Nodetool upgradesstables </operating-scylla/nodetool-commands/upgradesstables>`).

Once a table is encrypted, all resulting SSTables are encrypted using the most current key and algorithm. 
When you encrypt an existing table, the new SSTables are encrypted. The old SSTables which existed before the encryption are not updated. These tables are encrypted according to the same actions as described previously. 

When is Data Decrypted?
========================

When Scylla reads an encrypted SSTable from disk, it fetches the encryption key's ID from the SSTable and uses it to extract the key and decrypt the data.
When Scylla reads an encrypted system table, it fetches the system table encryption key location from the scylla.yaml file. It locates the key and uses it to extract the key and decrypt the data. 


Encryption Keys
===============

Two types of encryption keys are available: System Keys and Table Keys. 

System Keys
-----------

System keys are used for encrypting system data, such as commit logs, hints, and/or other user table keys. When a Replicated Key Provider is used for encrypting SSTables, the table keys are stored in the encrypted_keys table, and the system key is used to encrypt the encrypted_keys table. The system key is stored as the contents of a local file and is encrypted with a single key that you provide. The default location of system keys is ``/etc/scylla/resources/system_keys/`` and can be changed with the ``system_key_directory`` option in scylla.yaml file. When a Local Key Provider is used for encrypting system info, you can provide your own key, or Scylla can make one for you.

.. _Replicated:

Table Keys
----------
Table keys are used for encrypting SSTables. Depending on your key provider, this key is stored in different locations:

* Replicated Key Provider - encrypted_keys table
* KMIP Key Provider - KMIP server
* Local Key Provider - in a local file with multiple keys. You can provide your own key or Scylla can make one for you.

Key Providers
=============

When encrypting the system tables or SSTables, you need to state which provider is holding your keys. You can use the following options:

.. list-table::
   :widths: 33 33 33
   :header-rows: 1

   * - Key Provider Name
     - key_provider Name
     - Description 
   * - Local Key Provider 
     - LocalFileSystemKeyProviderFactory (**default**)
     - Stores the key on the same machine as the data.   
   * - Replicated Key Provider 
     - ReplicatedKeyProviderFactory
     - Stores table keys in a Scylla table where the table itself is encrypted using the system key (available from 2019.1.3)
   * - KMIP Key Provider 
     - KmipKeyProviderFactory
     - External key management server (available from 2019.1.3)

Cipher Algorithms
=================

The following cipher_algorithims are available for use with Scylla using `OpenSSL <https://www.openssl.org/>`_. Note that the default algorithm (AES/CBC/PKCS5Padding with key strength 128 ) is recommended.

.. list-table::
   :widths: 70 30
   :header-rows: 1

   * - cipher_algorithm 
     - secret_key_strength
   * - AES/CBC/PKCS5Padding (**default**)
     - 128 (**default**), 192, or 256 
   * - AES/ECB/PKCS5Padding
     - 128, 192, or 256
   * - Blowfish/CBC/PKCS5Padding    
     - 32-448
   * - DES/CBC/PKCS5Padding
     - 56
   * - DESede/CBC/PKCS5Padding
     - 112 or 168
   * - RC2/CBC/PKCS5Padding
     - 40-128 

About Local Key Storage
=======================

Local keys are used for encrypting user data, such as SSTables. 
Currently, this is the only option  available for user data and, as such, is the default key storage manager. 
With local key storage, keys are stored locally on disk in a text file. The location of this file is specified in the scylla.yaml. 

.. caution:: Care should be taken so that no unauthorized person can access the key data from the file system. Make sure that the owner of this file is the ``scylla`` user and that the file is **not** readable by **other users**, not accessible by **other roles**. 

You should also consider keeping the key directory on a network drive (using TLS for the file sharing) to avoid having keys and data on the same storage media, in case your storage is stolen or discarded. 

Encrypt a Single Table
======================

This procedure demonstrates how to encrypt a new table.

**Before you Begin**

* Make sure to `Set the KMIP Host`_ if you are using KMIP.

* If you want to make your own key, use the procedure in `Create Encryption Keys`_ and skip to step 3. If you do not create your own key, Scylla will create one for you in the ``secret_key_file`` path. If you are not creating your own key, start with step 1.  

**Procedure**

#. By default, the encryption key is located in the ``/etc/scylla/`` directory, and the file is named ``data_encryption_keys``. If you want to save the key in a different directory, create one. This example will create encryption keys in a different directory (``/etc/scylla/encryption_keys``, for example), which ensures that the owner of this directory is ``scylla`` and not another user.

   .. note:: Using the default location results in a known permission issue (scylladb/scylla-tools-java#94), so it is recommended to use another location as described in the example.

   .. code-block:: none
   
      sudo mkdir -p /etc/scylla/encryption_keys
      sudo chown -R scylla:scylla /etc/scylla/encryption_keys
      sudo chmod -R 700 /etc/scylla/encryption_keys

#. Create the keyspace if it doesn’t exist.

#. Create the table using the ``CREATE TABLE`` CQL statement, adding any :ref:`additional options <create-table-statement>`. To encrypt the table, use the options for encryption below, remembering to set the ``secret_key_file <path>`` to the same directory you created in step 1.

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
   * ``secret_key_file`` - the location that Scylla will store the key it creates (if one does not exist in this location) or the location of the key. By default the location is ``/etc/scylla/data_encryption_keys``. 

   Example:

   Continuing the example from above, this command will instruct Scylla to encrypt the table and will save the key in the location created in step 1. 

   .. code-block:: cql

      CREATE TABLE data.atrest (pk text primary key, c0 int) WITH 
        scylla_encryption_options = {  
          'cipher_algorithm' : 'AES/ECB/PKCS5Padding', 
          'secret_key_strength' : 128,  
          'key_provider': 'LocalFileSystemKeyProviderFactory',  
          'secret_key_file': '/etc/scylla/encryption_keys/data_encryption_keys'
        }
      ;

#. From this point, every new SSTable created for the ``atrest`` table is encrypted, using the ``data_encryption_keys`` key located in ``/etc/scylla/encryption_keys/``. This table will remain encrypted with this key until you either change the key, change the key properties, or disable encryption.

#. To ensure all SSTables for this table on every node are encrypted, run the :doc:`Nodetool upgradesstables </operating-scylla/nodetool-commands/upgradesstables>` command. If not, the SSTables remain unencrypted until they are compacted or flushed from MemTables. 

   For Example:

   .. code-block:: none

      nodetool upgradesstables data atrest

#. Your SSTables are encrypted. If you want to change the key at any point, use the `Update Encryption Properties of Existing Tables`_ procedure. Always keep your key in a safe location known to you. Do not lose it. See `When a Key is Lost`_.

Update Encryption Properties of Existing Tables
-----------------------------------------------

You can encrypt any existing table or use this procedure to change the cipher algorithm, key location or key strength or even disable encryption on a table. 

**Procedure**

#. Edit the table properties to enable encryption of one table of your choosing. Use the properties explained in `Encrypt a Single Table`_ if needed. 

   .. code-block:: cql

      ALTER TABLE <keyspace>.<table_name> (...<columns>...) WITH 
        scylla_encryption_options = { 
          'cipher_algorithm' : <hash>,   
          'secret_key_strength' : <len>,   
          'key_provider': <provider>, 
          'secret_key_file': <path> 
        }
      ;


   Example:

   Continuing the example from above, this command will instruct Scylla to encrypt the table and will save the key in the location created in step 1. 

   .. code-block:: cql

      ALTER TABLE data.atrest (pk text primary key, c0 int) WITH 
        scylla_encryption_options = {  
          'cipher_algorithm' : 'AES/ECB/PKCS5Padding', 
          'secret_key_strength' : 192,  
          'key_provider': 'LocalFileSystemKeyProviderFactory',  
          'secret_key_file': '/etc/scylla/encryption_keys/data_encryption_keys'
        }
      ;



#. If you want to make sure that SSTables that existed before this change are also encrypted, you can either upgrade them using the ``nodetool upgradesstables`` command or wait until the next compaction. If you decide to wait, Scylla will still be able to read the old unencrypted tables. If you change the key or remove encryption, Scylla will still continue to read the old tables as long as you still have the key. If your data is encrypted and you do not have the key, your data is unreadable. 

   * If you decide to upgrade all of your old SSTables run the :doc:`nodetool upgradesstables </operating-scylla/nodetool-commands/upgradesstables>` command. 

     .. code-block:: none

         nodetool upgradesstables <keyspace> <table>

     For example:

     .. code-block:: none

         nodetool upgradesstables ks test  

   * Repeat this command on all nodes as nodetool runs locally. 
 
#. If you want to change the key or disable encryption, repeat the `Update Encryption Properties of Existing Tables`_ procedure using the examples below as reference. 

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


To change the cipher algorithm from AES/ECB/PKCS5Padding to DES/CBC/PKCS5Padding and to change the key strength from 128 to 56 on an existing table:

.. code-block:: cql
  
   ALTER TABLE ks.test WITH
     scylla_encryption_options = { 
        'cipher_algorithm' : 'DES/CBC/PKCS5Padding', 
        'secret_key_strength' : 56,  
        'key_provider': 'LocalFileSystemKeyProviderFactory',  
        'secret_key_file': '/etc/scylla/encryption_keys/data_encryption_keys'
     }
   ;

To disable encryption on an encrypted table named test in keyspace ks:

.. code-block:: cql

   ALTER TABLE ks.test WITH
      scylla_encryption_options =  { 'key_provider' : 'none’ };


Create Encryption Keys
======================

Depending on your key provider, you will either have the option of allowing Scylla to generate an encryption key, or you will have to provide one:

* KMIP Key Provider - you don't need to generate any key yourself
* Replicated Key Provider - you must generate a system key yourself
* Local Key Provider - If you do not generate your own secret key, Scylla will create one for you

When encrypting Scylla config by ``configuration_encryptor``, you also need to generate a secret key and upload the key to all nodes.


Use the key generator script
----------------------------

The Key Generator script generates a key in the directory of your choice. 

**Procedure**


#. Create (if it doesn't exist) a local directory for storing the key.  Make sure that the owner of the directory is ``scylla`` and not another user. Make sure that the ``scylla`` user can read, write, and execute over the parent directory. Following this procedure makes ``/etc/scylla/encryption_keys/`` the parent directory of your keys.

   For example:

   .. code-block:: none

      sudo mkdir -p /etc/scylla/encryption_keys/system_keys
      sudo chown -R scylla:scylla /etc/scylla/encryption_keys
      sudo chmod -R 700 /etc/scylla/encryption_keys

#. Create a key using the local file key generator script making sure that the keyfile owner is scylla and not another user. Run the command:

   .. code-block:: none

      sudo -u scylla /bin/local_file_key_generator [options] [key-path]

   Where:

   * ``-a,--alg <arg>`` - the encryption algorithm (i.e. AES, 3DES) you want to use to encrypt the key
   * ``-c,--append`` - appends the output to the key file (default is to overwrite)
   * ``-h,--help`` - displays the help menu
   * ``-l,--length <arg>`` - the length of the encryption key in bits (i.e. 128, 256)
   * ``-m,--block-mode <arg>`` - the encryption algorithm block mode (i.e. CBC, EBC)
   * ``-p,--padding <arg>`` - the encryption algorithm padding method (i.e. PKCS5)
   * ``key-path`` - is the directory you want to place the key into (/etc/scylla/encryption_keys, for example)


   For Example:

   To create a secret key and a system key using other encryption settings in a different location:

   .. code-block:: none

      sudo -u scylla /bin/local_file_key_generator -a AES -m ECB -p PKCS5 -l 192 /etc/scylla/encryption_keys/secret_key
      sudo -u scylla /bin/local_file_key_generator -a AES -m CBC -p PKCS5 -l 128 /etc/scylla/encryption_keys/system_keys/system_key
   
   To display the secret key parameters:

   .. code-block:: none

      sudo cat /etc/scylla/encryption_keys/secret_key

   Returns:
   
   .. code-block:: none
   
      AES/ECB/PKCS5Padding:192:8stVxW5ypYhNxsnRVS1A6suKhk0sG4Tj

   To display the system key parameters:
   
   .. code-block:: none
   
      sudo cat /etc/scylla/encryption_keys/system_keys/system_key

   Returns:
   
   .. code-block:: none
   
      AES/CBC/PKCS5Padding:128:GGpOSxTGhtPRPLrNPYvVMQ==


   Once you have created a key, copy the key to each node, using the procedure described in `Copy keys to nodes`_.

Copy keys to nodes
------------------

Every key you generate needs to be copied to the nodes for use in local key providers. 

**Procedure**

#. Securely copy the key file, using ``scp`` or similar, to the same path on all nodes in the cluster. Make sure the key on each target node is moved to the same location as the source directory and that the target directory has the same permissions as the source directory. 

#. Repeat for all nodes in the cluster.

Encrypt System Resources
========================

System encryption is applied to semi-transient on-disk data, such as commit logs, batch logs, and hinted handoff data. 
This ensures that all temporarily stored data is encrypted until fully persisted to final SSTable on disk. 
Once this encryption is enabled, it is used for all system data. 


**Procedure**

#. Edit the scylla.yaml file - located in /etc/scylla/scylla.yaml and add the following:

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

   Where: 

   * ``kmip_host`` - is the address for your KMIP server

#. Do not close the yaml file. Change the system key directory location according to your settings. 

   * ``system_key_directory`` is the location of the system key you created in `Create Encryption Keys`_.

   .. code-block:: none

      system_key_directory: /etc/scylla/encryption_keys/system_keys

#. Save the file. 
#. Drain the node with :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>`
#. Restart the scylla-server service.

   .. include:: /rst_include/scylla-commands-restart-index.rst 

   .. wasn't able to test this successfully

.. Encrypt and Decrypt Configuration Files
.. =======================================

.. Using the Configuration Encryption tool, you can encrypt parts of the scylla.yaml file which contain encryption configuration settings. 

.. **Procedure**

.. 1.  Run the Configuration Encryption script:

.. test code-block: none

.. /bin/configuration_encryptor [options] [key-path]

.. Where:

.. * ``-c, --config`` - the path to the configuration file (/etc/scylla/scylla.yaml, for example)
.. * ``-d, --decrypt`` - decrypts the configuration file at the specified path
.. * ``-o, --output`` - (optional) writes the configuration file to a specified target. This can be the same location as the source file. 
.. * ``-h. --help`` - help for this command

.. For example:

.. test code-block: none

.. sudo -u scylla /bin/configuration_encryptor -c /etc/scylla/scylla.yaml /etc/scylla/encryption_keys/secret_key
.. end of test


Set the KMIP Host
=================

If you are using :term:`KMIP <Key Management Interoperability Protocol (KMIP)>` to encrypt tables or system information, add the KMIP server information to the ``scylla.yaml`` configuration file. 

#. Edit the ``scylla.yaml`` file located in ``/etc/scylla/`` and add the following in KMIP host(s) section:

   .. code-block:: yaml

      #
      # kmip_hosts:
      #   <name>:
      #       hosts: <address1[:port]> [, <address2[:port]>...]
      #       certificate: <identifying certificate> (optional)
      #       keyfile: <identifying key> (optional; it is required if "certificate" is set)
      #       truststore: <truststore for SSL connection> (optional)
      #       certficate_revocation_list: <CRL file> (optional)
      #       priority_string: <kmip tls priority string> 
      #       username: <login> (optional>
      #       password: <password> (optional)
      #       max_command_retries: <int> (optional; default 3)
      #   <name>:
   
   Where:

   * ``<name>`` - The cluster name.
   * ``hosts`` - The list of hosts specified by IP and port for the KMIP server. The KMIP connection management only supports failover, so all requests go through a single KMIP server. There is no load balancing, as currently no KMIP servers support read replication or other strategies for availability. Hosts are tried in the order they appear, and the next one in the list is tried if the previous one fails. The default number of retries is three, but you can customize it with "max_command_retries". 
   * ``certificate`` - The name of the certificate and path used to identify yourself to the KMIP server.
   * ``keyfile`` - The name of the key used to identify yourself to the KMIP server. It is generated together with the certificate.
   * ``truststore`` - The location and key for the truststore to present to the KMIP server.
   * ``certficate_revocation_list`` - The path to a PEM-encoded certificate revocation list (CRL) - a list of issued certificates that have been revoked before their expiration date.
   * ``priority_string`` - The KMIP TLS priority string.
   * ``username`` - The KMIP server user name.
   * ``password`` - The KMIP server password.
   * ``max_command_retries`` - The number of attempts to connect to the KMIP server before trying the next host in the list. 

#. Save the file. 
#. Drain the node with :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>`
#. Restart the scylla-server service.

.. include:: /rst_include/scylla-commands-restart-index.rst
 
When a Key is Lost
==================

It is crucial to back up all of your encryption keys in a secure way. Keep a copy of all keys in a secure location. In the event that you do lose a key, your data encrypted with that key will be unreadable.

Additional Resources
====================

* :doc:`nodetool upgradesstables </operating-scylla/nodetool-commands/upgradesstables>`
* :ref:`CREATE TABLE parameters <create-table-statement>`

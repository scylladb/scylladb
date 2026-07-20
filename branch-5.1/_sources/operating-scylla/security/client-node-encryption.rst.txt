Encryption: Data in Transit Client to Node
==========================================

Follow the procedures below to enable a client to node encryption.
Once enabled, all communication between the client and the node is transmitted over TLS/SSL.
The libraries used by Scylla for OpenSSL are FIPS 140-2 certified.

Workflow
^^^^^^^^

Each Scylla node needs to be enabled for TLS/SSL encryption separately. Repeat this procedure for each node.

#. `Configure the Node`_
#. `Validate the Clients`_

Configure the Node
^^^^^^^^^^^^^^^^^^
This procedure is to be done on **every** Scylla node, one node at a time (one by one).

.. note:: If you are working on a new cluster skip steps 1 & 2.

**Procedure**

#. Run ``nodetool drain``.

#. Stop Scylla.

   .. include:: /rst_include/scylla-commands-stop-index.rst

#. Edit ``/etc/scylla/scylla.yaml`` to modify the ``client_encryption_options``.

   Available options are:

   * ``enabled`` (default - false)
   * ``certificate`` - A PEM format certificate, either self-signed, or provided by a certificate authority (CA).
   * ``keyfile`` - The corresponding PEM format key for the certificate
   * ``truststore`` - Optional path to a PEM format certificate store holding the trusted CA certificates. If not    provided, Scylla will attempt to use the system truststore to authenticate certificates.

      .. note:: If using a self-signed certificate, the "truststore" parameter needs to be set to a PEM format container with the private authority.

   * ``certficate_revocation_list`` - The path to a PEM-encoded certificate revocation list (CRL) - a list of issued certificates that have been revoked before their expiration date.

   For example:
   
   .. code-block:: yaml

      client_encryption_options:
          enabled: true
          certificate: /etc/scylla/db.crt
          keyfile: /etc/scylla/db.key
          truststore: <path to a PEM-encoded trust store> (optional)
          certficate_revocation_list: <path to a PEM-encoded CRL file> (optional)
          require_client_auth: ...
          priority_string: SECURE128:-VERS-TLS1.0:-VERS-TLS1.1

#. Start Scylla: 

   .. include:: /rst_include/scylla-commands-start-index.rst

#. To validate that encrypted connection to the node is enabled, check the logs using ``journalctl _COMM=scylla``. You should see the following ``message: storage_service - Enabling encrypted CQL connections between client and node``.


Priority String and TLSv1.2/1.3 support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
You can use the **Priority String** to control the require TLS version, Strength and more
For Priority string syntax and options see `gnutls manual <https://gnutls.org/manual/html_node/Priority-Strings.html>`_

For example, to disable TLS1.1 or lower, with minimum 128 bit security, use a prio string of `SECURE128:-VERS-TLS1.0:-VERS-TLS1.1` 
To enable 128-bit and 192-bit secure ciphers, while disabling all TLS versions except TLS 1.2 & TLS 1.3,  use a prio string of `SECURE128:+SECURE192:-VERS-ALL:+VERS-TLS1.2:+VERS-TLS1.3`


Validate the Clients
^^^^^^^^^^^^^^^^^^^^
**Before you Begin**

In order for cqlsh to work in client to node encryption SSL mode, you need to generate cqlshrc file.

For Complete instructions, see :doc:`Generate a cqlshrc File <gen-cqlsh-file>`

**Procedure**

#. Following the generation of the cqlshrc file, the following files are generated:

   - db.key
   - db.crt
   - cadb.key
   - cadb.pem

   Copy these files to your client/s, from which you run cassandra-stress.

#. To run cassandra-stress with SSL, each client running cassandra-stress needs to have a java key store file (.jks). This file can be made using the ``cadb.pem`` file and must be present on every client that runs cassandra-stress.

   * Generate the Java keystore for the node certs

     .. code-block:: yaml

        openssl pkcs12 -export -out keystore.p12 -inkey /home/scylla/server_files/db.key -in /home/scylla/server_files/db.crt -password <password>

        keytool -importkeystore -destkeystore keystore.jks -srcstoretype PKCS12 -srckeystore keystore.p12

     .. note:: Always use a password with at least 1 character with `openssl pkcs12 -export` to avoid keytool import null issue.

   * Generate the Java truststore for the trust provider

     .. code-block:: yaml

        openssl pkcs12 -export -out truststore.p12 -inkey /home/scylla/server_files/cadb.key -in /home/scylla/server_files/cadb.pem -password <password>

         keytool -importkeystore -destkeystore truststore.jks -srcstoretype PKCS12 -srckeystore truststore.p12

   * `Download`_ and install the Java security providers:

     ..  _`Download` : http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html

     Install to ``<jre>/lib/security``

     .. note:: make sure you have the latest version from this location. 

#. Run Cassandra stress with the parameters below:

   .. code-block:: yaml

      cassandra-stress write n=1000000 cl=ONE -node 10.240.0.48 -transport keystore=keystore.jks keystore-password=[password] truststore=truststore.jks truststore-password=[password] -mode native cql3 -pop -rate threads=50

   .. note:: when running cassandra-stress you may encounter an exception, if some nodes are still not in client to node SSL encrypted mode, yet the cassandra-stress will continue to run and connect only to the nodes it can.

   .. When using Scylla v1.6.x or lower you will need a dummy keystore in the default (conf/.keystore) location with password "cassandra" to run. The contents is irrelevant. Also, it only pertains to cassandra-stress. It has no impact/relation to using the normal java driver connection or cqlsh.

#. Enable encryption on the client application.

   .. include:: /operating-scylla/security/_common/ssl-hot-reload.rst

See Also
--------
* :doc:`Encryption Data in Transit Node to Node </operating-scylla/security/node-node-encryption/>`
* :doc:`Generating a self-signed Certificate Chain Using openssl </operating-scylla/security/generate-certificate/>`
* :doc:`Authorization</operating-scylla/security/authorization>`

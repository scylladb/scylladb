Encryption: Data in Transit Node to Node
========================================

Communication between all or some nodes can be encrypted. The controlling parameter is :code:`server_encryption_options`.

Once enabled, all communication between the nodes is transmitted over TLS/SSL.
The libraries used by ScyllaDB for OpenSSL are FIPS 140-2 certified.

To build a self-signed certificate chain, see :doc:`generating a self-signed certificate chain using openssl </operating-scylla/security/generate-certificate/>`.

**Procedure**

#. Configure the ``internode_encryption``, under :code:`/etc/scylla/scylla.yaml`.

   Available options are:

   * ``internode_encryption`` can be one of the following:

     * ``none`` (default) - No traffic is encrypted.
     * ``all`` - Encrypts all traffic
     * ``dc`` - Encrypts the traffic between the data centers.
     * ``rack`` - Encrypts the traffic between the racks.
     * ``transitional`` - Encrypts all outgoing traffic, but allows non-encrypted incoming. Used for upgrading cluster(s) without downtime.

   * ``certificate`` - A PEM format certificate, either self-signed, or provided by a certificate authority (CA).
   * ``keyfile`` - The corresponding PEM format key for the certificate.
   * ``truststore`` - Optional path to a PEM format certificate store of trusted CAs. If not provided, ScyllaDB will attempt to use the system trust store to authenticate certificates.
   * ``certficate_revocation_list`` - The path to a PEM-encoded certificate revocation list (CRL) - a list of issued certificates that have been revoked before their expiration date.
   * ``require_client_auth`` - Set to ``True`` to require client side authorization. ``False`` by default.
   * ``priority_string`` - Specifies session's handshake algorithms and options to use. By default there are none.
     For information on priority strings, refer to this `guide <https://gnutls.org/manual/html_node/Priority-Strings.html>`_.

   scylla.yaml example:

   .. code-block:: yaml

      server_encryption_options:
          internode_encryption: <none|rack|dc|all|transitional>
          certificate: <path to a PEM-encoded certificate file>
          keyfile: <path to a PEM-encoded key for certificate>
          truststore: <path to a PEM-encoded trust store> (optional)
          certficate_revocation_list: <path to a PEM-encoded CRL file> (optional)


#. Restart ScyllaDB node to apply the changes.

   .. include:: /rst_include/scylla-commands-restart-index.rst

.. include:: /operating-scylla/security/_common/ssl-hot-reload.rst

Moving an existing cluster to TLS without downtime
--------------------------------------------------

Upgrading an existing cluster, without TLS enabled, to use secure traffic requires a series of incremental operations:

#. For each node, add a passive TLS configuration, with internode encryption still off, and restart the node:

   .. code-block:: yaml

      ssl_storage_port: 7001
      server_encryption_options:
          internode_encryption: none
          certificate: <path to a PEM-encoded certificate file>
          keyfile: <path to a PEM-encoded key for certificate>
          truststore: <path to a PEM-encoded trust store> (optional)
          certficate_revocation_list: <path to a PEM-encoded CRL file> (optional)

   This will enable a passive TLS connector on each node. Outgoing traffic will still be unencrypted.

#. For each node, change the ``internode_encryption`` parameter to ``transitional`` and restart. This will cause all outgoing traffic to use encryption.

#. For each node, change the ``internode_encryption`` parameter to ``all``, ``rack`` or ``dc`` and restart. This will cause outgoing traffic to use encryption depending on destination, and will also enforce incoming traffic use encryption if required by the mode.

See Also
--------
* :doc:`Encryption Data in Transit Client to Node </operating-scylla/security/client-node-encryption/>`
* :doc:`Generating a self-signed Certificate Chain Using openssl </operating-scylla/security/generate-certificate/>`
* :doc:`Authorization</operating-scylla/security/authorization/>`
* :doc:`Invalid SSL Protocol Error </troubleshooting/error-messages/invalid-ssl-prot-error>`

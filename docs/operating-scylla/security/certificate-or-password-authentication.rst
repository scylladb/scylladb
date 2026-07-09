Certificate or Password Authentication
======================================

The ``CertificateOrPasswordAuthenticator`` lets a single encrypted CQL port serve
two kinds of clients at the same time:

* **Certificate clients** — clients that present a TLS client certificate.
  The role name is extracted from the certificate's subject or SAN fields using
  the same ``auth_certificate_role_queries`` rules as
  :doc:`Certificate Based Authentication </operating-scylla/security/certificate-authentication/>`.
  No username/password exchange (SASL) takes place for these clients.

* **Password clients** — clients that do *not* present a TLS client certificate.
  The server falls back to the normal SASL username/password challenge, exactly
  as with ``PasswordAuthenticator``.

This is useful when you want to migrate an existing password-authenticated cluster
to certificate authentication incrementally, or when different client types coexist
in the same deployment.

.. note::

   The server's ``client_encryption_options`` usually includes a ``truststore``
   and must set ``require_client_auth: optional``.  This puts TLS in *optional*
   mode: the server requests a client certificate during the TLS handshake but
   accepts connections that do not provide one.  The authenticator then decides
   what to do based on whether a certificate was presented.

Procedure
---------

#. Enable authentication and define the required roles as described in
   :doc:`Enable Authentication </operating-scylla/security/authentication/>`.

#. Enable CQL transport TLS with a truststore and ``require_client_auth: optional``.

   On each node, edit ``/etc/scylla/scylla.yaml``.  If you want a dedicated
   encrypted port alongside the default plain port (recommended, so that
   management tools can still connect without TLS), set all four transport ports
   explicitly:

   .. code-block:: yaml

      native_transport_port: 9042
      native_shard_aware_transport_port: 19042
      native_transport_port_ssl: 9142
      native_shard_aware_transport_port_ssl: 19142

      client_encryption_options:
        enabled: True
        certificate: <server cert>
        keyfile: <server key>
        truststore: <CA cert that signed the client certificates>
        require_client_auth: optional

   .. tip::

      Setting all four port options explicitly is only necessary when you want
      to keep a plain (non-TLS) CQL port alongside a dedicated TLS port.
      Without explicit values, enabling TLS makes all CQL ports TLS-only, which
      prevents plain drivers (such as the management framework) from connecting.
      If TLS-only CQL is acceptable, you can omit the explicit port assignments.

#. Configure the authenticator and role-extraction rules on each node:

   .. code-block:: yaml

      authenticator: com.scylladb.auth.CertificateOrPasswordAuthenticator
      auth_certificate_role_queries:
        - source: SUBJECT
          query: CN=([^,\s]+)

   The ``auth_certificate_role_queries`` rules are identical to those used by
   ``CertificateAuthenticator``.  The first matching expression is used.
   Available sources are ``SUBJECT`` (distinguished name) and ``ALTNAME``
   (subject alternative name extensions).

#. Restart each node.

How It Works
------------

On every new CQL connection:

1. If the connection arrives on a plain (non-TLS) port, or if the TLS handshake
   completed but the client did not send a certificate, the server issues a SASL
   challenge and requires a valid username/password.

2. If the client presents a TLS certificate that is trusted by the configured
   ``truststore``, the role name is extracted using ``auth_certificate_role_queries``
   and the connection is authenticated without any SASL exchange.

3. If the client presents a certificate that is **not** trusted by the
   ``truststore``, the TLS handshake fails and the connection is rejected before
   authentication begins.

Additional Resources
--------------------

* :doc:`Enable Authentication </operating-scylla/security/authentication/>`
* :doc:`Certificate Based Authentication </operating-scylla/security/certificate-authentication/>`
* :doc:`Enable Authorization </operating-scylla/security/enable-authorization/>`
* :doc:`Encryption: Data in Transit Client to Node </operating-scylla/security/client-node-encryption/>`
* :doc:`Generating a self-signed Certificate Chain Using openssl </operating-scylla/security/generate-certificate/>`

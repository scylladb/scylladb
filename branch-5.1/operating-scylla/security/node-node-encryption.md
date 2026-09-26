# Encryption: Data in Transit Node to Node

Communication between all or some nodes can be encrypted. The controlling parameter is `server_encryption_options`.

Once enabled, all communication between the nodes is transmitted over TLS/SSL.
The libraries used by Scylla for OpenSSL are FIPS 140-2 certified.

To build a self-signed certificate chain, see [generating a self-signed certificate chain using openssl](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/generate-certificate.md).

**Procedure**

1. Configure the `internode_encryption`, under `/etc/scylla/scylla.yaml`.

   Available options are:
   * `internode_encryption` can be one of the following:
     * `none` (default) - No traffic is encrypted.
     * `all` - Encrypts all traffic
     * `dc` - Encrypts the traffic between the data centers.
     * `rack` - Encrypts the traffic between the racks.
   * `certificate` - A PEM format certificate, either self-signed, or provided by a certificate authority (CA).
   * `keyfile` - The corresponding PEM format key for the certificate.
   * `truststore` - Optional path to a PEM format certificate store of trusted CAs. If not provided, Scylla will attempt to use the system trust store to authenticate certificates.
   * `certficate_revocation_list` - The path to a PEM-encoded certificate revocation list (CRL) - a list of issued certificates that have been revoked before their expiration date.
   * `require_client_auth` - Set to `True` to require client side authorization. `False` by default.
   * `priority_string` - Specifies session’s handshake algorithms and options to use. By default there are none.
     For information on priority strings, refer to this [guide](https://gnutls.org/manual/html_node/Priority-Strings.html).

   scylla.yaml example:
   ```yaml
   server_encryption_options:
       internode_encryption: <none|rack|dc|all>
       certificate: <path to a PEM-encoded certificate file>
       keyfile: <path to a PEM-encoded key for certificate>
       truststore: <path to a PEM-encoded trust store> (optional)
       certficate_revocation_list: <path to a PEM-encoded CRL file> (optional)
   ```
2. Restart Scylla node to apply the changes.

   Supported OS
   ```shell
   sudo systemctl restart scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl restart scylla
   ```

   (without restarting *some-scylla* container)

Once `internode_encryption` or `client_encryption_options` is enabled
(by being set to something other than none), the SSL / TLS certificates and key files specified in scylla.yaml
will continue to be monitored and reloaded if modified on disk.
When the files are updated, Scylla reloads them and uses them for subsequent connections.

## See Also

* [Encryption Data in Transit Client to Node](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/client-node-encryption.md)
* [Generating a self-signed Certificate Chain Using openssl](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/generate-certificate.md)
* [Authorization](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md)
* [Invalid SSL Protocol Error](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/error-messages/invalid-ssl-prot-error.md)

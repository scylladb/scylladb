# Encryption: Data in Transit Client to Node

Follow the procedures below to enable a client to node encryption.
Once enabled, all communication between the client and the node is transmitted over TLS/SSL.
The libraries used by Scylla for OpenSSL are FIPS 140-2 certified.

## Workflow

Each Scylla node needs to be enabled for TLS/SSL encryption separately. Repeat this procedure for each node.

1. [Configure the Node]()
2. [Validate the Clients]()

## Configure the Node

This procedure is to be done on **every** Scylla node, one node at a time (one by one).

#### NOTE
If you are working on a new cluster skip steps 1 & 2.

**Procedure**

1. Run `nodetool drain`.
2. Stop Scylla.

   Supported OS
   ```shell
   sudo systemctl stop scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl stop scylla
   ```

   (without stopping *some-scylla* container)
3. Edit `/etc/scylla/scylla.yaml` to modify the `client_encryption_options`.

   Available options are:
   * `enabled` (default - false)
   * `certificate` - A PEM format certificate, either self-signed, or provided by a certificate authority (CA).
   * `keyfile` - The corresponding PEM format key for the certificate
   * `truststore` - Optional path to a PEM format certificate store holding the trusted CA certificates. If not    provided, Scylla will attempt to use the system truststore to authenticate certificates.
     > #### NOTE
     > If using a self-signed certificate, the “truststore” parameter needs to be set to a PEM format container with the private authority.
   * `certficate_revocation_list` - The path to a PEM-encoded certificate revocation list (CRL) - a list of issued certificates that have been revoked before their expiration date.

   For example:
   ```yaml
   client_encryption_options:
       enabled: true
       certificate: /etc/scylla/db.crt
       keyfile: /etc/scylla/db.key
       truststore: <path to a PEM-encoded trust store> (optional)
       certficate_revocation_list: <path to a PEM-encoded CRL file> (optional)
       require_client_auth: ...
       priority_string: SECURE128:-VERS-TLS1.0:-VERS-TLS1.1
   ```
4. Start Scylla:

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)
5. To validate that encrypted connection to the node is enabled, check the logs using `journalctl _COMM=scylla`. You should see the following `message: storage_service - Enabling encrypted CQL connections between client and node`.

## Priority String and TLSv1.2/1.3 support

You can use the **Priority String** to control the require TLS version, Strength and more
For Priority string syntax and options see [gnutls manual](https://gnutls.org/manual/html_node/Priority-Strings.html)

For example, to disable TLS1.1 or lower, with minimum 128 bit security, use a prio string of SECURE128:-VERS-TLS1.0:-VERS-TLS1.1
To enable 128-bit and 192-bit secure ciphers, while disabling all TLS versions except TLS 1.2 & TLS 1.3,  use a prio string of SECURE128:+SECURE192:-VERS-ALL:+VERS-TLS1.2:+VERS-TLS1.3

## Validate the Clients

**Before you Begin**

In order for cqlsh to work in client to node encryption SSL mode, you need to generate cqlshrc file.

For Complete instructions, see [Generate a cqlshrc File](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/gen-cqlsh-file.md)

**Procedure**

1. Following the generation of the cqlshrc file, the following files are generated:
   - db.key
   - db.crt
   - cadb.key
   - cadb.pem

   Copy these files to your client/s, from which you run cassandra-stress.
2. To run cassandra-stress with SSL, each client running cassandra-stress needs to have a java key store file (.jks). This file can be made using the `cadb.pem` file and must be present on every client that runs cassandra-stress.
   * Generate the Java keystore for the node certs
     ```yaml
     openssl pkcs12 -export -out keystore.p12 -inkey /home/scylla/server_files/db.key -in /home/scylla/server_files/db.crt -password <password>

     keytool -importkeystore -destkeystore keystore.jks -srcstoretype PKCS12 -srckeystore keystore.p12
     ```

     #### NOTE
     Always use a password with at least 1 character with openssl pkcs12 -export to avoid keytool import null issue.
   * Generate the Java truststore for the trust provider
     ```yaml
     openssl pkcs12 -export -out truststore.p12 -inkey /home/scylla/server_files/cadb.key -in /home/scylla/server_files/cadb.pem -password <password>

      keytool -importkeystore -destkeystore truststore.jks -srcstoretype PKCS12 -srckeystore truststore.p12
     ```
   * [Download](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html) and install the Java security providers:

     Install to `<jre>/lib/security`

     #### NOTE
     make sure you have the latest version from this location.
3. Run Cassandra stress with the parameters below:
   ```yaml
   cassandra-stress write n=1000000 cl=ONE -node 10.240.0.48 -transport keystore=keystore.jks keystore-password=[password] truststore=truststore.jks truststore-password=[password] -mode native cql3 -pop -rate threads=50
   ```

   #### NOTE
   when running cassandra-stress you may encounter an exception, if some nodes are still not in client to node SSL encrypted mode, yet the cassandra-stress will continue to run and connect only to the nodes it can.

   <!-- When using Scylla v1.6.x or lower you will need a dummy keystore in the default (conf/.keystore) location with password "cassandra" to run. The contents is irrelevant. Also, it only pertains to cassandra-stress. It has no impact/relation to using the normal java driver connection or cqlsh. -->
4. Enable encryption on the client application.

   Once `internode_encryption` or `client_encryption_options` is enabled
   (by being set to something other than none), the SSL / TLS certificates and key files specified in scylla.yaml
   will continue to be monitored and reloaded if modified on disk.
   When the files are updated, Scylla reloads them and uses them for subsequent connections.

### See Also

* [Encryption Data in Transit Node to Node](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/node-node-encryption.md)
* [Generating a self-signed Certificate Chain Using openssl](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/generate-certificate.md)
* [Authorization](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md)

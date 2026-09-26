# Generate a cqlshrc File

Making connections to a ScyllaDB cluster that uses SSL can be a tricky process, but it doesn’t diminish the importance of properly securing your client connections with SSL. This is especially needed when you are connecting to your cluster via the Internet or an untrusted network.

## Prerequisites

Install the Java Cryptography Extensions. You can download the extensions from [Oracle](https://www.oracle.com/). The extension must match your installed Java version. Once downloaded, extract the contents of the archive to the `lib/security` subdirectory of your JRE installation directory `/usr/lib/jvm/java-8-oracle/jre/lib/security/14`.

## Procedure

1. Create a new cqlsh configuration file at `~/.cassandra/cqlshrc`, using the template below.
   ```console
   [authentication]
   username = myusername
   password = mypassword
   [cql]
   version = 3.3.1
   [connection]
   hostname = 127.0.0.1
   port = 9042
   factory = cqlshlib.ssl.ssl_transport_factory
   [ssl]
   certfile = path/to/rootca.crt
   validate = true
   userkey = client_key.key
   usercert = client_cert.crt_signed
   ```

   The `[ssl]` section of the above template applies to a CA signed certificate. If you are using
   [a self-signed certificate](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/generate-certificate.md), the `[ssl]` section will
   resemble the following:
   ```console
   [ssl]
   certfile = /etc/scylla/db.crt
   validate = true
   userkey = /etc/scylla/db.key
   usercert = /etc/scylla/db.crt
   ```

   #### NOTE
   * If `validate = true`, the certificate name must match the machine’s hostname.
   * If using client authentication (`require_client_auth = true` in `cassandra.yaml`), you also need to point to
     your userkey and usercert. SSL client authentication is only supported via cqlsh on C\* 2.1 and later.
2. Change the following parameters:

   | Parameter name   | Description                                                                         | Acceptable Values / Notes                                                |
   |------------------|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
   | username         | Your username                                                                       | This requires password authentication to be set and roles to be created. |
   | password         | The password that is associated with the username you specified.                    | This requires password authentication to be set and roles to be created. |
   | version          | CQL version that the cluster you are connecting to is using                         | If you are not sure run `nodetool version`                               |
   | certfile         | Root certificate that was used to sign file specified with the `usercert` parameter | Applies to CA signed certificates                                        |
   | userkey          | Key certificate used for `cqlsh`                                                    |                                                                          |
   | usercert         | Signed security certificate to use when connecting to a node using `cqlsh`          |                                                                          |
3. Save your changes. Connect to the node using `cqlsh --ssl`. If the configuration settings were saved correctly, you will be able to connect.
4. Run Cassandra Stress to generate required files and to connect to the SSL cluster. Supply the URL of the SSH node, and the path to your certificates. In addition supply the credentials associated with the certificate. The truststore file is the Java keystore containing the cluster’s SSL certificates. For example:
   ```none
   $> cassandra-stress write -node 127.0.0.1 -transport truststore=/path/to/cluster/truststore.jks truststore-password=mytruststorepassword -mode native cql3 user=username password=mypassword
   ```

   Cassandra stress will generate some files, you will need these to configure client - node encryption in-transit.

## Additional Topics

* [Encryption: Data in Transit Client to Node](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/client-node-encryption.md)

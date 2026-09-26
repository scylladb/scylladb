# Invalid SSL Protocol

Trying to connect cqlsh with Scylla 3.x results in **TLSv1.2 is not a valid SSL protocol** error.
Recent Scylla versions did not allow the use of the TLSv1 protocol and yet cqlsh seems to use it by default.
The solution is to upgrade to a more recent version of Scylla which contains the patch to fix the issue.
If this is not an option, change the cqlshrc file to contain the following:

```yaml
[ssl]
certfile=<path to cert file>
version=TLSv1_2
```

Alternatively, you can set the SSL_VERSION environment variable in the Linux shell. The proper way to do that is via the `export` command. If you prefer, you can apply the environment variable to a single run of cqlsh only, using SSL_VERSION=TLSv1_2 cqlsh [args]

## See Also

* [Encryption: Data in Transit Node to Node](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/node-node-encryption.md)
* [Encryption Data in Transit Client to Node](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/client-node-encryption.md)
* [Generating a self-signed Certificate Chain Using openssl](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/generate-certificate.md)
* [Authorization](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md)

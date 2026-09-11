Generating a self-signed Certificate Chain Using openssl
========================================================
For local communication, self-signed certificates and a private trust store are usually sufficient for securing communication. Indeed, several nodes can share the same certificate, as long as we ensure that our trust configuration is not tampered with.

To build a self-signed certificate chain, begin by creating a certificate configuration file like this:

.. code-block:: cfg

   [ req ]
   default_bits = 4096
   default_keyfile = <hostname>.key
   distinguished_name = req_distinguished_name
   req_extensions = v3_req
   prompt = no
   [ req_distinguished_name ]
   C = <country code>
   ST = <state>
   L = <locality/city>
   O = <domain>
   OU = <organization, usually domain>
   CN= <hostname>.<domain>
   emailAddress = <email>
   [v3_ca]
   subjectKeyIdentifier=hash
   authorityKeyIdentifier=keyid:always,issuer:always
   basicConstraints = CA:true
   [v3_req]
   # Extensions to add to a certificate request
   basicConstraints = CA:FALSE
   keyUsage = nonRepudiation, digitalSignature, keyEncipherment

Substitute the values in <> with whichever suits your organization. For this example, let’s call our host :code:`db`, and our domain :code:`foo.bar`, and create a file called :code:`db.cfg`:

.. code-block:: cfg

   [ req ]
   default_bits = 4096
   default_keyfile = db.key
   distinguished_name = req_distinguished_name
   req_extensions = v3_req
   prompt = no
   [ req_distinguished_name ]
   C = SE
   ST = Stockholm
   L = Stockholm
   O = foo.bar
   OU = foo.bar
   CN= db.foo.bar
   emailAddress = postmaster@foo.bar
   [v3_ca]
   subjectKeyIdentifier=hash
   authorityKeyIdentifier=keyid:always,issuer:always
   basicConstraints = CA:true
   [v3_req]
   # Extensions to add to a certificate request
   basicConstraints = CA:FALSE
   keyUsage = nonRepudiation, digitalSignature, keyEncipherment

.. note:: Please note that each new signed certificate should have different "CN=" in "req_distinguished_name" section. Otherwise it won't pass `openssl verify` check.

Then, begin by generating a self-signing certificate authority key:

.. code-block:: shell

   openssl genrsa -out cadb.key 4096

And using this, a certificate signing authority:

.. code-block:: shell

   openssl req -x509 -new -nodes -key cadb.key -days 3650 -config db.cfg -out cadb.pem

Now, generate a private key for our certificate:

.. code-block:: shell

   openssl genrsa -out db.key 4096

And from this, a signing request:

.. code-block:: shell

   openssl req -new -key db.key -out db.csr -config db.cfg

Then we can finally create and sign our certificate:

.. code-block:: shell

   openssl x509 -req -in db.csr -CA cadb.pem -CAkey cadb.key -CAcreateserial  -out db.crt -days 365 -sha256

As a result, we should now have:

* :code:`db.key` - PEM format key that will be used by the database node.
* :code:`db.crt` - PEM format certificate for the `db.key` signed by the `cadb.pem` and used by database node.
* :code:`cadb.pem` - PEM format signing identity that can be used as a trust store. Use it to sign client certificates that will connect to the database nodes.

Place the files in a directory of your choice and make sure you set permissions so your Scylla instance can read them. Then update the server/client configuration to reference them.

When restarting Scylla with the new configuration, you should see the following messages in the log:

When node-to-node encryption is active:

.. code-block:: console

   Starting Encrypted Messaging Service on SSL port 7001

When client to node encryption is active:

.. code-block:: console

   Enabling encrypted CQL connections between client and server

See Also
--------

* :doc:`Encryption: Encryption: Data in Transit Node to Node </operating-scylla/security/node-node-encryption/>`
* :doc:`Authorization</operating-scylla/security/authorization/>`

.. include:: /rst_include/apache-copyrights.rst

Tests for Alternator that should also pass, identically, against DynamoDB.

Tests use the boto3 library for AWS API, and the pytest frameworks
(both are available from Linux distributions, or with "pip install").

To run all tests against the local installation of Alternator on
http://localhost:8000, just run `pytest`.

Some additional pytest options:
* To run all tests in a single file, do `pytest test_table.py`.
* To run a single specific test, do `pytest test_table.py::test_create_table_unsupported_names`.
* Additional useful pytest options, especially useful for debugging tests:
  * -v: show the names of each individual test running instead of just dots.
  * -s: show the full output of running tests (by default, pytest captures the test's output and only displays it if a test fails)

Add the `--aws` option to test against AWS instead of the local installation.
For example - `pytest --aws test_item.py` or `pytest --aws`.

If you plan to run tests against AWS and not just a local Scylla installation,
the files ~/.aws/credentials should be configured with your AWS key:

```
[default]
aws_access_key_id = XXXXXXXXXXXXXXXXXXXX
aws_secret_access_key = xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

and ~/.aws/config with the default region to use in the test:
```
[default]
region = us-east-1
```

## HTTPS support

In order to run tests with HTTPS, run pytest with `--https` parameter. Note that the Scylla cluster needs to be provided
with alternator\_https\_port configuration option in order to initialize a HTTPS server.
Moreover, running an instance of a HTTPS server requires a certificate. Here's how to easily generate
a key and a self-signed certificate, which is sufficient to run `--https` tests:

```
openssl genrsa 2048 > scylla.key
openssl req -new -x509 -nodes -sha256 -days 365 -key scylla.key -out scylla.crt
```

If this pair is put into `conf/` directory, it will be enough
to allow the alternator HTTPS server to think it's been authorized and properly certified.
Still, boto3 library issues warnings that the certificate used for communication is self-signed,
and thus should not be trusted. For the sake of running local tests this warning is explicitly ignored.


## Authorization

By default, boto3 prepares a properly signed Authorization header with every request.
In order to confirm the authorization, the server recomputes the signature by using
user credentials (user-provided username + a secret key known by the server),
and then checks if it matches the signature from the header.
Early alternator code did not verify signatures at all, which is also allowed by the protocol.
A partial implementation of the authorization verification can be allowed by providing a Scylla
configuration parameter:
```yaml
  alternator_enforce_authorization: true
```
The implementation is currently coupled with Scylla's system\_auth.roles table,
which means that an additional step needs to be performed when setting up Scylla
as the test environment. Tests will use the following credentials:
Username: `alternator`
Secret key: `secret_pass`

With CQLSH, it can be achieved by executing this snipped:

```bash
cqlsh -x "INSERT INTO system_auth.roles (role, salted_hash) VALUES ('alternator', 'secret_pass')"
```

Most tests expect the authorization to succeed, so they will pass even with `alternator_enforce_authorization`
turned off. However, test cases from `test_authorization.py` may require this option to be turned on,
so it's advised.

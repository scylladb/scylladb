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


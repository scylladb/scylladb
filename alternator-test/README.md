Tests for Alternator that should also pass, identically, against DynamoDB.

Tests use the boto3 library for AWS API, and the pytest frameworks
(both are available from Linux distributions, or with "pip install").

To run tests against AWS and not just a local Scylla installation,
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

* Running "pytest" runs all tests.
* To run all tests in a single file, do `pytest test_table.py`.
* To run a single specific test, do `pytest test_table.py::test_create_table_unsupported_names`.
* By default test run against a local Scylla installation at
  http://localhost:8000. Add the "--aws" option to test against AWS instead
  (the us-east-1 region).
  For example - `pytest --aws test_item.py` or `pytest --aws`.
* Additional useful pytest options, especially useful for debugging tests:
  * -v: show the names of each individual test running instead of just dots.
  * -s: show the full output of running tests (by default, pytest captures the test's output and only displays it if a test fails)

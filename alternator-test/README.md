Tests for Alternator that should also pass, identically, against DynamoDB.

Tests use the boto3 library for AWS API, and the pytest frameworks
(both are available from Linux distributions, or with "pip install".

The files ~/.aws/credentials should be configure with your AWS key:

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

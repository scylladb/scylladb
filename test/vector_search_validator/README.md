# `vector-search-validator` tests for Scylla and Vector Store

`vector-search-validator` is a testing tool for validating the functionality of
integration between Scylla and Vector Store. Such integration depends on the
Scylla cluster and the Vector Store nodes and also on the DNS service. For this
reason we run `vector-search-validator` in a network and storage linux
namespace to separate it from the host environment. `vector-search-validator`
contains DNS server and all tests in one binary. It uses external scylla and
vector-store binaries.

The `test_validator.py::test_validator[test-case]` is the entry point for
running the tests. It is parametrized with name of the test case.  Available
test cases are taken dynamically from the `vector-search-validator` binary.

To run test with `dev` Scylla and test case `cql` run the following command in
the dbuild environment (non dbuild environment is not supported currently, as
test needs to have `sudo` permissions without password):

```bash
$ pytest --mode=dev test/vector_search_validator/test_validator.py::test_validator[cql]
```

To run all tests with `dev` Scylla run the following command:

```bash
$ pytest --mode=dev test/vector_search_validator/test_validator.py
$ pytest --mode=dev test/vector_search_validator/test_validator.py::test_validator
```

You can tests with custom filters supported by validator. To run filtered tests
with `dev` Scylla run the following command:

```bash
$ pytest --mode=dev test/vector_search_validator/test_validator.py --filters filter1,filter2,...
```

Logs are stored in
`testlog/{mode}/vector_search_validator/{test-case}-{run_id}/` directory.

Implementing new test cases on the Scylla repository side means adding new test
in crate `crates/validator-scylla`.


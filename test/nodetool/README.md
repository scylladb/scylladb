# Front-end tests for nodetool

The tests in this directory exercise the nodetool client itself, mocking the API backend.
This allows for testing all combinations of all supported options, and still keeping the tests quick.

The tests can be run against both the scylla-native nodetool (default), or the inherited, C\*-based nodetool.

Run all tests against the scylla-native nodetool:
```
pytest --nodetool=scylla .
```

You can specify the path to the scylla binary with the `--nodetool-path` option. By default the tests will pick up the ScyllaDB executable, that is appropriate for the `--mode` option (defaults to `dev`).

Run all tests against the C* nodetool:
```
pytest --nodetool=cassandra .
```

Again, you can specify the path to the nodetool binary with `--nodetool-path` option. By default, `<scylladb.git>/tools/java/bin/nodetool` will be used.
When running the test against the java-nodeotol, you can specify the path to JMX with `--jmx-path` option. By default, `<scylladb.git>/tools/jmx/scripts/scylla-jmx` will be used.

If you add new tests, make sure to run all tess against both nodetool implementations, to avoid regressions. Note that CI/promotion will only run the tests against the scylla-native nodetool.

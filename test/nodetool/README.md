# Front-end tests for nodetool

The tests in this directory exercise the nodetool client itself, mocking the API backend.
This allows for testing all combinations of all supported options, and still keeping the tests quick.

Run all tests:
```
pytest .
```

By default the tests will pick up the ScyllaDB executable, that is appropriate for the `--mode` option (defaults to `dev`).

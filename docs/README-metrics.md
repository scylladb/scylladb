# ScyllaDB metrics docs scripts

The following files extracts metrics from C++ source files and generates documentation:

- **`scripts/get_description.py`** - Metrics parser and extractor
- **`scripts/metrics-config.yml`** - Configuration for special cases only
- **`docs/_ext/scylladb_metrics.py`** - Sphinx extension for rendering

## Configuration

The system automatically handles most metrics extraction. You only need configuration in the `metrics-config.yml` file for:

**Complex parameter combinations:**
```yaml
"cdc/log.cc":
  params:
    part_name;suffix: [["static_row", "total"], ["clustering_row", "failed"]]
    kind: ["total", "failed"]
```

**Multiple parameter values:**
```yaml
"service/storage_proxy.cc":
  params:
    _short_description_prefix: ["total_write_attempts", "write_errors"]
```

**Complex expressions:**
```yaml
"tracing/tracing.cc":
  params:
    "max_pending_trace_records + write_event_records_threshold": "max_pending_trace_records + write_event_records_threshold"
```

**Group assignments:**
```yaml
"cql3/query_processor.cc":
  groups:
    "80": query_processor
```

A key is a line number, and the group takes effect from the start of that line
onwards. The key therefore has to sit after anything that would overwrite it and
before the first metric that needs it:

- Where `add_group()` is followed by its metrics, use the line `add_group()` is
  on. A key naming the line before it would be overwritten when that line is
  parsed. This is the number the validation error reports.
- Where the metrics are built into a vector and passed to `add_group()` further
  down, the parser reaches them before it ever sees the group, so the key has to
  name a line above the first metric. The `query_processor` entry above is this
  shape: line 80 sits well before the first metric on line 115.

**Skip files:**
```yaml
"seastar/tests/unit/metrics_test.cc": skip
```

## Validation

Use the built-in validation to check all metrics files:

```bash
# Validate all metrics files
python scripts/get_description.py --validate -c scripts/metrics-config.yml

# Validate with verbose output
python scripts/get_description.py --validate -c scripts/metrics-config.yml -v
```

The GitHub workflow `docs-validate-metrics.yml` automatically runs validation on PRs to `master` that modify `.cc` files or metrics configuration.

## Common fixes

- **"Parameter not found"**: Add parameter mapping to config `params` section
- **"Could not resolve param"**: Check parameter name matches C++ code exactly
- **"No group found"**: Add group mapping or verify `add_group()` calls
- **"Unresolved metric name"**: The name still contains characters from the
  originating C++ expression, usually because `add_group()` was called with a
  computed value rather than a literal (e.g. `add_group("prefix"s + n, ...)`).
  Map that line number to the group name(s) the code produces at runtime in the
  `groups` section, listing each one if the call sits inside a loop.

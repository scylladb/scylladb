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

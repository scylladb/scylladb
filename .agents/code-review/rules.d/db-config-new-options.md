# New db::config options

Applies to: db/config.hh, db/config.cc

New tunables should be added as native cluster config options, not to
db::config. If the diff adds a new configuration option (a new
`named_value<...>` member in db/config.hh, or its `(this, "<name>", ...)`
initializer in db/config.cc), say so once, at the declaration:

- New options should be added as native cluster config options
  (db/cluster_config_registry.cc; see "Adding a Native Option" in
  docs/dev/cluster_config.md) rather than to db::config.
- db::config remains appropriate only when the value is needed before the
  node joins the cluster (addresses, ports, seeds, directories, memory/IO
  sizing, TLS/auth bootstrap, anything read at startup), is inherently
  node-local, or is used by tools/tests running without a cluster.
- Ask the author to state the reason in the PR description. If the
  description already gives one, don't comment.

Don't flag renamed, removed or deprecated options, changed defaults or
descriptions, or value_status changes.

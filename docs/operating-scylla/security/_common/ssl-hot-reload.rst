
Once ``internode_encryption`` or ``client_encryption_options`` is enabled
(by being set to something other than none), the SSL / TLS certificates and key files specified in scylla.yaml
will continue to be monitored and reloaded if modified on disk.
When the files are updated, ScyllaDB reloads them and uses them for subsequent connections.

# Keeping sstables on S3

## Endpoints config file

First one need to describe endpoints where sstables can be stored. This is done
in a yaml file with the following format:

```
endpoints:
  - name: $endpoint_address_or_domain_name
    port: $port_number
    https: optional True or False
    aws_region: optional region name, e.g. us-east-1
    aws_key: optional AWS key value
    aws_secret: optional AWS secret value
```

The last three items must be all present or all absent. When set the values are
used by the S3 client to sign requests. If not set requests are sent unsigned
which may not always accepted by the server.

By default Scylla tries to read it from the `object_storage.yaml` file
located in the same directory with the `scylla.yaml`. Optionally, the
`--object-storage-config-file $path` option can be specified.

## Enabling the feature

Currently the object-storage backend works if `keyspace-storage-options` feature
(experimental) is ON. Use `--experimental-features=keyspace-storage-options` to
enable it.

## Creating keyspace

Sstables location is keyspace-scoped. In order to create a keyspace with S3
storage use `CREATE KEYSPACE` with `STORAGE = { 'type': 'S3', 'endpoint': '$endpoint_name', 'bucket': '$bucket' }`
parameters.

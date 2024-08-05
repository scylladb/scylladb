# Introduction

Scylla has the ability to communicate directly with S3-compatible storage. This
ability can be used in several ways, and to enable those features one first needs
to configure scylla with the endpoints

## Endpoints config file

First one need to describe endpoints where sstables can be stored. This is done
in a yaml file with the following format:

```yaml
endpoints:
  - name: $endpoint_address_or_domain_name
    port: $port_number
    https: optional True or False
    aws_region: optional region name, e.g. us-east-1
    aws_access_key_id: optional AWS access key ID
    aws_secret_access_key: optional AWS secret access key
    aws_session_token: optional AWS session token
```

The `aws_...` options can be configured via environment variables, the variables
names are

* AWS_DEFAULT_REGION
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* AWS_SESSION_TOKEN

Those parameters must be all present or all absent. When set the values are
used by the S3 client to sign requests. If not set requests are sent unsigned
which may not always accepted by the server.

By default Scylla tries to read it from the `object_storage.yaml` file
located in the same directory with the `scylla.yaml`. Optionally, the
`--object-storage-config-file $path` option can be specified.

# Keeping sstables on S3

On of the ways to use object storage is to keep sstables directly on it as objects.

## Enabling the feature

Currently the object-storage backend works if `keyspace-storage-options` is listed
in `experimental_features` in `scylla.yaml`. like:

```yaml
experimental_features:
  - keyspace-storage-options
```

It can also be enabled with `--experimental-features=keyspace-storage-options`
command line option when launchgin scylla.

## Creating keyspace

Sstables location is keyspace-scoped. In order to create a keyspace with S3
storage use `CREATE KEYSPACE` with `STORAGE = { 'type': 'S3', 'endpoint': '$endpoint_name', 'bucket': '$bucket' }`
parameters, where `$endpoint_name` should match with the corresponding `name`
of the configured endpoint in the YAML file above.

In the following example, an endpoint named "s3.us-east-2.amazonaws.com" is
defined in `object_storage.yaml`, and this endpoint is used when creating the
keyspace "ks".

in `object_storage.yaml`:

```yaml
endpoints:
  - name: s3.us-east-2.amazonaws.com
    port: 443
    https: true
    aws_region: us-east-2
    aws_access_key_id: EXAMPLE_ACCESS_KEY_ID
    aws_secret_access_key: EXAMPLE_SECRET_ACCESS_KEY
```

and when creating the keyspace:

```cql
CREATE KEYSPACE ks
  WITH REPLICATION = {
   'class' : 'NetworkTopologyStrategy',
   'replication_factor' : 1
  }
  AND STORAGE = {
   'type' : 'S3',
   'endpoint' : 's3.us-east-2.amazonaws.com',
   'bucket' : 'bucket-for-testing'
  };
```

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

## Configuring AWS S3 access

You can define endpoint details in the `scylla.yaml` file. For example:
```yaml
object_storage_endpoints:
  - name: s3.us-east-1.amazonaws.com
    port: 443
    https: true
    aws_region: us-east-1
```

### Local/Development Environment

In a local or development environment, you usually need to set authentication tokens in environment variables to ensure the client works properly. For instance:
```sh
export AWS_ACCESS_KEY_ID=EXAMPLE_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=EXAMPLE_SECRET_ACCESS_KEY
```

Additionally, you may include an `aws_session_token`, although this is not typically necessary for local or development environments:

```sh
export AWS_ACCESS_KEY_ID=EXAMPLE_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=EXAMPLE_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN=EXAMPLE_TEMPORARY_SESSION_TOKEN
```
### Important Note

The examples above are intended for development or local environments. You should *never* use this approach in production. The Scylla S3 client will first attempt to access credentials from environment variables. If it fails to obtain credentials, it will then try to retrieve them from the AWS Security Token Service (STS) or the EC2 Instance Metadata Service.

For the EC2 Instance Metadata Service to function correctly, no additional configuration is required. However, STS requires the IAM Role ARN to be defined in the `scylla.yaml` file, as shown below:
```yaml
object_storage_endpoints:
  - name: s3.us-east-1.amazonaws.com
    port: 443
    https: true
    aws_region: us-east-1
    iam_role_arn: arn:aws:iam::123456789012:instance-profile/my-instance-instance-profile
```

## Creating keyspace

Sstables location is keyspace-scoped. In order to create a keyspace with S3
storage use `CREATE KEYSPACE` with `STORAGE = { 'type': 'S3', 'endpoint': '$endpoint_name', 'bucket': '$bucket' }`
parameters, where `$endpoint_name` should match with the corresponding `name`
of the configured endpoint in the YAML file above.

In the following example, an endpoint named "s3.us-east-2.amazonaws.com" is
defined in `scylla.yaml`, and this endpoint is used when creating the
keyspace "ks".

in `scylla.yaml`:

```yaml
object_storage_endpoints:
  - name: s3.us-east-2.amazonaws.com
    port: 443
    https: true
    aws_region: us-east-2
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

# Copying sstables on S3 (backup)

It's possible to upload sstables from data/ directory on S3 via API. This is good
to do because in that case all the resources that are needed for that operation (like
disk IO bandwidth and IOPS, CPU time, networking bandwidth) will be under Seastar's
control and regular Scylla workload will not be randomly affected.

The API endpoint name is `/storage_service/backup` and its Swagger description can be
found [here](./api/api-doc/storage_service.json). Accepted parameters are

* *keyspace*: the keyspace to copy sstables from
* *table*: the table to copy sstables from
* *snapshot*: the snapshot name to copy sstables from
* *endpoint*: the key in the object storage configuration file
* *bucket*: bucket name to put sstables' files in
* *prefix*: prefix to put sstables' files under

Currently only snapshot backup is possible, so first one needs to take [snapshot](docs/kb/snapshots.rst)

All tables in a keyspace are uploaded, the destination object names will look like
`s3://bucket/some/prefix/to/store/data/.../sstable`

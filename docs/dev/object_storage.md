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

# Manipulating S3 data

This section intends to give an overview of where, when and how we store data in S3 and provide a quick set of commands  
which help gain local access to the data in case there is a need for manual intervention.

Most of the time it won't be necessary to touch the data on S3 directly, there are transparent REST APIs and Scylla Manager  
commands for backup and restore and Scylla can operate normally with S3 storage configured in the  
`CREATE KEYSPACE` cql documented at [ScyllaDB CQL Extensions | ScyllaDB Docs](https://opensource.docs.scylladb.com/stable/cql/cql-extensions.html#keyspace-storage-options).  

However, if for some reason the SSTables become corrupted and need an offline scrub before re-uploading  
or if a bug investigation leads to the need to analyze the backup data, follow the information below to access  
that data.  

Issue tracking the document [here](https://github.com/scylladb/scylladb/issues/22438).

## Object Storage Layout

There are currently three mechanisms in Scylla which write data to S3:

1. Scylla Manager backup

When performing a backup with `sctool`, a `backup` prefix is created within the bucket passed as argument and  
under that prefix, Scylla Manager stores all the backup data of all the backup tasks organized by cluster name,  
datacenter, keyspace, etc.

Follow [Specification | ScyllaDB Docs](https://manager.docs.scylladb.com/branch-3.3/backup/specification) in the Scylla Manager documentation for the exact layout  
under the `backup` prefix.

2. `/storage_service/backup` REST API

When using the `/storage_service/backup` REST API, the data is stored under the prefix passed as argument to the API.  
The structure under this prefix is identical to what you’d find in the typical Scylla snapshot.  
There is a manifest file which contains the list of Data files for each SSTable, the schema file and all the SSTables  
components stored flat under the prefix.
```perl
scylla-bucket/prefix/
│
├── manifest.json
├── schema.cql
|
├── me-3gqe_1lnj_4sbpc2ezoscu9hhtor-big-Data.db
├── me-3gqe_1lnj_4sbpc2ezoscu9hhtor-big-Index.db
├── me-3gqe_1lnj_4sbpc2ezoscu9hhtor-big-Summary.db
├── ...
│
├── ma-1abx_k29m_9fyug3sdtjwj8krpqh-big-Data.db
├── ma-1abx_k29m_9fyug3sdtjwj8krpqh-big-Index.db
├── ma-1abx_k29m_9fyug3sdtjwj8krpqh-big-Summary.db
├── ...
│
└── ... (more SSTable components)
```
See the API [documentation](#copying-sstables-on-s3-backup) for more details about the actual backup request.

3. `CREATE KEYSPACE` with S3 storage

When creating a keyspace with S3 storage, the data is stored under the bucket passed as argument to the `CREATE KEYSPACE` statement.  
Once the statement is issued, Scylla will use transparently the S3 bucket as the location of the SSTables for that keyspace.  
Like in the case above, there is no hierarchy for the data, *all SSTables components are stored flat within the bucket*.
```perl
scylla-sstables-bucket/
│
├── 3gqe_1lnj_4sbpc2ezoscu9hhtor/
│   ├── Data.db
│   ├── Index.db
│   ├── Summary.db
│   └── ...
│
├── 1abx_k29m_9fyug3sdtjwj8krpqh/
│   ├── Data.db
│   ├── Index.db
│   ├── Summary.db
│   └── ...
│
└── ... (other SSTable folders)
```

## Downloading, deleting, uploading SSTables

To manually manage sstables on S3, AWS CLI commands can be used, but first it's mandatory to have awscli  
installed ([installation guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)) and have the proper credentials set up in order to be able to access ScyllaDB S3 buckets.

Please make sure your `~/.aws/credentials` file points to a valid set of S3 credentials.  
Either refresh credentials if you use an OKTA-based fetching tool or make sure they point to a valid IAM user with S3 access.

Provided all the prerequisites above are fulfilled and you're able to run
```sh
aws s3 ls s3://your-bucket/
```
and see something (or at least not see an error if the bucket is empty), you're all set for the next commands.
> **NOTE:**
> Please refer to the sections above for the prefix layout of each S3 use case.

### Downloading SSTables

Fetching the SSTables of your backup can be easily done by  
e.g. copying each individual component
```sh
aws s3 cp s3://your-bucket/path/to/sstable/me-3gqb_1izi_0pxn421yzymfw5c8zf-big-Data.db  /local/path/to/sstable/component
```
or downloading an entire sstable using globs
```sh
aws s3 cp s3://your-bucket/path-to-sstables/ /local/path/for/sstables --exclude "*" --include 'some-sstable-generation-big-*' --recursive
```
### Deleting SSTables

components individually
```sh
aws s3 rm s3://your-bucket/path/to/sstable/me-3gqb_1izi_0pxn421yzymfw5c8zf-big-Data.db
```
or the entire SSTable using globs
```sh
aws s3 rm s3://your-bucket/path-to-sstables/ --exclude "*" --include 'some-sstable-generation-big-*' --recursive
```
### Uploading SSTables
components individually
```sh
 aws s3 cp /local/path/to/sstable/me-3gqb_1izi_0pxn421yzymfw5c8zf-big-Data.db s3://your-bucket/path/to/sstable/component
 ```
or the entire SSTable using globs
```sh
aws s3 cp /local/path/for/sstables s3://your-bucket/path-to-sstables/ --exclude "*" --include 'some-sstable-generation-big-*' --recursive
```

## Metadata touchups

In case of Scylla Manager backups, if manual scrubbing is needed and SSTables will be re-uploaded,  
multiple things would need to be changed, same thing if you need to drop some SSTables altogether.  
As you might’ve seen in the Scylla Manager [Specification Docs](https://manager.docs.scylladb.com/branch-3.3/backup/specification), we keep a JSON manifest per node  
and that manifest file contains lots of SSTable-dependent information:

* list of SSTables per table owned by node
* total size of SSTables in the chunk of table owned
* total size of all chunks of tables owned
* the list of tokens owned by the node

As the name of the fields suggests, all the information in the list above depends on the SSTables content, so any attempt  
to fix locally a corrupt SSTable and re-upload, most probably will force you to update them in the manifest file of the node.  
There is high likelihood that a scrubbed SSTable results in different values for all the fields specified above.

For the `storage_service/backup` REST API, in theory only removing an entire SSTable from the backup would require changing  
the manifest file and remove the corresponding entry for the SSTable, in all other cases, no metadata changes needed.

For the `CREATE KEYSPACE` on S3, there is no need to update any metadata as we currently don’t have any.

> **NOTE:**
> It’s obvious to say that re-uploading a scrubbed SSTable means re-uploading all its components as it’s likely most of them were changed.

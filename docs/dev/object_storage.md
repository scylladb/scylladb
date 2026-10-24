# Keeping sstables on S3/GS

On of the ways to use object storage is to keep sstables directly on it as objects.

## Enabling the feature

Object-storage backed keyspaces are supported out of the box; no experimental
feature flag is required. Simply configure the object-storage endpoints in
`scylla.yaml` (see below) and create a keyspace with the desired storage
options (see `CREATE KEYSPACE` extensions below).

> **Status.** Object-storage keyspaces are supported for tablets only and target
> archival workloads. A number of capabilities are not available for them -
> notably snapshots, backup and restore - and some additional configuration is
> required (`query_page_size_in_bytes`, raised timeouts). See
> [Object storage keyspaces: status and limitations](../operating-scylla/admin.rst)
> in the user documentation for the authoritative list. Keep that list and this
> document in sync when the supported set changes.

## Configuring AWS S3 access

You can define endpoint details in the `scylla.yaml` file. For example:
```yaml
object_storage_endpoints:
  - name: https://s3.us-east-1.amazonaws.com:443
    aws_region: us-east-1
```

## Configuring GCP storage access

Similarly to AWS, define endpoint details in `scylla.yaml` like:
```yaml
object_storage_endpoints:
  - name: https://storage.googleapis.com
    type: gs
    credentials_file: <gcp account credentials json file>
```

Typically, google compute storage only uses the same endpoint URI (unless using private proxy or mock
server), so `name` can also use the `default` moniker.

`credentials_file` can be omitted, in which case the default credentials on the machine
will be used, i.e. resolving the current users credentials or fallback to machine credentials
if running on a GCP instance.

If set, the environment variable `GOOGLE_APPLICATION_CREDENTIALS` can be set to point to a 
credentials file. 

If no credentials file is set, the default credentials will be searched, i.e. `application_default_credentials.json`
in the gcp local data folder.

You can also set the `credentials_file` to `none` to completely skip authentication. Useful for testing
on mock servers.


### Local/Development Environment

In a local or development environment, you usually need to set AWS authentication tokens in environment variables to ensure the client works properly. For instance:
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

For gs, when using local mock server, authentication is normally not used.

### Important Note

The examples above are intended for development or local environments. You should *never* use this approach in production. The Scylla S3 client will first attempt to access credentials from environment variables. If it fails to obtain credentials, it will then try to retrieve them from the AWS Security Token Service (STS) or the EC2 Instance Metadata Service.

For the EC2 Instance Metadata Service to function correctly, no additional configuration is required. However, STS requires the IAM Role ARN to be defined in the `scylla.yaml` file, as shown below:
```yaml
object_storage_endpoints:
  - name: https://s3.us-east-1.amazonaws.com:443
    aws_region: us-east-1
    iam_role_arn: arn:aws:iam::123456789012:role/my-scylla-role
```

## Creating keyspace with S3

Sstables location is keyspace-scoped. In order to create a keyspace with S3
storage use `CREATE KEYSPACE` with `STORAGE = { 'type': 'S3', 'endpoint': '$endpoint_name', 'bucket': '$bucket' }`
parameters, where `$endpoint_name` should match with the corresponding `name`
of the configured endpoint in the YAML file above.

In the following example an endpoint is defined in `scylla.yaml` and then used
when creating the keyspace "ks". Note that the `endpoint` value in `CREATE KEYSPACE`
is the endpoint `name` copied verbatim: the lookup is an exact string match, so
`https://s3.us-east-2.amazonaws.com` and `https://s3.us-east-2.amazonaws.com:443`
are different endpoints.

in `scylla.yaml`:

```yaml
object_storage_endpoints:
  - name: https://s3.us-east-2.amazonaws.com:443
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
   'endpoint' : 'https://s3.us-east-2.amazonaws.com:443',
   'bucket' : 'bucket-for-testing'
  };
```


## Creating keyspace with GS

This mirrors AWS S3 config.

in `scylla.yaml`:

```yaml
object_storage_endpoints:
  - name: default
    type: gs
    credentials_file: <credentials file>|none
```

and when creating the keyspace:

```cql
CREATE KEYSPACE ks
  WITH REPLICATION = {
   'class' : 'NetworkTopologyStrategy',
   'replication_factor' : 1
  }
  AND STORAGE = {
   'type' : 'GS',
   'endpoint' : 'default',
   'bucket' : 'bucket-for-testing'
  };
```

# Copying sstables on S3/GS (backup)

It's possible to upload sstables from data/ directory on S3 via API. This is good
to do because in that case all the resources that are needed for that operation (like
disk IO bandwidth and IOPS, CPU time, networking bandwidth) will be under Seastar's
control and regular Scylla workload will not be randomly affected.

The API endpoint name is `/storage_service/backup` and its Swagger description can be
found [here](./api/api-doc/storage_service.json). Accepted parameters are

* *keyspace*: the keyspace to copy sstables from
* *table*: the table to copy sstables from
* *snapshot*: the snapshot name to copy sstables from
* *endpoint*: the key in the object storage configuration file. Can be either an AWS or GCP endpoint
* *bucket*: bucket name to put sstables' files in
* *prefix*: prefix to put sstables' files under

Currently only snapshot backup is possible, so first one needs to take [snapshot](docs/kb/snapshots.rst)

All tables in a keyspace are uploaded, the destination object names will look like
`s3://bucket/some/prefix/to/store/data/.../sstable`
or 
`gs://bucket/some/prefix/to/store/data/.../sstable`

# System tables
There are a few system tables that object storage related code needs to touch in order to operate.
* [system_distributed.snapshot_sstables](docs/dev/snapshot_sstables.md) - Used during restore by worker nodes to get the list of SSTables that need to be downloaded from object storage and restored locally.
* [system.sstables](docs/dev/system_keyspace.md#systemsstables) - Used to keep track of SSTables on object storage when a keyspace is created with object storage storage_options.

# Manipulating S3 data

This section intends to give an overview of where, when and how we store data in S3 and provide a quick set of commands  
which help gain local access to the data in case there is a need for manual intervention.

Most of the time it won't be necessary to touch the data on S3 directly, there are transparent REST APIs and Scylla Manager  
commands for backup and restore and Scylla can operate normally with S3 storage configured in the  
`CREATE KEYSPACE` cql documented at [ScyllaDB CQL Extensions](../cql/cql-extensions.md#keyspace-storage-options).  

However, if for some reason the SSTables become corrupted and need an offline scrub before re-uploading  
or if a bug investigation leads to the need to analyze the backup data, follow the information below to access  
that data.  

Issue tracking the document [here](https://github.com/scylladb/scylladb/issues/22438).

## Object Storage Layout

There are currently three mechanisms in Scylla which write data to S3/GS:

1. Scylla Manager backup

When performing a backup with `sctool`, a `backup` prefix is created within the bucket passed as argument and  
under that prefix, Scylla Manager stores all the backup data of all the backup tasks organized by cluster name,  
datacenter, keyspace, etc.

Follow [Specification | ScyllaDB Docs](https://manager.docs.scylladb.com/stable/backup/specification.html) in the Scylla Manager documentation for the exact layout  
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

### The snapshot manifest

Each table snapshot directory contains a manifest.json file that lists the contents of the snapshot and some metadata.
The json structure is as follows:
```
{
  "manifest": {
    "version": "1.0",
    "scope": "node"
  },
  "node": {
    "host_id": "<UUID>",
    "datacenter": "mydc",
    "rack": "myrack"
  },
  "snapshot": {
    "name": "snapshot name",
    "created_at": seconds_since_epoch,
    "expires_at": seconds_since_epoch | null,
  },
  "table": {
    "keyspace_name": "my_keyspace",
    "table_name": "my_table",
    "table_id": "<UUID>",
    "tablets_type": "none|powof2",
    "tablet_count": N
  },
  "sstables": [
    {
      "id": "67e35000-d8c6-11f0-9599-060de9f3bd1b",
      "toc_name": "me-3gw7_0ndy_3wlq829wcsddgwha1n-big-TOC.txt",
      "data_size": 75,
      "index_size": 8,
      "first_token": -8629266958227979430,
      "last_token": 9168982884335614769,
      "tablet_id": 0,
      "repaired_at": 0,
    },
    {
      "id": "67e35000-d8c6-11f0-85dc-0625e9f3bd1b",
      "toc_name": "me-3gw7_0ndy_3wlq821a6cqlbmxrtn-big-TOC.txt",
      "data_size": 73,
      "index_size": 8,
      "first_token": 221146791717891383,
      "last_token": 7354559975791427036,
    },
    ...
  ],
  "files": [ ... ]
}

The `manifest` member contains the following attributes:
- `version` - representing the version of the manifest itself. It is incremented when members are added or removed from the manifest.
- `scope` - the scope of metadata stored in this manifest file.  The following scopes are supported:
    - `node` - the manifest describes all SSTables owned by this node in this snapshot.

The `node` member contains metadata about this node that enables datacenter- or rack-aware restore.
- `host_id` - is the node's unique host_id (a UUID).
- `datacenter` - is the node's datacenter.
- `rack` - is the node's rack.

The `snapshot` member contains metadata about the snapshot.
- `name` - is the snapshot name (a.k.a. `tag`)
- `created_at` - is the time when the snapshot was created.
- `expires_at` - is an optional time when the snapshot expires and can be dropped, if a TTL was set for the snapshot.  If there is no TTL, `expires_at` may be omitted, set to null, or set to 0.

The `table` member contains metadata about the table being snapshot.
- `keyspace_name` and `table_name` - are self-explanatory.
- `table_id` - a universally unique identifier (UUID) of the table set when the table is created.
- `tablets_type`:
    - `none` - if the keyspace uses vnodes replication
    - `powof2` - if the keyspace uses tablets replication, and the tablet token ranges are based on powers of 2.
    - `arbitrary` - if the keyspace uses tablets replication, and the tablet token ranges and count can be arbitrary.
- `tablet_count` - Optional. If `tablets_type` is not `none`, contains the number of tablets allcated in the table. If `tablets_type` is `powof2`, tablet_count would be a power of 2.

The `sstables` member is a list containing metadata about the SSTables in the snapshot.
- `id` - is the STable's unique id (a UUID).  It is carried over with the SSTable when it's streamed as part of tablet migration, even if it gets a new generation.
- `toc_name` - is the name of the SSTable Table Of Contents (TOC) component.
- `data_size` and `index_size` - are the sizes of the SSTable's data and index components, respectively.  They can be used to estimate how much disk space is needed for restore.
- `first_token` and `last_token` - are the first and last tokens in the SSTable, respectively.  They can be used to determine if a SSTable is fully contained in a (tablet) token range to enable efficient file-based streaming of the SSTable.
- `tablet_id` - Optional. The id of the tablet that owned the SSTable when the snapshot was taken. Omitted for vnode-based tables.
- `repaired_at` - Optional. When the SSTable was last repaired, or 0 if it was never repaired.

The optional `files` member may contain a list of non-SSTable files included in the snapshot directory, not including the manifest.json file and schema.cql.
```

#### Cluster level manifest

A manifest for a cluster level snapshot backup differs from the per-node ones on a few points:

```
{
  "manifest": {
    "version": "1.0",
    "scope": "dc"
  },
  "nodes" : [
      {
        "host_id": "<UUID>",
        "datacenter": "mydc",
        "rack": "myrack"
      },
      ...
  ],
  "snapshot": {
    "name": "snapshot name",
    "created_at": seconds_since_epoch,
    "expires_at": seconds_since_epoch | null,
  },
  "table": {
    "keyspace_name": "my_keyspace",
    "table_name": "my_table",
    "table_id": "<UUID>",
    "tablets_type": "none|powof2",
    "tablet_count": N
  },
  "tablets" : [
    {
        "id": <tablet id>
        "first_token": <token>,
        "last_token": <token>,
        "repair_time": <int64_t>,
        "repaired_at": <int64_t>
    }, 
    ...
  ]
  "sstables": [
    {
      "id": "67e35000-d8c6-11f0-9599-060de9f3bd1b",
      "toc_name": "me-3gw7_0ndy_3wlq829wcsddgwha1n-big-TOC.txt",
      "data_size": 75,
      "index_size": 8,
      "first_token": -8629266958227979430,
      "last_token": 9168982884335614769,
      "tablet_id": <tablet id>,
      "node": <node id>,
      "additional_nodes" : [ <other node id>, ... ]
    },
    ...
  ],
  "files": [ ... ]
}
```

The `scope` of the manifest is "dc" (note: technically, it can span multiple dcs), and instead of a top-level `node` attribute it has an array called `nodes` containing each node present in the backup.

The manifest also contains the set of tablets at the point of snapshot.

Note: cluster level backups utilize sstable de-duplication of the repair set. Thus, any sstables which fall into the repaired set of data will only be included from one of the replicas in the snapshot.

Each sstable entry points out owning node and tablet. A sstable can also optionally point to an array of additional nodes. These are nodes for which the
data in this sstable was de-duplicated, and to which it should be restored as well.

3. `CREATE KEYSPACE` with S3/GS storage

When creating a keyspace with S3/GS storage, the data is stored under the bucket passed as argument to the `CREATE KEYSPACE` statement.
Once the statement is issued, Scylla will transparently use the S3/GS bucket as the location of the SSTables for that keyspace.

Automatically managed object-storage tables use a unified layout shared across S3 and GS. SSTable components are stored under a static `sstables/` prefix and grouped by the stable SSTable identifier (`sstable_id`), not by the node-local generation name:

```perl
scylla-sstables-bucket/
│
└── sstables/
    ├── 4f4d0a90-d8c6-11f0-8b18-060de9f3bd1b/
    │   ├── Data.db
    │   ├── Index.db
    │   ├── Summary.db
    │   ├── TOC.txt
    │   ├── ...
    │   └── refs/
    │       └── nodes/
    │           ├── 7adf1ca2-6783-40ab-aa1f-ef1e0b5d98ba/
    │           │   └── 3gqe_1lnj_4sbpc2ezoscu9hhtor
    │           └── 10e68be1-d352-4173-bf34-2249dbb7329e/
    │               └── 4c5s_0281_0v5kg2b4gri84iggoz
    ├── 87a72290-d8c6-11f0-a5ea-060de9f3bd1b/
    │   ├── Data.db
    │   ├── Index.db
    │   ├── Summary.db
    │   ├── TOC.txt
    │   ├── ...
    │   └── refs/
    │       └── nodes/
    │           └── 7adf1ca2-6783-40ab-aa1f-ef1e0b5d98ba/
    │               └── 5n8a_03tw_2jv4g2a4k2m33sq8ah
    └── ...
```

The managed SSTable prefix is:

```text
sstables/
```

Each SSTable lives under:

```text
sstables/{sstable_id}/
```

SSTable component object names are the component suffixes used by the local SSTable format, for example `Data.db`, `Index.db`, `Summary.db`, `Scylla.db`, and `TOC.txt`. Since these object names do not include the SSTable version or format, Scylla stores them as object attributes on the `TOC.txt` object:

- `version` - the SSTable version, for example `me` or `mt`.
- `format` - the SSTable format, for example `big`.

These attributes are written when the TOC is uploaded and preserved when the TOC is copied as part of SSTable clone/component-rewrite flows. Tools that read live object-storage SSTables directly, such as `scylla sstable`, use these attributes to identify the SSTable descriptor before opening the component set.

Reference objects under `refs/nodes/` record which nodes still own a reference to the SSTable data:

```text
sstables/{sstable_id}/refs/nodes/{host_id}/{generation}
```

The reference object body is empty. Its name is the metadata: `{host_id}` identifies the node and `{generation}` is that node's local SSTable generation name for the shared SSTable.

The `sstable_id` identifies the shared object-storage SSTable data. The local `generation` identifies a node-local SSTable entry in `system.sstables`. A newly created SSTable normally has an `sstable_id` derived from its generation. After tablet migration or reference sharing, multiple local SSTable entries can have different generations while pointing at the same object-storage data via the same `sstable_id`.

Object-storage SSTable lifecycle:
- Creation: Scylla uploads component objects under `{sstable_id}` and creates this node's `refs/nodes/{host_id}/{generation}` reference before sealing the local row in `system.sstables`.
- Sharing: when tablet migration can share object-storage data, the receiving node creates a new local SSTable entry with its own generation and adds a node reference under the existing `{sstable_id}` prefix instead of copying all component objects.
- Local removal: when a node removes its local SSTable, it first deletes its own reference object. If other references remain, component objects are left intact.
- Final cleanup: component objects are deleted only after no reference objects remain for the `sstable_id`. This prevents one node from deleting shared data still referenced by another node.

The `status` and `state` fields in `system.sstables` describe the local SSTable entry lifecycle. They do not describe a global lifecycle state for the object-storage component set identified by `sstable_id`.

### Restore into object-storage tables

Only the tablet-aware restore, `/storage_service/tablets/restore`, can restore into a table on object storage. The legacy restore, `/storage_service/restore`, rejects such a table.

The restore does not download the components to the node. The components stay in the object storage. For each backup SSTable, the restore does one of two things:

- **Share.** If the backup is in the unified layout, in the location of the table, the components are already where the table needs them. The restore copies nothing. The restore creates only the `system.sstables` entry and the node reference `refs/nodes/{host_id}/{generation}`. Sharing needs the `SSTABLE_REFERENCE_SHARING` cluster feature.
- **Copy.** In all other cases, the object storage copies the components into the location of the table. The copy gets a new `sstable_id`, because the `sstable_id` is the prefix of the object names, and two replicas which restore the same backup SSTable must not write the same objects. `Scylla.db` contains the `sstable_id`, so the restore writes `Scylla.db` again instead of copying it.

An object storage can copy an object only inside one endpoint. So the backup and the table must use the same endpoint. The restore rejects a backup location on another endpoint before it starts.

The restore recognizes a backup in the unified layout when the backup has the same endpoint, bucket and prefix as the table. The object names of such a backup do not contain the generation. The restore takes the `sstable_id` from `system_distributed.snapshot_sstables`, and the generation, version and format from the `toc_name` of the manifest.

The snapshot can be dropped while the restore runs. So after the restore creates the node reference of a shared SSTable, the restore checks that the snapshot still has its reference `refs/snapshot-{tag}/{generation}`. If not, the restore removes the node reference and the entry, and fails. The components are deleted if no other reference is left.

No backup writes the unified layout yet. `/storage_service/backup` and the cluster backup keep the generation in the component names. So today every restore copies.

The restore creates each entry with the `creating` status, before it writes any object. The entry gets the `sealed` status when the restored SSTable is attached to the table. So after a failed or aborted restore, the entries which are not `sealed` are left. Boot time garbage collection removes them and their objects. For a shared SSTable, garbage collection removes only the node reference. The components stay while another reference exists.

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
As you might’ve seen in the Scylla Manager [Specification Docs](https://manager.docs.scylladb.com/stable/backup/specification.html), we keep a JSON manifest per node  
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

For `CREATE KEYSPACE` on S3/GS storage, Scylla tracks object-storage SSTables in `system.sstables` and uses reference objects under the SSTable prefix to decide when component objects can be deleted. Manual changes to the objects under this layout should keep `system.sstables`, component objects, and `refs/nodes/{host_id}/{generation}` objects consistent.

> **NOTE:**
> It’s obvious to say that re-uploading a scrubbed SSTable means re-uploading all its components as it’s likely most of them were changed.

# Relationship between references and the catalog tables in cluster snapshots/backups

This section describes how `refs/` and various catalog tables interact with each other
in order to provide consistent and crash safe cluster snapshotting/backups for remote
object-storage tables.
  
The `system_distributed.*` snapshot tables and the `refs/` objects in the bucket are
complementary halves of one mechanism, the references pin data (and are the core of
providing zero-copy cluster snapshots) and the catalog tables rows provide the accounting
to make sure snapshots/backups are crash-safe, consistent in high-concurrency cluster operations
and safe on retake.

A snapshot reference is an empty object at `{prefix}/{sid}/refs/snapshot-{tag}/{gen}`
which says that the `{tag}` snapshot has a claim too on this SSTable.
The nodes themselves (data) also have a claim on an SSTable via the refs at
`refs/nodes/{host_id}/{generation}` (only a single copy exists on object storage).
The union of these two refs sets is what governs the lifetime of the SSTable.
No references at all, the SSTable gets destroyed. No nodes refs, but at least a snapshot ref,
the SSTable is kept alive with its `status` state machine transitioned to `snapshot_owned`
in the sstables registry. 

The catalog tables rows are the only way to find those references again (well, you can also
list and try to figure it out, and sometimes we do in nasty corner cases) each
`snapshot_sstables` row records the sstable id, the owning node and the `toc_name` from
which the exact reference name is derived, while `snapshot_nodes`, `snapshot_tablets`,
`snapshot_keyspaces` and `snapshot_tables` record who participated and what the schema
looked like. The `snapshots` row is the commit marker for a cluster snapshot.

There is a particular ordering we keep consistent all over the implementation to be able
to provide crash safety and recovering capabilities.
A snapshot writes catalog rows first, then creates the references
and the coordinator writes the commit marker last, so a committed tag's
rows are guaranteed to name every reference. Cleanup runs in the opposite order, each
node derives its reference names from its rows and deletes the references first
(falling back to listing `{sid}/refs/` in edge cases when a row cannot name its reference),
then deletes the rows and the commit marker from `snapshots` goes last.

### Write order

The coordinator writes the schema rows (`snapshot_keyspaces`, `snapshot_tables`) before fanning out the per-node RPCs,
this is what makes a crashed attempt even enumerable from the catalog.
Each node cleans up any residue of a previous failed attempt under the tag,
inserts its `snapshot_sstables`/`snapshot_nodes`/`snapshot_tablets` rows, and only then
it creates its snapshot `refs/`.
The coordinator writes the snapshots marker last, after all nodes succeeded.

### `sstables` registry coupling

When a snapshotted sstable is later compacted away, the node deletes its `nodes/` reference.
If snapshot references remain, the components are kept and the sstable's registry entry is retained
as snapshot_owned instead of being deleted.
The boot GC we have inplace re-evaluates `snapshot_owned` entries. Once the refs listing is empty,
it deletes the components and the entry.
So the catalog tables do the refs accounting, the refs pin the SSTables components and the local registry remembers
which pinned SSTables this node must keep re-checking.

### Inplace restores

In order to accomodate restoring into the same cluster where the snapshot still lives, there is a trick involved
so that restores TTL-based GC doesnt end up destroying the original snapshots/backups.
Restore never touches the snapshot tag's rows, it stages its working rows under `{tag}/restore/{table_id}` (TTL'd so that crashed restores and currently even succesful restores cleanup their rows automatically in 3 days) and relies on the snapshot's references to keep components alive while cloning.

### Crashing behavior

Crash recovery leans mainly on the write order, we try hard to never end up with refs in the bucket
that cannot be index from a catalog table row somewhere. In some edge cases though it can happen and that's
why we will need to implement a background healing operation to scan the bucket and collect garbage from past crashes.

A snapshot writes its catalog rows before creating any ref and the commit marker only at the very end,
so whatever a crash leaves behind is always discoverable. A tag that has rows but no marker is
by definition a failed attempt, and the rows are enough to find and release its refs. Each row's toc_name
points to the exact ref object (in corner cases where we lost that, we list sstable's refs/ directory to recover them).

We currently have around 4 codeplaces doing garbage cleanup of crashed attempts, each doing it at the moment when most
information is available so we avoid doing lots of object storage requests or QUORUM reads to figure out what needs to get deleted.
a) Immediate rollback done inside the failing attempt itself, it releases the refs best-effort potentially leaving only rows
which will be cleaned up by one of the other mechanisms below.
b) Cleanup on re-take, it happens in 2 stages: per node (which cleans whatever is node-owned: refs, snapshot_sstables, components and registry entry) and via coordinator (for the schema and tablet catalog rows).
c) Boot GC, it's node local, it re-evaluates `snapshot_owned` entries from the registry and potentially deletes, refs, components and the entry itself if conditions are met.
d) (future work) `clearsnapshot --force` it's operator driven which is able to delete garbage if no re-take happens for a previously crashed tag.
e) (future work) background healing operation as a last resort process: cleaning refs whose rows are gone, also covers some edge cases like
departed nodes or tables dropped in a particular bad moment. It'll also do automatic expiration of snapshots.

### Layout examples for local and remote tables

Assuming keyspace `ks`, table `t`, table_id `f0d1c0de`, one sstable with id `3gqe…tor` and generation `3gqe_1lnj_4sbpc2ezoscu9hhtor`,
node `n1`, host_id `aaaa0001` in `dc1`, `rack1`, snapshot tag `tagalong`. The restore target table has id `b00bcafe`.
Backups sit at `endpoint/backups` bucket, prefix `bk`.
Remote table to be snapshotted/backed-up/restored lives in `scylla-bucket/sstables/..`

#### Local tables

a) Snapshot
Each node hardlinks the sstable components into the usual per-table snapshot directory, <data_dir>/ks/t-<uuid>/snapshots/tag/.
Nothing is written to any bucket. The catalog accounts the snapshot: the coordinator writes the schema rows (`snapshot_keyspaces`, `snapshot_tables`), every node inserts its `snapshot_nodes`, `snapshot_tablets` and `snapshot_sstables` rows (one per sstable, state=local, with the toc_name naming the on-disk sstable), and the coordinator writes the `snapshots` commit marker last.

~~~
disk (each node):
  <data_dir>/ks/t-f0d1c0de/snapshots/tagalong/me-3gqe_..._tor-big-Data.db   (hardlink)
  <data_dir>/ks/t-f0d1c0de/snapshots/tagalong/me-3gqe_..._tor-big-*.db     (all components)
bucket: nothing

catalog (in this order):
  snapshot_keyspaces  ('tagalong','ks')               -> keyspace CREATE statement       [coordinator, pre-RPC]
  snapshot_tables     ('tagalong','ks','t')           -> f0d1c0de, CREATE TABLE,
                                                         tablet_layout                   [coordinator, pre-RPC]
  snapshot_nodes      ('tagalong','dc1','rack1',aaaa0001)                              [node n1]
  snapshot_sstables   ('tagalong','ks','t','dc1','rack1', first_token, 3gqe…tor)
                      -> toc_name='me-3gqe_..._tor-big-TOC.txt', node=aaaa0001,
                         tablet=0, state=local, prefix=''                                [node n1]
  snapshot_tablets    ('tagalong','ks','t','dc1', first_token)
                      -> tablet_id=0, last_token, repaired_at                            [node n1]
  snapshots           ('tagalong')                    -> created_at, expires_at          [coordinator, LAST:
                                                                                          the commit marker]
~~~

b) Backup
The coordinator marks the destination in `snapshot_remote_locations` (state=being_backed_up) and fans out per-node upload requests.
Each node uploads its snapshotted components into the refs-based layout under the destination prefix `bucket/bk/sstables/3gqe…tor/<component>`
and creates a snapshot reference `bucket/bk/sstables/3gqe…tor/refs/snapshot-<tag>/<gen>` before uploading, so the uploaded sstable is pinned exactly like a native object-storage one. As uploads complete, the node's `snapshot_sstables` rows flip to state=remote_and_local (or remote, when the local snapshot files are removed after upload).
Finally one manifest per destination is written at `bucket/bk/snapshots/ks/t/tagalong/manifest.json` and `snapshot_remote_locations` flips to the final state.
The snapshot ref is the uploaded SSTable's only claim; if multiple snapshots reference the SSTable, it is destroyed on object storage when the last ref goes.
This mechanism doesn't touch `system.sstables` registry, that is only used for remote object storage tables. 
On local disk nothing new appears; with --move-files the snapshot directory contents are removed.

~~~
bucket (per sstable, the reference goes first):
  backups/bk/sstables/3gqe…tor/refs/snapshot-tagalong/3gqe_1lnj_4sbpc2ezoscu9hhtor
      (empty object; the uploaded sstable's only claim, no node refs are created)
  backups/bk/sstables/3gqe…tor/me-3gqe_..._tor-big-Data.db
  backups/bk/sstables/3gqe…tor/me-3gqe_..._tor-big-*.db                     (all components)
bucket (per destination, last):
  backups/bk/snapshots/ks/t/tagalong/manifest.json

catalog:
  snapshot_remote_locations ('tagalong','dc1') -> endpoint, 'backups', 'bk',
                            state: being_backed_up -> remote_and_local
                            (remote with --move-files)
  snapshot_sstables         row of 3gqe…tor rewritten with state=remote_and_local,
                            everything else unchanged

disk: unchanged (--move-files removes the snapshot directory contents)
~~~

c) Restore
Restore is driven by the manifests, not by the catalog tables. `restore_tablets` takes the manifest paths, parses them, and populates its working rows into `snapshot_sstables`/`snapshot_remote_locations` catalogs under a staging name derived from the tag and the target table id, `tag/restore/<table_id>`.
Restore pushes these catalog rows with a TTL so a crashed restore's rows expire. The live tag's rows are never read or modified.
The nodes download the components tablet by tablet from `bucket/bk/sstables/<sid>/` into the target table's live data directory as freshly named sstables, and set the `downloaded` flag on the corresponding staging row, which makes the restore resumable.
The bucket is only read, the snapshot's references and the live catalog rows are untouched.

~~~
catalog:
  snapshot_remote_locations ('tagalong/restore/b00bcafe','dc1')
                            -> endpoint, 'backups', 'bk', state=remote
  snapshot_tables           ('tagalong/restore/b00bcafe','ks','t')
                            -> the target table's pre-restore CREATE statement
                               (recovers the original tablet hints after a crash)
  snapshot_sstables         ('tagalong/restore/b00bcafe','ks','t','dc1','rack1',
                             first_token, 3gqe…tor)
                            -> state=remote, prefix='bk/sstables/3gqe…tor',
                               downloaded=false
  per downloaded sstable:   UPDATE downloaded=true
                            (the resume point for a retried restore)

disk:
  <data_dir>/ks/t-b00bcafe/me-<fresh generation>-big-*.db   (downloaded components,
                                                             new generations)

bucket: read-only. The live tag's rows and references: untouched.
~~~

#### Remote tables

a) Snapshot
No data moves at all. The components already live at `bucket/sstables/3gqe…tor/`.
Each node creates one empty reference object per sstable, `bucket/sstables/3gqe…tor/refs/snapshot-<tag>/<gen>`,
which pins the components for as long as it exists (components are deleted only when the sstable's whole refs/ listing is empty, both node refs and snapshot refs).
The catalog records the same rows as in the local case, with `toc_name`/`prefix` pointing into the bucket, and the `snapshots` marker commits the tag.

~~~
bucket:
  scylla-bucket/sstables/3gqe…tor/refs/snapshot-tagalong/3gqe_1lnj_...
      (one empty object per sstable; no data is copied)
disk: nothing (object-storage tables have no local snapshot directories)

catalog: the same six writes as the local snapshot, same order, marker last;
         the sstable row's toc_name/prefix point into the bucket
~~~

b) Backup
This is mostly an in-place promotion.
The components are already in the table's own bucket and pinned by the snapshot references since snapshot time,
so nothing is uploaded and no new references are created.
The `snapshot_remote_locations` catalog rows flip state to `remote`.
For the `snapshot_sstables` catalog, the per-node handler rewrites each uploaded sstable's row with the state promoted.
The only new created object on remote is the manifest at `scylla-bucket/sstables/snapshots/ks/t/<tag>/manifest.json`.

~~~
bucket:
  scylla-bucket/sstables/snapshots/ks/t/tagalong/manifest.json
      (the only new object; the components are already in place, pinned by the
       snapshot references since snapshot time)

catalog:
  snapshot_remote_locations ('tagalong','dc1') -> endpoint, 'scylla-bucket',
                            'sstables', state=remote
  snapshot_sstables         rows rewritten with state=remote
~~~

c) Restore
Manifest-driven and staged exactly like the local case.
The difference lies in how the data lands. When the backup lives in the same bucket under the unified layout,
the target table's sstables are cloned by reference sharing, each node creates its own `refs/nodes/<host>/<gen>` reference and `system.sstables` registry entry for the shared components.

~~~
bucket:
  scylla-bucket/sstables/3gqe…tor/refs/nodes/aaaa0001/<clone generation>
      (the clone's node ref, created after verifying that
       refs/snapshot-tagalong/... still exists; the clone is undone if the
       snapshot vanished mid-restore)
  no components are copied; a backup elsewhere on the same endpoint is
  byte-copied under a fresh sstable id; a backup on another endpoint is rejected

node-local:
  system.sstables gains the clone's registry entry

catalog: the same staging rows as the local restore
~~~

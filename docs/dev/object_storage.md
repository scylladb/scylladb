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

The same list can also be given on the command line, as a single string in
YAML or JSON syntax. This is convenient for tests, which then don't need to
write a configuration file:
```sh
scylla --object-storage-endpoints '[{"name": "http://127.0.0.1:9000", "aws_region": "us-east-1"}]'
```

The option can also be changed while Scylla is running, by updating the
`system.config` table with the same string. The new list replaces the old one:
```cql
UPDATE system.config SET value = '[{"name": "http://127.0.0.1:9000", "aws_region": "us-east-1"}]'
    WHERE name = 'object_storage_endpoints';
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

## Object storage page cache

A keyspace created with the `'disk_cache': 'pages'` storage option (which
requires the experimental feature `object-storage-page-cache`) caches, on the
node's local disk, the byte ranges of its sstables which it reads from the
object store. See the [CQL extensions](../cql/cql-extensions.md) for the
option, and the [administration guide](../operating-scylla/admin.rst) for its
configuration and metrics. The code is in the files named
`*object_storage_page_cache*`, each of which starts with a comment explaining
its part. This section explains how the parts fit together.

### The tables

The cache is stored in two tables of the node-local `system_cache` keyspace,
whose writes skip the commitlog (`durable_writes = false`): the cache only
holds copies of what the object store holds, so losing recent writes in a
crash is harmless.

* `system_cache.pagecache`: the cached pages, with the key
  `((shard, sstable), component, offset)` and the page as a blob. All the
  pages of an sstable are in one partition, so dropping them takes a single
  partition tombstone.
* `system_cache.pagecache_hits`: a small row for each cached page, with the
  same key, recording the page's size and when it was last hit. It is a
  separate table because it is rewritten on every hit, while the pages are
  written once; and choosing which pages to evict reads only this table.

Eviction and the count of the cache's bytes find pages only through their
rows in `pagecache_hits`, so a page left without one would stay, uncounted,
until its sstable is deleted:

* A page's row in `pagecache_hits` is written before the page itself - a
  failure between the two writes leaves a row without a page, which is
  harmless - and the eviction of a page doesn't come between these two writes:
  the hand doesn't evict a page which is being cached, and a page isn't cached
  while the hand is evicting it.
* A crash loses what the memtables hold, and when memtables take too much
  memory, the largest is flushed - almost always `pagecache`'s - so the rows
  of `pagecache_hits` could stay in memory for long, while their pages were
  flushed; and a flush of `pagecache_hits` could persist the deletion of an
  evicted page's row there, while the deletion of the page itself was still in
  `pagecache`'s memtable. So once a minute, the eviction fiber flushes the
  tables, as needed: if pages were evicted, `pagecache` and then
  `pagecache_hits` (between evictions); if pages were only cached, only
  `pagecache_hits`; and if nothing changed, nothing - so an idle cache, or one
  with room to spare, doesn't flush `pagecache` every minute. Then a crash
  leaves without their rows only the pages cached in about the last minute
  before it.

Both tables use the `fixed_shard_partitioner`, which puts the `shard` column
in the high bits of the token, so each shard's cache - the cache of the
sstables it owns - is stored on that shard, and is a contiguous range of
tokens. Each shard reads and writes only its own cache. Their row cache is
disabled (the pages are cached in memory by the user tables' own caches), and
their tombstones are dropped immediately.

Both tables use incremental compaction - `pagecache` with a space
amplification goal of 1.5, and `pagecache_hits` without one. The
[administration guide](../operating-scylla/admin.rst) explains these choices,
and the disk space which the cache takes.

The cache is reached through `sstables::object_storage_page_cache` (an
interface, so the sstables code doesn't depend on the code which reads and
writes tables), implemented by `db::object_storage_page_cache_table`. It
reads and writes the tables directly, rather than with internal CQL, for
the lookups and writes which reads of sstables make, and uses internal CQL
queries only for its scans of whole tables or sstables. A lookup is made
while an sstable read holds its permit of its reader concurrency semaphore,
so it must not wait on that semaphore, which could be waiting for the read:
an internal CQL query would wait there for admission - all the reads would
wait for their lookups, and their lookups for admission - and even a lookup
which skips admission would wait there for memory, when the semaphore's
reads use too much memory, and let only one of them proceed - possibly the
one waiting for the lookup. So the lookups read with permits of a semaphore
without limits, `database::object_storage_page_cache_semaphore()`, which
never waits; the pages which a read keeps are tracked in its own permit,
without waiting either.

### Pages

The cache holds an sstable component's bytes exactly as they are stored in
the object store - below the file I/O extensions, so the pages of an
encrypted sstable stay encrypted. A component is split into pages
(`object_storage_page_cache_geometry.hh`):

* a compressed Data component, in its compressed chunks;
* an encrypted Data component, in the encryption's 4 KiB blocks, because
  decryption needs whole blocks anyway;
* any other component - an uncompressed Data component, and the index
  components - in fixed 4 KiB pages.

The Data component is read through a `data_source`
(`object_storage_page_cache_source.hh`), and the index components through a
`seastar::file` (`object_storage_page_cache_file.hh`) which uses the same
`data_source`. It reads the cached pages of its range with a cursor, a
mutation reader of `system_cache.pagecache` which it keeps open from page to
page - and between pages of a query, when the querier cache saves the
query's sstable reader - and at the first page which isn't cached, reads all the rest of its range from the
object store with a single request - even if some of it is cached - and
caches the pages it reads. This minimizes the number of requests, assuming
that a request costs the same regardless of its size. A read may skip
forward - for example, between rows far apart in a wide partition. A skip of
up to a megabyte (`max_read_through_skip`) continues with the same request,
and caches the skipped pages too; after a longer skip, the source closes the
request, rather than download pages which the read doesn't need, and
continues from the cache, and with a new request at the next page which
isn't cached.

### Which reads populate the cache

Every read of an sstable of a `disk_cache` keyspace reads the cached pages,
but only the user's reads (`table::make_mutation_reader()`, directly or
through the row cache) without `BYPASS CACHE` add pages to the cache, and
record hits. This is the `sstables::populate_disk_cache` flag, which defaults
to `no`, so background reads - compaction, streaming, view building, and
opening an sstable - don't need to set it. A read which must not populate the
cache reads the index through a second set of index files, which don't
populate it, and doesn't use the in-memory caches of the index, which read
their misses through the populating files. The reader of sstables of the old
`ka` and `la` formats, which Scylla no longer writes, never populates the
cache.

### Eviction

* When an sstable is deleted, its pages are evicted
  (`object_storage_base::destroy()`), before its objects are deleted. When
  compaction deletes the sstables it compacted, the sstables it wrote already
  hold their cached data (see "Compaction-resistant caching" below).
* When the node starts, after loading the sstables, each shard evicts the
  pages of sstables which no longer exist on it - for example, after a crash
  between deleting an sstable and evicting its pages
  (`sstables_manager::evict_orphans_from_page_cache()`). The startup
  doesn't wait for the cache to be recounted without these pages - a count
  scans all of the shard's `pagecache_hits`, which may take minutes in a
  large cache - but the background fiber recounts it.
* When a shard's cache grows beyond its share of
  `object_storage_page_cache_size_in_mb`, a background fiber evicts the
  pages which were least recently hit, approximately, until the cache is
  10% under its limit. A hand moves over the shard's rows of
  `pagecache_hits`, in the order of their keys, and evicts every page which
  was never hit since it was cached, or was last hit before a cutoff time.
  When the hand starts a pass, it chooses the cutoff from a histogram of how
  long ago the cache's pages were last hit, so that the pages which it
  evicts add up to at least a quarter of the cache - first the pages which
  were never hit. So pages are evicted in the order of their last hits, not
  of their keys; and one pass, with one cutoff, serves several evictions.
  If pages are hit during a pass - after the histogram from which its
  cutoff was chosen - a pass may evict too little; so if two passes didn't
  evict enough, the third evicts pages regardless of their hits, which
  keeps the cache within its limit even if all of its pages are hit all the
  time.

  To save writes, each shard remembers which pages it recorded a hit of in
  the hand's current pass, and doesn't write their hits again - so a page's
  last hit is known only to within a pass. It remembers them in a Bloom
  filter, of about 1.2 bytes per page, sized for the pages which the cache
  can hold: about 1% of the other pages seem to be in it, so their first hit
  in the pass isn't recorded, and they may be evicted a pass early. A page
  cached while it is in the filter is written with a hit, because the
  filter can't forget it. A read records its hits in the
  background, rather than wait for their writes - which wait when memtables
  take too much memory - and if too many of these writes are in progress, it
  doesn't record its hits at all.

  Each shard counts the bytes in its cache in memory, adding a page's size
  when it is cached and subtracting it when it is evicted. Caching a page
  which was already cached - for example, when a long read which missed the
  cache fetches the rest of its range, including pages which were cached -
  counts it again. So as the hand passes the pages, it recounts their bytes,
  and builds the histogram for its next pass, and when it completes a pass,
  the recount replaces the count. The hand reads `pagecache_hits` in batches,
  so it knows which pages cached during the pass it won't see - those up to
  the end of its batch, which aren't in it - and these are counted when they
  are cached, or, if they are cached while the hand fetches a batch, when the
  batch arrives. If the hand stops in the middle of a batch, it stops
  counting the pages cached within the batch which are still after it, which
  it will fetch again. Only when the node starts, and after it evicted the
  orphans, are the bytes and the histogram counted by a scan of the table,
  in the background (`recount_cache()`) - and the evictions, which subtract
  the pages they evict from the count, wait for the scan, which may have
  counted these pages.

### Compaction-resistant caching

Compaction replaces its input sstables with new ones, and deleting the inputs
evicts their pages. If the new sstables started with nothing cached, every
compaction would lose the cache of the data it compacts - so a workload which
writes, and therefore compacts, would keep losing its cache, and a major
compaction would drop all of it. So while compaction writes an output sstable,
it copies to the cache the pages of the output which hold data that was cached
in an input (`object_storage_page_cache_compaction.hh`). When the inputs are
deleted, the output already has their data cached. Until then, the cache
holds both - incremental compaction deletes its inputs while it runs, so this
is temporary and bounded.

Input and output pages don't correspond by their offsets - an output page holds
the merged data of all the inputs - but by the keys of their data. This must
be accurate to about a page, because when the cache holds a small part of the
data, its pages are scattered, so a neighbor of the right page is useless. So
the mapping isn't by when the pages are read and written - each input is read
ahead of the merged data which is written, by many pages - but by keys:

* For each input sstable, the reader tells, for each fragment (partition start,
  row, range tombstone change, partition end), where it starts in the Data
  component, through compaction's `read_monitor` of the sstable. Whether that
  position's page is cached is found in the sstable's rows of
  `pagecache_hits`, which are read alongside the reader, in batches - not from
  compaction's own reads, which read the cached pages only until the first page
  which isn't cached, and then read the rest of the sstable from the object
  store in one request. The rows also tell when the pages were last hit. The
  input keeps its fragments which were in cached pages ("hot").
* For each output sstable, the writer tells the key and the offset of each
  fragment it writes, through compaction's `write_monitor`, and the fragment is
  hot if an input had a hot fragment with the same key (partition and position
  in it). The merged data is written in the order of its keys, and every input
  is read past a key before the data at that key is written - the merge needs
  the next fragment of every input to choose which is first - so the inputs and
  outputs only move forward, and keep little in memory.
* A page of the output's Data component (a compressed chunk, or 4 KiB of an
  uncompressed one) is copied if at least a 16th of its data is hot. The cache
  knows which pages were read, but not which of their rows: a cached page of a
  large input holds about the same data as a page of the output, which should
  be copied - with the rows around its rows, such as rows which writes added -
  but a cached page of a small input, such as an sstable which a memtable
  flush wrote, whose rows are spread thinly over the keys, holds a range of
  keys which fills many pages of the output (about as many as the output is
  larger than the input), each with a few of its rows among many which weren't
  cached. Copying all of these would turn one cached page into tens or
  hundreds, for a read which probably needed one of its rows - and we don't
  know which. A read of a row also reads the data
  around it: a forward read reads from the start of the row's block in the
  partition's row index (or from the start of the partition, if it has no
  row index), a reversed read reads the whole block, and both parse the
  start of the row after the last one they return. The blocks of the output
  aren't those of the inputs, so when a page is copied, so are all the
  blocks with hot data in it, whole, with the start of the row after each.
* The sink which writes each of the output's components to the object store,
  below compression and the file I/O extensions, splits it into the pages in
  which reads cache it, and copies the pages to copy with
  `object_storage_page_cache::put_copied_page()`. A page which isn't decided
  yet - because the writer hasn't finished writing its data - waits in the
  sink until it is. The storage finds the output by the sstable's id, so the
  sstable needn't know about it.
* The index is copied too, so that a read of cached data needs no request to
  the object store at all: a partition's entries in the index are copied if its
  hot data is in Data pages which are copied. A partition usually ends before
  the page which holds its end is decided, so the decision about its index
  entries waits for the page. The Index component of a `me` sstable is read by
  intervals: a read reads the whole range of the Index between two of the
  Summary's entries - and the next range too, if the entry it looks for is the
  last in its range, to know where the partition ends. So the whole interval
  is copied if any of its partitions is hot, and so is the next interval if
  the last partition of a hot interval is hot; its pages wait in the sink until
  this is decided. (So the whole entry of a hot wide partition is copied,
  including its promoted index, even if only a few of its rows are hot.)
* The Partitions and Rows components of a BTI sstable are tries, which a read
  descends from the root to the key it looks for - and then to the next key,
  to know where its partition, or block of rows, ends. So the writer of each
  trie marks the keys whose data is hot, and the next keys, and tells the
  output the position of each node it writes whose subtree has a marked key
  - including the root - before writing the node; the page which holds the
  node is copied. (The marks are a generic feature of the trie writer: each
  key may have a 32-bit mark, which isn't written, and the writer tells its
  sink the largest mark in the subtree of each node it writes.) A key in
  Partitions is marked if its partition is hot; a key in Rows - the
  separator of a block of a partition's rows - if its block is copied; and the
  header of a partition's entry in Rows, which a read reads first, if the
  partition is hot. Whether they are is decided only when the partition's
  blocks are, which may be after the partition ends, so the writer writes a
  partition's entries in Rows and Partitions only then.

A copied page gets the latest last hit of the input pages which made it hot,
so it is evicted when they would have been. If it got no last hit, the hand
would evict it first - in a full cache, right away - and if it got the current
time, data which was compacted again and again would look recently used, and
never be evicted.

Most compactions compact data of which nothing is cached, so before a
compaction reads its inputs, it looks up whether any of them has a Data page
cached - and if none does, it doesn't track its inputs and outputs at all.

A failure to read `pagecache_hits` or to copy a page doesn't fail the
compaction: it only stops copying pages. Resharding, which writes sstables of
other shards, doesn't copy pages, and neither does scrub, which may write its
outputs interleaved rather than in the order of their keys.

The copying is approximate, so the cache doesn't keep exactly what it held:

* Rows which were read only from an sstable much smaller than the compaction's
  output aren't kept. A cached page of an input holds a range of keys which
  fills about as many pages of the output as the output is larger than the
  input - so if it is more than 16 times larger, less than a 16th of each of
  these pages is hot, and none is copied. This happens when a small sstable,
  such as one which a memtable flush wrote, is compacted into much more data:
  mostly by a major compaction (or TWCS's compaction of a closed window), but
  also by a size-tiered compaction of more than 16 sstables, or by leveled
  compaction of a flush into L1, if L1 holds more than 16 times as much data.
  A size-tiered compaction of a few sstables of similar sizes, or leveled
  compaction from one level to the next, which holds 10 times as much data,
  doesn't. And because the sizes of sstables grow geometrically, sstables more
  than 16 times smaller than a compaction's output hold only a small part of
  its data (typically a few percent), though it is the newest data, which some
  workloads read more. A read of such a row misses the cache once after the
  compaction. A row which was read from a large input too is kept, because
  that input's cached page makes the output's page hot. Likewise, a hot page
  isn't copied if writes added to its range more than 15 times as much data as
  it had.
* Conversely, when the output is larger than an input by up to 16 times, a
  cached page of the input is copied as that many pages of the output (in
  size-tiered compaction, about as many as the compaction's inputs, and in
  leveled compaction from one level to the next, about 11), all with its last
  hit, though a read probably needed only one of them. Repeated compactions
  compound this, until the copies of pages which aren't read again are
  evicted, as their last hits age; a row which is read again keeps about that
  many pages cached, rather than one.
* A copied page copies the whole row-index blocks with hot data in it, which
  a reversed read reads, though a forward read of a row reads only from the
  start of its block - so in a wide partition, a hot row may cost the cache
  up to a whole block (64 KiB by default) more than was cached. In a `me`
  sstable, the whole Index entry of a hot partition is copied, including all
  of its promoted index.
* Until the inputs are deleted, the cache holds both their pages and the
  copies. Incremental compaction deletes its inputs as it goes, but a
  compaction which deletes them only at its end can grow the cache by up to
  all of its hot data - and if this exceeds the cache's limit, the hand may
  evict copies, whose last hits are those of the input pages, rather than
  the input pages, which will be deleted anyway.
* The sink holds the output's pages in memory until it is decided whether
  to copy them - from the start of the row-index block being written, so
  at most about a block, except for a row larger than a block, which the
  sink holds whole (as the reader already holds it).
* Reads go on during a compaction, and keep caching pages of its inputs, and
  recording hits of them - but each input fetches its rows of
  `pagecache_hits` in batches, ahead of where the compaction reads it, so it
  doesn't see the pages which are cached or hit behind that: such a page
  isn't copied, so a read of it misses the cache once after the compaction,
  and the copy of a page doesn't get such a hit, so it looks older than it
  is, and may be evicted sooner. With incremental compaction this lasts only
  briefly: each output sstable, once written, replaces the input sstables
  which the compaction exhausted, and reads then cache, and hit, its pages
  directly. A compaction which replaces its inputs only when it ends, such as
  a major compaction of a large table, may lose more.
* sstables which aren't written by compaction or by a memtable flush (see
  "Caching on flush" below) - by repair, streaming, or a tablet's migration
  to another node - start with nothing cached.

### Caching on flush

A workload which reads data soon after writing it would read it from the
object store after every memtable flush: memtables are flushed directly to
the object store. So with caching on flush, a flush caches all the pages of
the sstable which it writes, as it writes them, with the same machinery as
compaction-resistant caching: `make_page_cache_flush_output()` makes an output, which the flush's
write monitor gives the sstable writer, whose data is all hot.

The flushed pages are cached in the middle of the order in which the hand
evicts pages, nearer to its end: as if they were last hit just after the pages
which were never hit, or last hit longest ago, which make up a quarter of the
cache (`flushed_page_last_hit()`, from the histogram of the pages' ages). So the hand evicts them soon - early in its next pass -
unless they are read, but not before the pages which were never hit, such as
those which a scan cached; and if those are a quarter of the cache, the
flushed pages join them. Caching them as just hit would let a write-heavy
workload push out the pages which reads use; caching them as never hit would
let them be evicted before the reads which follow the writes come.

`object_storage_page_cache_populate_on_flush` (default true) disables it.

### Migrating tablets

When a tablet moves between shards of the same node, its sstables are cloned
by reference, keeping their sstable id, so the pages which the old shard
cached are copied to the new shard's cache, before the clones are loaded
(`storage_service::clone_locally_tablet_storage()`). When a tablet moves to
another node, the new node starts with an empty cache, and the old node
evicts its pages when it deletes its sstables.

### Errors

A failure of the cache never fails a read: a failure to read pages from the
cache is treated as a miss, and a failure to cache a page or to record a hit
is logged and ignored.

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
* [system_distributed.snapshot_attempts](docs/dev/snapshot_attempts.md) - Lists the attempts of each cluster snapshot tag and which one committed.
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

A snapshot reference is an empty object at `{prefix}/{sid}/refs/snapshot-{name}/{gen}`
which says that the snapshot has a claim too on this SSTable. `{name}` is the
snapshot's catalog name: for a cluster snapshot it is `{tag}/{attempt}`, so the
object is `refs/snapshot-{tag}/{attempt}/{gen}`; for a backup uploaded from a
local table it is the plain tag (see "Attempts" below).
The nodes themselves (data) also have a claim on an SSTable via the refs at
`refs/nodes/{host_id}/{generation}` (only a single copy exists on object storage).
The union of these two refs sets is what governs the lifetime of the SSTable.
No references at all, the SSTable gets destroyed. No nodes refs, but at least a snapshot ref,
the components are kept and only the catalog rows know about them.

The catalog tables rows are the only way to find those references again (well, you can also
list and try to figure it out, and sometimes we do in nasty corner cases) each
`snapshot_sstables` row records the sstable id, the owning node and the `toc_name` from
which the exact reference name is derived, while `snapshot_nodes`, `snapshot_tablets`,
`snapshot_keyspaces` and `snapshot_tables` record who participated and what the schema
looked like. The `snapshots` row is the commit marker for a cluster snapshot, and
`snapshot_attempts` lists the tag's attempts and which one committed.

### Attempts

The attempt ID is not supposed to be user-exposed, but rather a record that the coordinator makes to keep track of whether a snapshot RPC round succeeded or not.
This helps us fix some issues we had with the previous implementation:

* Cleanup for retakes of crashed attempts used to be problematic and complicated, now a retake of a crashed attempt is pretty easy, coordinator sees there is no "committed" attempt, let's the request through and if it finishes successfully, the leftovers from past failed attempts are cleaned easily.
* We had a bunch of races of failed over coordinators cleaning up with new elected coordinator handling the retake. Now those will carry 2 separate attempt IDs, so no races.
* It makes redundant https://github.com/scylladb/scylladb/pull/32505 which works around inplace restores into the same cluster, for which we had to "invent" a staging name so that its TTL-based rows dont mess up the snapshot's own rows. 


This will work like this:
 1. snapshot request for tag comes, the topology request gets written to group0
 2. coordinator checks if for this tag, there is a successful attempt, if yes, rejects, if not, it lets it through.
 3. coordinator creates new attempt uuid. It inserts a (tag, uuid, committed=false) row in the new table snapshot_attempts . It also inserts the schema rows under tag/uuid .
 4. group0 barrier, replica set computed, RPC sent to each replica
 5. replica handler takes the session guard, takes the snapshot, writes its catalog rows under tag/uuid
 6. coordinator takes barrier, sets committed=true in the new snapshot_attempts table to mark that this attempt finished successfully, write the snapshots table row and then cleans up all crap from previous failed attempts of tag if any.

If an attempt fails/crashes/etc, a re-take basically repeats steps 2-5. The failed attempts catalog rows and refs are cleaned up by coordinator in step 6.

### Write order

The coordinator writes the attempt into the request entry and into `snapshot_attempts`,
then the schema rows (`snapshot_keyspaces`, `snapshot_tables`) under the attempt name,
before fanning out the per-node RPCs. This is what makes a crashed attempt enumerable
from the catalog.
Each node inserts its `snapshot_sstables`/`snapshot_nodes`/`snapshot_tablets` rows, and only then
it creates its snapshot `refs/`.
The coordinator commits the attempt and writes the snapshots marker last, after all nodes succeeded,
then collects the failed attempts of the tag.

### `sstables` registry coupling

When a snapshotted sstable is later compacted away, the node deletes its `nodes/` reference and its registry entry.
If snapshot references remain, the components are kept. The registry describes live sstables only, so the row goes away, the snapshot catalogs
will link the remained components/references and the code that releases the
last snapshot ref will delete the components too.

### Inplace restores

Restoring into the cluster where the snapshot still lives must not let the restore's TTL-based row cleanup destroy the original snapshot.
Restore writes its working rows (`snapshot_sstables`, `snapshot_remote_locations`, `snapshot_tables`) under the snapshot name of the restore request,
with a 3-day TTL so that crashed restores and currently even successful restores clean up their rows automatically.
The snapshot's rows live under the attempt name `{tag}/{attempt}`, so the restore never touches them, and the snapshot's references keep the components alive while cloning.
The `snapshot_remote_locations` row of a backup is keyed by the plain tag like the restore's, so an in-place restore of a backup overwrites it with a TTL'd row.

### Crashing behavior

Crash recovery leans mainly on the write order, we try hard to never end up with refs in the bucket
that cannot be index from a catalog table row somewhere. In some edge cases though it can happen and that's
why we will need to implement a background healing operation to scan the bucket and collect garbage from past crashes.

A snapshot writes its catalog rows before creating any ref and the commit marker only at the very end,
so whatever a crash leaves behind is always discoverable. An attempt that is not committed in
`snapshot_attempts` is by definition a failed one, and its rows are enough to find and release its refs. Each row's toc_name
points to the exact ref object (in corner cases where we lost that, we list sstable's refs/ directory to recover them).

We currently have 4 codeplaces doing garbage cleanup of crashed attempts, each doing it at the moment when most
information is available so we avoid doing lots of object storage requests or QUORUM reads to figure out what needs to get deleted.
a) Collection of failed attempts, done by the coordinator after the commit. For every attempt of the tag but the one the request entry names it releases the refs (and the components when a ref was the last claim), deletes the snapshot_sstables rows, prunes the schema, tablets and nodes catalog rows and deletes the attempts row. The (dc, rack) partitions come from the attempt's snapshot_nodes rows, so it also reaches rows of nodes that
stopped being replicas since. A failed request collects its own attempt the same way. A crash between the group0 write of the
attempt and its snapshot_attempts row leaves an attempt named only in the request entry.
b) Boot GC, it's node local, it reaps the registry entries of sstables whose creation or removal did not complete, deleting their refs and components if the last ref was dropped.
c) (future work) `clearsnapshot --force` it's operator driven which is able to delete garbage if no re-take happens for a previously crashed tag.
d) (future work) background healing operation as a last resort process: cleaning refs whose rows are gone, also covers some edge cases like
departed nodes or tables dropped in a particular bad moment. It'll also do automatic expiration of snapshots.

### Layout examples for local and remote tables

Assuming keyspace `ks`, table `t`, table_id `f0d1c0de`, one sstable with id `3gqe…tor` and generation `3gqe_1lnj_4sbpc2ezoscu9hhtor`,
node `n1`, host_id `aaaa0001` in `dc1`, `rack1`, snapshot tag `tagalong`. The restore target table has id `b00bcafe`.
Backups sit at `endpoint/backups` bucket, prefix `bk`.
Remote table to be snapshotted/backed-up/restored lives in `scylla-bucket/sstables/..`

#### Local tables

a) Snapshot
Each node hardlinks the sstable components into the usual per-table snapshot directory, <data_dir>/ks/t-<uuid>/snapshots/tag/.
Nothing is written to any bucket. The catalog accounts the snapshot under the attempt name `tagalong/<attempt>`: the coordinator writes the `snapshot_attempts` row and the schema rows (`snapshot_keyspaces`, `snapshot_tables`), every node inserts its `snapshot_nodes`, `snapshot_tablets` and `snapshot_sstables` rows (one per sstable, state=local, with the toc_name naming the on-disk sstable), and the coordinator commits the attempt and writes the `snapshots` commit marker last. Only the snapshot directory, the `snapshot_attempts` rows and the marker are keyed by the plain tag.

~~~
disk (each node):
  <data_dir>/ks/t-f0d1c0de/snapshots/tagalong/me-3gqe_..._tor-big-Data.db   (hardlink)
  <data_dir>/ks/t-f0d1c0de/snapshots/tagalong/me-3gqe_..._tor-big-*.db     (all components)
bucket: nothing

catalog (in this order):
  snapshot_attempts   ('tagalong', <attempt>)         -> committed=false                 [coordinator, pre-RPC]
  snapshot_keyspaces  ('tagalong/<attempt>','ks')     -> keyspace CREATE statement       [coordinator, pre-RPC]
  snapshot_tables     ('tagalong/<attempt>','ks','t') -> f0d1c0de, CREATE TABLE,
                                                         tablet_layout                   [coordinator, pre-RPC]
  snapshot_nodes      ('tagalong/<attempt>','dc1','rack1',aaaa0001)                    [node n1]
  snapshot_sstables   ('tagalong/<attempt>','ks','t','dc1','rack1', first_token, 3gqe…tor)
                      -> toc_name='me-3gqe_..._tor-big-TOC.txt', node=aaaa0001,
                         tablet=0, state=local, prefix=''                                [node n1]
  snapshot_tablets    ('tagalong/<attempt>','ks','t','dc1', first_token)
                      -> tablet_id=0, last_token, repaired_at                            [node n1]
  snapshot_attempts   ('tagalong', <attempt>)         -> committed=true                  [coordinator]
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
Restore is driven by the manifests, not by the catalog tables. `restore_tablets` takes the manifest paths, parses them, and populates its working rows into the `snapshot_sstables`/`snapshot_remote_locations` catalogs under the snapshot name of the request, `tagalong`, and persists the target table's pre-restore schema in `snapshot_tables` under the same name.
Restore writes these catalog rows with a 3-day TTL so a crashed restore's rows expire. The snapshot's rows live under `tagalong/<attempt>` and are never read or modified.
The nodes download the components tablet by tablet from `bucket/bk/sstables/<sid>/` into the target table's live data directory as freshly named sstables, and set the `downloaded` flag on the corresponding working row, which makes the restore resumable.
The bucket is only read, the snapshot's references and the attempt's catalog rows are untouched.

~~~
catalog (all rows with a 3-day TTL):
  snapshot_remote_locations ('tagalong','dc1')
                            -> endpoint, 'backups', 'bk', state=remote
                               (same key as the backup's row, see Inplace restores)
  snapshot_tables           ('tagalong','ks','t')
                            -> the target table's pre-restore CREATE statement
                               (recovers the original tablet hints after a crash)
  snapshot_sstables         ('tagalong','ks','t','dc1','rack1',
                             first_token, 3gqe…tor)
                            -> state=remote, prefix='bk/sstables/3gqe…tor',
                               downloaded=false
  per downloaded sstable:   UPDATE downloaded=true
                            (the resume point for a retried restore)

disk:
  <data_dir>/ks/t-b00bcafe/me-<fresh generation>-big-*.db   (downloaded components,
                                                             new generations)

bucket: read-only. The attempt's rows ('tagalong/<attempt>') and references: untouched.
~~~

#### Remote tables

a) Snapshot
No data moves at all. The components already live at `bucket/sstables/3gqe…tor/`.
Each node creates one empty reference object per sstable, `bucket/sstables/3gqe…tor/refs/snapshot-<tag>/<attempt>/<gen>`,
which pins the components for as long as it exists (components are deleted only when the sstable's whole refs/ listing is empty, both node refs and snapshot refs).
The catalog records the same rows as in the local case, with `toc_name`/`prefix` pointing into the bucket, and the `snapshots` marker commits the tag.

~~~
bucket:
  scylla-bucket/sstables/3gqe…tor/refs/snapshot-tagalong/<attempt>/3gqe_1lnj_...
      (one empty object per sstable; no data is copied)
disk: nothing (object-storage tables have no local snapshot directories)

catalog: the same writes as the local snapshot, same order, under the attempt
         name 'tagalong/<attempt>', marker last; the sstable row's
         toc_name/prefix point into the bucket
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
      (the clone's node ref, created after verifying that the snapshot
       reference still exists; the clone is undone if the snapshot vanished
       mid-restore)
  no components are copied; a backup elsewhere on the same endpoint is
  byte-copied under a fresh sstable id; a backup on another endpoint is rejected
node-local:
  system.sstables gains the clone's registry entry

catalog: the same staging rows as the local restore
~~~

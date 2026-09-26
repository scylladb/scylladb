# Nodetool viewbuildstatus

#### Versionadded
Added in version 2.3: (experimental)

**viewbuildstatus** - `<keyspace> <view>` Shows the progress of a materialized view build.

For example:

```default
nodetool viewbuildstatus my_keyspace my_view


my_view has not finished building; node status is below.

 Host                Info

 55.40.72.17       STARTED
```

Output options:

**UNKNOWN** - The host doesn’t know about that view.

**STARTED** - The host is building the view.

**SUCCESS** - The host has finished building the view.

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool.md)

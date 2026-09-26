# Nodetool gettraceprobability

**gettraceprobability** - Displays the current trace probability value. This value is the probability for tracing a request.
To change this value see [settraceprobability](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/settraceprobability.md).

For example:

```none
nodetool gettraceprobability
```

returns:

```none
Current trace probability: 0.0
```

## Additional Information

* [settraceprobability](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/settraceprobability.md) - Nodetool Reference
* [CQL tracing in Scylla blog](https://www.scylladb.com/2016/08/04/cql-tracing/)

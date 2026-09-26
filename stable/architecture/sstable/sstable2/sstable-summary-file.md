# SSTable Summary File

SSTable Summary file format

```c
struct summary_entry {
    char key[...]; // variable-length.
    be64 position;
};
```

```c
struct summary_la {
    struct header {
        be32 min_index_interval;
        be32 size;
        be64 memory_size;
        be32 sampling_level;
        be32 size_at_full_sampling;
    } header;
```

```default
    le32 positions[header.size];
    summary_entry entries[header.size];

    be32 first_key_size;
    char first_key[first_key_size];

    be32 last_key_size;
    char last_key[last_key_size];
};
```

[ScyllaDB Architecture](https://opensource.docs.scylladb.com/stable/architecture/index.md)

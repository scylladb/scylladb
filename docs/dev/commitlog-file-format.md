ScyllaDB commitlog segment file format
======================================

**Note:** Commitlog file formats are subject to change between scylla versions. Users should not make assumptions about nor rely on them.
Commitlog files should *never* be used across ScyllaDB updates. This information is provided mainly for ScyllaDB contributors.

File descriptor structure
-------------------------

ScyllaDB commitlog segment files are named with a versioned, time-indexed scheme, as

```
        <Prefix><version>-<id>.log
```

Where `<Prefix>` is application specific, but typically "Commitlog-", or in case of 
files being recycled "Recycled-Commitlog-", `<version>` is the file format version,
and `<id>` is the id part of a replay position (timestamp + shard).

Segment file data structure
---------------------------

All control data is written in network byte order.

The file consists of a file header, followed by any number of chunks. Each chunk has 
its own header + a marker to the start of next chunk, to allow skipping it more easily,
should any data corruption be present in the chunk's data.

Chunks contain data entries, with a small header, stored data + checksums to verify its integrity.

An entry can be a "multi-entry", i.e. several entries written as one.

Version 2
---------

(Format used in ScyllaDB 1.0 to as of this writing - named '2' because it is a slight deviation on the format used in cassandra)

```

        Segment file header

        magic           : uint32_t - ('S'<<24) |('C'<< 16) | ('L' << 8) | 'C';
        version         : uint32_t - same as descriptor
        id              : uint64_t - same as descriptor
        crc             : uint32_t - CRC32 of version, low 32 of id, high 32 of id. 

        Chunk header

        file_pos        : uint32_t - the file position of next chunk
        crc             : uint32_t - CRC32 of low 32 of segment id, high 32 of id and file offset of end of this header.

        Entry

        size            : uint32_t - size of entry (data + full headers). Must be smaller than MAX_UINT32.
        crc1            : uint32_t - CRC32 of size
        data            : bytes - actual entry data
        crc2            : uint32_t - CRC32 of size, data

        Multi-entry

        magic           : multi marker - 0xffffffff (MAX_UINT32)
        size            : size of all entries in this multi-entry + headers
        crc             : CRC32 of magic, size
        <entries> * N
        crc2            : CRC32 of magic, size and data in each entry
```

Version 3
---------

Modified from v2 to improve error detection/false positive elimination.
This version does CRC per written disk block instead of actual entry.
Every block is also tagged with which file is being written, this to
be able to better distinguish new data from that left over from recycling
(reusing old files) and actual disk/file corruption.

```

        Segment file header

        magic           : uint32_t - ('S'<<24) |('C'<< 16) | ('L' << 8) | 'C';
        version         : uint32_t - same as descriptor
        id              : uint64_t - same as descriptor
        alignment       : uint32_t - disk block size
        crc             : uint32_t - CRC32 of version, low 32 of id, high 32 of id, alignment. 

        Chunk header

        file_pos        : uint32_t - the file position of next chunk
        crc             : uint32_t - CRC32 of low 32 of segment id, high 32 of id and file offset of end of this header.

        Entry

        size            : uint32_t - size of entry (data + full headers). Must be smaller than MAX_UINT32.
        crc             : uint32_t - CRC32 of size
        data            : bytes - actual entry data

        Multi-entry

        magic           : multi marker - 0xffffffff (MAX_UINT32)
        size            : size of all entries in this multi-entry + headers
        crc             : CRC32 of magic, size
        <entries> * N

        Disk block (block size = `alignment`)

        0 - <bs - 12>   : Interleaved file data, i.e. the content above
        <bs - 12>       : uint64_t - same as descriptor.
        <bs - 4>        : uint32_t - CRC32 of block data up until crc (bs - 4), including segment id

The main benefit of the tagged and CRC:ed block is that if the CRC is broken, we know the part
of this file is corrupt. If the CRC is correct, but segment ID does not match, we can assume
the file is not fully written/prematurely ended. Both cases can mean data loss, depending on 
how writing is done as well as OS and hardware.

```

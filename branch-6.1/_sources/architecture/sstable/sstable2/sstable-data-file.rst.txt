SSTable Data File
=================

The *data file* contains a part of the actual data stored in the
database. Basically, it is one long list of rows, where each row lists
its key and its columns.

The data file alone does not provide an efficient means to efficiently
find a row with a specific key. For this, the ``SSTables Index File`` and
:doc:`SSTables Summary File </architecture/sstable/sstable2/sstable-summary-file>` exist. Additionally
the ``SSTables Bloom Filter File`` exists
for quickly determining if a specific key exists in this SSTable
(an Apache Cassandra table is written incrementally to several separate SSTables).

The data file may be compressed as described in :doc:`SSTables
Compression </architecture/sstable/sstable2/sstable-compression>`. As
we explain there, the compression layer offers random access to the
uncompressed data, like an ordinary file, so here we can assume the data
file is uncompressed.

This document explains the format of the sstable data file, but glosses
over the question of how higher-level Apache Cassandra concepts - such as
clustering columns, static columns, collections, etc., translate to
sstable data. This is discussed in :doc:`SSTables
interpretation </architecture/sstable/sstable2/sstable-interpretation>`.

The Data File
.............

The data file is nothing more than a long sequence of rows:

.. code:: cpp

    struct data_file {
        struct row[];
    };

The code usually skips directly to the position of a row with a desired
key (using the index file), so we'll want an API to efficiently read
this whole row. We'll probably (TODO: find what uses this...
compaction?) also need an API to efficiently iterate over successive
rows (without rereading the same disk blocks).

Rows
....

    References: **SSTableIdentityIterator.java**, constructor.
    **DeletionTime.java**

Each row begins with a header which includes the row's *key*, if (and
when) it was deleted, followed by a sequence of *cells* (column names
and values) or other types of *atoms* described below. The last atom in
a row is special row-end atom, marking the end of the row.

.. code:: cpp

    struct row {
        be16 key_length;
        char key[key_length];
        struct deletion_time deletion_time;
        struct atom atoms[];
    };

Note that the row definition does not include its length - the reader
reads the row incrementally until reaching the row-end atom.
Alternatively, if we want to read an entire row into memory before
parsing it, we can figure out its length using the ``SSTables Index File``. If we
reached this row from a particular index entry, the next index entry
will point to the byte after the end of this row.

If we reached a particular row through the index, we may already know we
have the right key, and can skip the key at the beginning of the row
without bothering to deserialize it.

The deletion\_time structure determines whether this is a row tombstone
- i.e., whether the whole row has been deleted, and if so, when:

.. code:: c

    struct deletion_time {
        be32 local_deletion_time;
        be64 marked_for_delete_at;
    };

The special value LIVE = (MAX\_BE32, MIN\_BE64), i.e., the bytes 7F FF
FF 80 00 00 00 00 00 00 00, is the default for live, undeleted, rows.
``marked_for_delete_at`` is a timestamp (typically in microseconds since
the UNIX epoch) after which data should be considered deleted. If set to
MIN\_BE64, the data has not been marked for deletion at all.
``local_deletion_time`` is the local server timestamp (in *seconds*
since the UNIX) epoch when this tombstone was created - this is only
used for purposes of purging the tombstone after gc\_grace\_seconds have
elapsed.

Atoms (cells, end-of-row, and more)
...................................

    References: **OnDiskAtom.java**:deserializeFromSSTable(),
    **ColumnSerializer**:deserializeColumnBody(),
    **RangeTombstone**:deserializeBody().

A row's value is a list of *atoms*, each of which is usually a cell (a
column name and value) or an end-of-row atom, but can also be additional
types of atoms as explained below.

Each atom, of any type begins with a column name, a byte string with
16-bit length. If the length of the name is 0 (in other words, the atom
begins with two null bytes), this is an end-of-row atom, as the other
atom types always have non-empty names. Note that, yes, the column names
are repeated in each and every row. The compression layer eliminates
much of the disk-space waste, but the overhead of parsing this verbose
encoding remains.

.. code:: cpp

    struct atom {
        be16 column_name_length;
        char column_name[column_name_length];
    }

If the atom has a non-empty name, it is *not* an end-of-row, and
following column\_name appears a single byte *mask*:

.. code:: cpp

    enum mask {
        DELETION_MASK        = 0x01,
        EXPIRATION_MASK      = 0x02,
        COUNTER_MASK         = 0x04,
        COUNTER_UPDATE_MASK  = 0x08,
        RANGE_TOMBSTONE_MASK = 0x10,
    };

.. code:: cpp

    struct nonempty_atom : atom {
        char mask;
    }

The mask determines which type of atom this is:

If mask & (RANGE\_TOMBSTONE\_MASK \| COUNTER\_MASK \| EXPIRATION\_MASK)
== 0, we have a regular cell. This has a 64-bit timestamp (can be used
to decide which value of a cell is most recent), and a value, serialized
into a byte array with 32-bit length:

.. code:: cpp

    struct cell_atom : nonempty_atom {
        be64 timestamp;
        be32 value_length;
        char value[value_length];
    };

(Note: The COUNTER\_UPDATE\_MASK and DELETION\_MASK might be turned in
for cell\_atom, modifying its meaning).

if mask & RANGE\_TOMBSTONE\_MASK, we have a

.. code:: cpp

    struct range_tombstone_atom : nonempty_atom {
        u16 last_column_length;
        char last_column_name[last_column_length];
        struct deletion_time dt;
    };

Such a range-tombstone atom effects not just the single column
column\_name, but the range between column\_name and last\_column\_name
(as usual, this range is defined using the underlying comparator of the
column name type).

if mask & COUNTER\_MASK, we have a

.. code:: cpp

    struct counter_cell_atom : nonempty_atom {
        be64 timestamp_of_last_delete;
        be64 timestamp;
        be32 value_length;
        char value[value_length];
    };

if mask & EXPIRATION\_MASK, we have a

.. code:: cpp

    struct expiring_cell_atom : nonempty_atom {
        be32 ttl;
        be32 expiration;
        be64 timestamp;
        be32 value_length;
        char value[value_length];
    };

Note that it is not valid to have more than one RANGE\_TOMBSTONE\_MASK,
COUNTER\_MASK or EXPIRATION\_MASK on the same atom.

Name and Value Serialization
............................

    References: **Composite.java**, **CompositeType.java**.

It is important to remember that both column *names* and *values*
described above are stored as a byte strings (preceded by its length,
16-bit or 32-bit). But in Apache Cassandra, both names and values may have
various types (as determined by the CQL schema), and those are
**serialized** to a byte string before the byte string is serialized to
disk as part of the atom.

This has a surprising effect on the encoding of column names in the data
file. Starting with Apache Cassandra 1.2, unless the table is created "WITH
compact storage", column names are always *composite*, i.e., a sequence
of components. A composite column name is serialized to a byte array
like this:

.. code:: cpp

    struct serialized_composite_name {
         struct {
             be16 component_length;
             char[] component; // of length component_length
             char end_of_component;        // usually 0, can be -1 (\0xff) or 1 (\0x01) - see below.
         } component[];
    }

Then ``end_of_component`` is usually 0, but can also be -1 or 1 for
specifying not a specific column but ranges, as explained in comments in
Composite.java and CompositeType.java.

So the surprising result is that even single-component column names
produce wasteful double-serialization (unless the tables has WITH
compact storage): For example, the column name "age", a composite name
with just one component, is first serialized into ``\0 \3 a g e \0`` and
then this serialized string is written as the column name, preceded by
its own length, 6: ``\0 \6 \0 \3 a g e \0``.

Note that the above means we need to know, when reading an sstable,
whether column names are composite or not. Therefore the sstable reader
need to know if this table has "WITH compact storage" or not.

CQL Row Marker
..............

In some cases (namely, tables built through CQL without "WITH compact
storage"), each row will contain a bizarre extra cell called a "CQL Row
Marker" which the Apache Cassandra developers (who apparently don't care about
wasting space...) added to allow a row to remain even if all its columns
are deleted. It's worth knowing that this extra cell exists, as its
existence might surprise the uninitiated.

The "CQL Row Marker" is a normal cell in a row, which has an "empty
composite" name and an empty value. Note that the cell's column name is
**not** empty - it can't be (an empty name is an end-of-row marker).
Rather, it is a composite name with one empty-string component. Such a
composite name is serialized, as explained above, to ``\0 \0 \0`` - the
first two null bytes are the empty component's length, and at the end we
have the additional null added in the serialization. This
three-null-bytes is what gets used as the column name

.. include:: /rst_include/architecture-index.rst

.. include:: /rst_include/apache-copyrights.rst

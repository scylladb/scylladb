SSTable Interpretation
======================

.. role:: raw-latex(raw)
   :format: latex
..

**Topic: Internals**

**Audience: Devops professionals, architects**

The SSTables Data File contains rows of data. This document discusses
how to interpret the various fields described in :doc:`SSTables Data File </architecture/sstable/sstable2/sstable-data-file/>` in the context of ScyllaDB, and how to
convert this data into ScyllaDB's native data structure:
**mutation\_partition**.

SSTable Rows
............

Each row in the SSTable isn't necessarily a full row of data. Rather, it
is just a **mutation**, a list of changed (added or deleted) columns and
their new values (or "tombstone" for a deleted column), and a timestamp
for each such change (this timestamp is used for reconciling conflicting
mutations). The full data row needed by a request will be composed from
potentially multiple sstables and/or the in-memory table(s).

As we'll explain below when discussing clustering columns, the best term
for what we read from one row in the SSTable isn't a "row", but rather a
**partition**.

For these reasons, ScyllaDB's internal representation for a row we read
from the SSTable is called ``class mutation_partition``.

Column Names
............

As explained in :doc:`SSTables Data File </architecture/sstable/sstable2/sstable-data-file>`, the
sstable row (a mutation partition) is a list of *cells* (column values).
Each cell is preceded by the full column name. This was considered a
good idea when Apache Cassandra was designed to support rows with many and
arbitrary columns, but ScyllaDB is more oriented toward the CQL use case
with a known schema. So ScyllaDB's rows do not store the full column name,
but rather store a numeric ID which points to the known list of columns
from the schema. So as we read column names from the SSTable in form of IDs, 
we need to translate the IDs into names by looking them up in the schema.

Composite Column Names
......................

But the column names mentioned in the sstable cells usually aren't a
field name mentioned in the schema, and additional processing needs to
be done to the column names which we read from the SSTable.

The first issue is that starting with Apache Cassandra 1.2 (and unless
``WITH COMPACT STORAGE`` is used), column names aren't plain strings,
but each is rather rather a *composite* name - a short list of name
components. See :doc:`SSTables Data File </architecture/sstable/sstable2/sstable-data-file>` for this
list's on-disk encoding, but for simplicity of exposition let's use the
convention of writing a composite name as **(part1:part2:...)**

Things are simplest when no clustering keys are involved; Then, cell
names have just one component. For example, consider the table created
with the following CQL command:

::

    CREATE TABLE harels (
            name text,
            age int,
            PRIMARY KEY (name)
    );
    INSERT INTO harels (name, age) VALUES ('nadav', 40);

In this table, we have a row with the key "nadav", and in this row, one
cell, with the column name "age" encoded as a single-component composite
string **(age)** (on disk, ``\003 a g e \0``). This only component,
"age", can be looked up in the table's schema, and converted to a column
ID in the ``mutation_partition``, as explained above.

CQL Row Marker
..............

As explained in :doc:`SSTables Data File </architecture/sstable/sstable2/sstable-data-file/>`,
Apache Cassandra always (except when a COMPACT table is used and other esoteric
exceptions) adds to each row an empty-named and empty-valued bogus
"cell", to solve various problems involving finding a row after its only
column has been deleted. This is also explained in a comment in
UpdateStatement.Java, and in
https://issues.apache.org/jira/browse/CASSANDRA-4361.

For example, if we inspect with ``tools/bin/sstable2json`` the table
created in the previous example, we find:

::

    {"key": "nadav",
     "cells": [["","",1426688662900463],
               ["age","40",1426688662900463]]}

The first cell, with the empty name ("") and value (the second ""), is
the "CQL Row Marker". As usual, the empty name "" shown by sstable2json
is not actually an empty string, but a composite with one empty
component, **()** (serialized on disk as ``'\000 \000 \000'``).

I hope we can simply ignore these CQL Row Marker cells, and not
duplicate them in ScyllaDB's internal format. We just need a different way
to allow empty rows (a row with only a key, but no data columns) to
exist, to circumvent the problems mentioned in CASSANDRA-4361 and the
comment in UpdateStatement.Java.

Clustering Keys
...............

When the table has a clustering key, column names in the sstable no
longer have a single component:

::

    USE try1;

::

    CREATE TABLE harels2 (
            name text,
            nick text,
            age int,
            PRIMARY KEY (name, nick)
    ) WITH compression = {};

::

    INSERT INTO harels2 (name, nick, age) VALUES ('nadav', 'nyh', 40);

Note how name and nick form the primary key, but the CQL syntax
specifies that the **partition key** is name, and the **clustering key**
is nick. This means that different tables entries that have the same
name (but different nick) will appear in the same partition, i.e., in
the same sstable row. Inside that partition, different nicks can appear,
each with its own age. To see what this looks like in the sstable we
again use the *sstable2json* tool, and see:

::

    {"key": "nadav",
     "cells": [["nyh:","",1427032626839065],
               ["nyh:age","40",1427032626839065]]}

In other words, the composite column name (nyh:age) is used to store the
age for the nick nyh, and a different column would be used to store some
other nick's age. Note how in (nyh:age), the "nyh" is not one of the CQL
column names, but rather the *value* of the clustering column nick, and
only the last component, "age", is an actual name of a field from the
CQL schema.

In ScyllaDB nomenclature, this single **partition** (with key
name="nadav") has multiple **rows**, each with a different value of the
clustering key (nick). Each of these rows has, as usual, columns whose
names are the fields from the CQL schema (and as explained above, are
kept as column IDs, not names).

So when converting an sstable row into a ``mutation_partition``, we need
to consult the schema to look for a clustering key. If "nick" is the
clustering key, we shouldn't look as usual for cells named (nick).
Instead, we expect *every* cell name to have >=2 components, where the
first component is a value of nick, and the second component an actual
column name. In the ``mutation_partition`` object, we need to insert
multiple ``row`` objects, where each row corresponds to one value of the
first component.

The silly cell with key **(nyh:)** (empty second component) and empty
value is the "CQL Row Marker" described above, which appears for each
*row* (combination of partition key and clustering key) separately.

Static Columns
..............

A static column is a special column which is shared by all rows of the
same partition. As we saw above, the case of multiple rows per partition
happens when there is a clustering column. When Datastax introduced
static columns in Cassandra 2.0.6, they used the following example
(http://www.datastax.com/dev/blog/cql-in-2-0-6):

::

    CREATE TABLE bills (
         user text,
         balance int static,
         expense_id int,
         amount int,
         PRIMARY KEY (user, expense_id)
      );

This is a table of bills (amounts that certain users need to pay).
According to the "PRIMARY KEY" line, the partition key is "user", and
"expense\_id" is the clustering key; This means that there will be a
partition (sstable row) for each user, and for each such partition, we
can have multiple expenses (*rows*), each with a different
clustering-key expense\_id, and corresponding amount. But the "balance"
column is for all the different expenses of the same user.

So if we insert one expense for 'user1', and set user1's balance::

    INSERT INTO bills (user, balance) VALUES ('user1', 17);
    INSERT INTO bills (user, expense_id, amount) VALUES ('user1', 1, 8);

What is written to the sstable looks like this (as usual, output from
``sstable2json``):

::

    {"key": "user1",
     "cells": [[":balance","17",1428849747953348],
               ["1:","",1428849747970947],
               ["1:amount","8",1428849747970947]]}

The (1:amount) and (1:) is what we already saw above, the new thing here
is the (:balance), a static column.

So sstables have static columns specially marked by an empty first
component of the composite cell name. We need to verify that each such
cell actually corresponds to a known static column from the table's
schema, and collect all these static columns into one row
(``_static_row``) stored in ScyllaDB's ``mutation_partition``.

TODO: CompositeType.java explains that static columns do not really have
an empty first component (size 0), but rather the first component has
the fake size STATIC\_MARKER = 0xFFFF (65536). We need to verify this.

Compound Clustering Key
.......................

When the clustering key is compound, i.e., composed of multiple columns,
the SSTable column names will contain more than two components. For
example consider:

::

    USE try1;
    CREATE TABLE bills3 (
         user text,
         expense_id int,
         year int,
         amount int,
         PRIMARY KEY (user, year, expense_id)
    );

::

    INSERT INTO bills3 (user, year, expense_id, amount) VALUES ('user1', 2015, 1, 8);

Here as usual, the first column name in "PRIMARY KEY", user, is the
partition key, but the two others, year and expense\_id are both
clustering columns, forming a compound clustering key. I.e., each
partition contains several rows, each defined by (and sorted by) the
pair (year, expense\_id).

The resulting SSTable row is:

::

    {"key": "user1",
     "cells": [["2015:1:","",1428853746711253],
               ["2015:1:amount","8",1428853746711253]]}

Note how the column name "amount" is now prefixed by two components, the
values of the two clustering columns. Of course, a schema can have any
number of clustering columns and as a result, expect that number of
prefix components in the SSTable's column names.

As before, all but the last column-name component are expected to be
values of the clustering-key of the various rows inside the partition,
and only the last component is a column name to be looked up in the
schema. It's safer, though, to consult with the schema to see the number
of clustering columns instead of guessing it as the number of components
minus one. This is a good sanity check, and also necessary when
collections are concerned (see below).

Compound Partition Key
......................

The row key read from the SSTable can also be a composite (a list of
components) if the schema says the partition key is compound. For
example:

::

    CREATE TABLE bills2 (
         user text,
         expense_id int,
         amount int,
         PRIMARY KEY ((user, expense_id))
    );
    INSERT INTO bills (user, expense_id, amount) VALUES ('user1', 1, 8);

Note the extra pair of parenthesis in the "PRIMARY KEY" specification,
which says that expense\_id is part of the partition key, not a
clustering ey.

The key of each SSTable row is now the pair (user, expense\_id), a
composite with two components.

TODO: Print the resulting SSTable

Collections
...........

The encoding of *collections* in SSTables is more complex.

Consider this table definition with a column which is a *set*
collection:

::

    CREATE TABLE col2 (
         user text,
         favorites set<text>,
         PRIMARY KEY (user)
    );

::

    INSERT INTO col2 (user, favorites) VALUES ('user1', {'raindrops', 'kittens'});

The resulting SSTable row is:

::

    {"key": "user1",
     "cells": [["","",1428855312063525],
               ["favorites:_","favorites:!",1428855312063524,"t",1428855312],
               ["favorites:6b697474656e73","",1428855312063525],
               ["favorites:7261696e64726f7073","",1428855312063525]]}

Here we also have two components in the column names, but we need to
know this is not the case of a clustering key ("favorites" isn't a value
of a clustering column) but that of a collection. We need to consult the
schema to tell the two cases apart (in this case, there are no
clustering columns, so no component needs to be treated as a clustering
column, so when we see two components, it must be a collection).

In a set, we have a cell for each item in the collection, and *second*
component of the cell's name is the serialized value. E.g., in our case,
the byte array "kittens" is shown by sstable2json in hex
(6b697474656e73) - in the actual SSTable the hex does not appear (there
is the length of the string followed by the actual bytes). The value of
each of these cells is empty for a *set* (for other types of collections
it is not empty - see below).

The weird cell in the beginning of the above sstable2json output (with
``favorites:_``) is not a normal cell - this is how sstable prints a
*range tombstone*, whose range is between the start of "favorites:" and
the end of "favorites:", the markedForDeleteAt value is 1428855312063524
and localDeletionTime is 1428855312. The need for this range tombstone
appears to be as follows: Because each of the collection's items is a
separate cell, when we *set* the collection (as in the INSERT command we
used) the intention is to delete any old item in the collection, if
there are any, and add the new items. The range tombstone takes care of
deleting all the old items.

The actual sstable doesn't have the "\_" or "!" characters printed by
sstable2json. What it really has is ``"\00\09favorites\ff"`` and
:raw-latex:`\0`0:raw-latex:`\0`9favorites:raw-latex:`\0`1". I.e., each
of these two column names has only one component, but instead of ending
as usual with the end-of-component byte :raw-latex:`\0`0, the first ends
with :raw-latex:`\ff `(START) and the second ends with :raw-latex:`\0`1
(END). This means the range tombstone spans everything between the first
column and the last, as indeed desired.

The second type of collection, the **map**, is similar to the **set**,
just the value is not empty but rather the desired value in the map. For
example,

::

    CREATE TABLE col4 (
         user text,
         favorites map<text,int>,
         PRIMARY KEY (user)
    ) WITH compression = {};
    INSERT INTO col4 (user, favorites) VALUES ('user1', {'raindrops' : 1, 'kittens' : 2});

We see in the SSTable with sstable2json:

::

    {"key": "user1",
     "cells": [["","",1428864848550739],
               ["favorites:_","favorites:!",1428864848550738,"t",1428864848],
               ["favorites:6b697474656e73","00000002",1428864848550739],
               ["favorites:7261696e64726f7073","00000001",1428864848550739]]}

I.e., indeed exactly the same as the representation of the set, except
the cell's values are the desired 1 and 2. The way the values are
printed above as strings ("0000002") is just an artifact of how
sstable2json works - the value is represented in the SSTable as an
actual serialized int (32-bit length 4, followed by 4 bytes of the
integer's representation) as expected.

For the ordered **list** collection, things are similar, but not quite
the same because of the need to keep the desired item order:

::

    CREATE TABLE col1 (
         user text,
         favorites list<text>,
         PRIMARY KEY (user)
    );
    INSERT INTO col1 (user, favorites) VALUES ('user1', ['raindrops', 'kittens']);

The resulting SSTable row is now:

::

    {"key": "user1",
     "cells": [["","",1428854738475900],
               ["favorites:_","favorites:!",1428854738475899,"t",1428854738],
               ["favorites:c2bcd290e12d11e49cac000000000000","7261696e64726f7073",1428854738475900],
               ["favorites:c2bcd291e12d11e49cac000000000000","6b697474656e73",1428854738475900]]}

Note how this time, the items ("raindrops" and "kittens") are the value
of the cell, not in the column name. In the column name we have some
long strings intended to sort in the requested list order. These long
hex strings are misrepresented by sstable2json - they are not a hex
string but rather a 16-byte UUID.

To merely keep the list items in order, Apache Cassandra could have used small
integers instead of these UUIDs. But these UUIDs have an additional
benefit: as http://www.datastax.com/dev/blog/cql3\_collections explains,
Apache Cassandra wishes to allow efficient *append* and *prepend* operations to
an existing list - without reading the existing list first (i.e., the
append/prepend is a fast write-only mutation, not a slow
read-modify-write operation). To achieve that, Apache Cassandra uses signed
time-UUIDs as the list sort string - with positive times used for append
operations, and negative for prepend operations. This ensures that, for
example, a later append always sorts after an earlier append - without
the append having to know which items already exist in the list.

ScyllaDB's internal storage of a collection in a mutation is the
``class collection_mutation``, and we need to convert the above
described representation into that class. TODO: I still can't figure out
exactly what is the internal structure of our collection\_mutation
(which hides behind an opaque byte array), or what functions we are
supposed to call to build one).

Cells with Expiration Time
..........................

An SSTable cell may have an expiration time, as explained
http://docs.datastax.com/en/cql/3.1/cql/cql\_using/use\_expire\_c.html.
Such a cell is marked by the EXPIRATION\_MASK bit in the mask byte, and
in addition to the cell's normal fields, has two additional fields,
"ttl" and "expiration", both 32-bit and measured in seconds. "ttl" is
the original time-to-live (number of seconds until expiration) specified
when the cell was created, and "expiration" is the absolute time when
the cell should be expired (in seconds since the Unix Epoch).

The following CQL example creates a cell with a time-to-live of 3600
seconds:

::

    CREATE TABLE ttl (
        name text,
        age int,
        PRIMARY KEY (name)
    );
    INSERT INTO ttl (name, age) VALUES ('nadav', 40) USING TTL 3600;

sstable2json prints the resulting SSTable row as:

::

    {"key": "nadav",
     "cells": [["","",1430151018675502,"e",3600,1430154618],
               ["age","40",1430151018675502,"e",3600,1430154618]]}

Note how each of cells (the cql row marker, and our actual data cell)
have a TTL of 3600 seconds, and an expiration time calculated by adding
3600 to the current time on the server. The fact that the row marker
also got this TTL is not certain to be a good thing - see discussion in
https://issues.apache.org/jira/browse/CASSANDRA-5762.

Cell Tombstone
..............

We've already discussed row tombstones (marking the deletion of an
entire row) and range tombstone (marking the deletion of a range of
columns). We can also have a cell tombstone, marking the deletion of a
single cell. A deleted cell is encoded in the SSTable like a normal
cell, except the mask has the DELETION\_MASK bit, and the *value* of the
cell contains the serialized ``local_deletion_time`` (the local server
time in seconds since the epoch), which is probably only needed for the
purpose of purging the tombstone after gc\_grace\_seconds have elapsed.

To create an sstable with a deleted cell in Apache Cassandra, consider creating
a table with some data:

::

    CREATE TABLE deleted (
        name text,
        age int,
        PRIMARY KEY (name)
    );
    INSERT INTO deleted (name, age) VALUES ('nadav', 40);

Then flushing this data to an SSTable (with
``bin/nodetool flush keyspacename``), and then deleting the cell we
added:

::

    DELETE age FROM deleted WHERE name = 'nadav';

When the second SSTable is flushed, it will have a cell tombstone.
sstable2json shows the second SSTable like this:

::

    {"key": "nadav",
     "cells": [["age",1430200516,1430200516937621,"d"]]}

Note how the cell has the DELETION\_MASK bit (written as a "d"), its
"value" is the local-deletion-time, 1430200516, and as usual it has a
timestamp

.. include:: /rst_include/architecture-index.rst

.. include:: /rst_include/apache-copyrights.rst


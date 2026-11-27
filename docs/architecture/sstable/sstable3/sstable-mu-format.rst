.. _sstable-mu-format:

SSTable mu Format: Physical Index Positions
============================================

The ``mu`` format is the ``mt`` format with five changes. The first three
apply only to compressed SSTables:

* Each compressed chunk in ``Data.db`` stores its own length, in a header and
  in a footer.
* The index components (``Partitions.db`` and ``Rows.db``) point to positions
  in the compressed file, not in the uncompressed data.
* ``CompressionInfo.db`` stores only a random sample of the chunk offsets.
* Each page of the index components ends with a footer. The footer holds a
  CRC32 of the page and a table of the ``Data.db`` chunks that the page refers
  to.
* ``Scylla.db`` always contains the ``incompatible_features`` subcomponent.

With the first three changes, a reader can find any row without the chunk
offsets from ``CompressionInfo.db``. Thus, it does not need to keep those
offsets in memory.

An uncompressed ``mu`` SSTable differs from an uncompressed ``mt`` SSTable in
``Scylla.db`` and in the page footers of ``Partitions.db`` and ``Rows.db``.
All components not described on this page are identical to ``mt``.

Notation and parameters
-----------------------

* ``L`` is the uncompressed chunk length, in bytes. It is the ``chunk_length``
  field of ``CompressionInfo.db``. It is a power of two.
* ``bit_width(x)`` is the number of bits needed to represent the unsigned
  integer ``x``. ``bit_width(0)`` is 0.
* ``ceil_div(a, b)`` is ``a / b`` rounded up to an integer.
* ``vint_size(x)`` is the number of bytes of the unsigned integer ``x`` in the
  variable-length integer (vint) encoding of the SSTable 3.x format: 1 byte if
  ``x < 2^7``, 2 bytes if ``x < 2^14``, 3 bytes if ``x < 2^21``, and so on.

All other quantities derive from ``L``:

``D_max = L + floor(L / 2)``
  The maximum length of the compressed data of one chunk.

``F_max = vint_size(D_max)``
  The maximum size, in bytes, of a chunk-length field.

``N_max = ceil_div(511 + D_max + 2 * F_max + 4, 512)``
  The maximum number of 512-byte blocks that one chunk can touch.

``W_N = bit_width(N_max - 1)``
  The width, in bits, of a block-count field.

``W_O = log2(L)``
  The width, in bits, of an offset-within-chunk field. An offset within a
  chunk is less than ``L``, so it fits in ``W_O`` bits.

The table lists these values for the chunk lengths that a table schema allows:

.. list-table::
   :header-rows: 1

   * - ``L`` (KiB)
     - ``D_max``
     - ``F_max``
     - ``N_max``
     - ``W_N``
     - ``W_O``
   * - 1
     - 1536
     - 2
     - 5
     - 3
     - 10
   * - 2
     - 3072
     - 2
     - 8
     - 3
     - 11
   * - 4
     - 6144
     - 2
     - 14
     - 4
     - 12
   * - 8
     - 12288
     - 2
     - 26
     - 5
     - 13
   * - 16
     - 24576
     - 3
     - 50
     - 6
     - 14
   * - 32
     - 49152
     - 3
     - 98
     - 7
     - 15
   * - 64
     - 98304
     - 3
     - 194
     - 8
     - 16
   * - 128
     - 196608
     - 3
     - 386
     - 9
     - 17

Data.db: framed chunks
----------------------

``Data.db`` is a sequence of chunks, starting at offset 0, with no gaps between
them. Each chunk has this layout::

    [ header ][ compressed data ][ checksum ][ footer ]

``header``
  ``F = vint_size(D)`` bytes. The length ``D`` of the compressed data, as an
  unsigned vint. The first byte starts with ``F - 1`` one bits and a zero bit.
  The other bits of the first byte, followed by the other ``F - 1`` bytes, hold
  ``D`` as a big-endian integer. The header must be the shortest encoding of
  ``D``, so ``F`` is at most ``F_max``.

``compressed data``
  ``D`` bytes. ``D`` is at most ``D_max``. A writer must not write a longer
  chunk. A reader must treat a larger ``D`` as corruption.

``checksum``
  4 bytes. The CRC32 of the header and the compressed data, as an unsigned
  big-endian integer. It does not cover the footer.

``footer``
  ``F`` bytes. The bytes of the header, in reverse order. Thus, the last byte
  of the footer is the first byte of the header.

The *extent* of a chunk is its size in ``Data.db``: ``D + 2 * vint_size(D) + 4``
bytes.

As in ``mt``, every chunk except the last decompresses to exactly ``L`` bytes.
The last chunk decompresses to at most ``L`` bytes.

A reader at the start of a chunk uses the header to find the end of the chunk.
The first byte of the header gives ``F``. A reader at the end of a chunk uses
the footer to find the start of the chunk. The last byte of the footer gives
``F``. A reader must treat a frame longer than ``F_max`` bytes, or a frame that
is not the shortest encoding of its value, as corruption.

A footer is not covered by a checksum. Instead, a reader must check that the
footer holds the same value as the header, and treat a mismatch as corruption.
A reader that moves backwards uses the footer before it can check it. If the
footer is corrupt, the reader computes a wrong chunk start. Then the header at
that position does not match the footer, or the checksum of the data does not
match.

``Digest.crc32`` holds the CRC32 of the whole ``Data.db`` file, including the
headers and footers.

CompressionInfo.db: sampled chunk offsets
-----------------------------------------

In ``mt``, ``CompressionInfo.db`` stores the offset of every chunk. In ``mu``,
it stores a random sample of the chunk offsets. The sample lets a reader pick
random chunks of ``Data.db``, for example to train a compression dictionary.
The layout is (integers are big-endian)::

    name:                 string (uint16 length, then the bytes)
    options:              uint32 count, then count pairs of strings
    chunk_length:         uint32
    data_length:          uint64
    sampling_denominator: uint32
    sample_size:          uint32
    sampled_offsets:      sample_size times uint64

The fields up to ``data_length`` are the same as in ``mt``.

``sampling_denominator``
  ``S``. The writer keeps each chunk in the sample with probability ``1 / S``,
  independently of the other chunks, but it leaves out the chunks near the end
  of the data. It always leaves out a last chunk that decompresses to fewer than
  ``L`` bytes, so every sampled chunk decompresses to ``L`` bytes. ``S`` is not
  0. The current writer uses ``S = 64``, and leaves out each chunk that starts
  less than ``max(16 KiB, L)`` bytes before the end of the uncompressed data.

``sample_size``
  The number of sampled offsets. It is not the number of chunks. The number of
  chunks is ``ceil_div(data_length, L)``.

``sampled_offsets``
  The offsets in ``Data.db`` of the headers of the sampled chunks. They
  increase strictly, and each is less than the size of ``Data.db``.

Every chunk that isn't near the end of the data has the same probability to be
sampled. So a uniform choice among the sampled offsets of an SSTable is a
uniform choice among those chunks. To combine SSTables with different ``L`` or
``S``, weight each sampled offset by ``S * L`` bytes of uncompressed data. A
reader that reads more than ``L`` bytes from a sampled offset must expect the
data to end first.

Without all chunk offsets, a reader can't translate a position in the
uncompressed data to a position in ``Data.db``. A reader must get physical
positions from the index, from a sampled offset, or from the headers and
footers of the chunks.

Physical positions
------------------

The index of a compressed ``mu`` SSTable identifies a position in ``Data.db``
with three values:

``C`` (chunk position)
  The offset in ``Data.db`` of the header of the chunk that holds the
  position.

``O`` (offset within chunk)
  The offset of the position in the decompressed chunk. It is less than the
  decompressed length of the chunk.

``N`` (extent hint)
  The number of 512-byte blocks that the chunk touches. It gives an upper
  bound of the chunk's extent. Blocks are aligned to offset 0 of ``Data.db``.
  Thus,
  ``N = ceil_div((C mod 512) + extent, 512)``, and ``1 <= N <= N_max``.

A reader can load the whole chunk with one read of the byte range from ``C`` to
``C - (C mod 512) + 512 * N``. For the last chunk, this range can extend past
the end of ``Data.db``, so the reader must cut it off at the file size. A reader
must still take the chunk length from the header. It must use ``N`` only to
size its first read.

``N`` and ``O`` are often stored together, as one unsigned integer ``LO`` of
``W_N + W_O`` bits::

    LO = ((N - 1) << W_O) | O

Index pages
-----------

``Partitions.db`` and ``Rows.db`` are divided into pages of 4096 bytes, as in
``mt``. In ``mu``, every page ends with a chunk table and a footer::

    [ trie nodes or raw bytes ][ zero padding ][ table: slot k-1 ... slot 0 ][ count ][ crc32 ]

``count``
  The number of slots in the table, as an unsigned 16-bit big-endian integer.
  It is 0 in ``Rows.db``, and in the ``Partitions.db`` of an uncompressed
  SSTable.

``crc32``
  The CRC32 of all bytes of the page before it, as an unsigned 32-bit
  big-endian integer. A reader must check it before it uses the page, and
  treat a mismatch as corruption.

``table``
  ``count`` entries of 5 bytes each. Slot ``s`` is at offset
  ``4096 - 6 - 5 * (s + 1)`` in the page. Each entry is an unsigned 40-bit
  big-endian integer that holds ``C`` in its high ``40 - W_N`` bits, and
  ``N - 1`` in its low ``W_N`` bits. A reader must treat an entry with
  ``N > N_max`` as corruption.

  Thus, a table can only hold chunks with ``C < 2^(40 - W_N)``. For example,
  this is 64 GiB for ``L = 4 KiB``, and 2 GiB for ``L = 128 KiB``. Partitions in
  later chunks use the inline form of the partition payload.

The *usable area* of a page is its first ``4096 - 6 - 5 * count`` bytes. Trie
nodes and the other data of the file are only in usable areas. No trie node
crosses the end of a usable area.

Every page has a footer, also the last one. The writer pads the last page to
its full size. Thus, the size of each file is a multiple of 4096. An empty file
has no pages.

Raw data
~~~~~~~~

Some data in the index files is not trie nodes: the row index headers in
``Rows.db``, and the trailer at the end of ``Partitions.db``. This data can be
longer than a page. It is written as a stream of bytes that fills the usable
area of a page, then continues at the start of the next page. Thus, it skips
the chunk table and the footer of each page.

A stored position of such data (for example, the position of a row index header
in a ``Partitions.db`` payload) always points at the byte that it names. If the
data before it ends exactly at the end of a usable area, the position is the
start of the next page, never the start of the chunk table.

Rows.db: row index header
-------------------------

In ``mt``, the header of a partition's row index stores the position of the
partition in ``Data.db`` as one unsigned vint. In ``mu``, this field holds the
physical position of the start of the partition:

* ``C``, as an unsigned vint.
* ``LO``, as an unsigned big-endian integer of ``ceil_div(W_N + W_O, 8)`` bytes.

The other header fields are unchanged. The base of the trie root delta is
still the position of the first byte of the position field. The header is raw
data (see `Raw data`_), so it can span pages, and this base can be on a later
page than the start of the header.

Partitions.db: trailer
----------------------

``Partitions.db`` ends with a trailer, as in ``mt``: the first and the last
partition key (each a 16-bit length and the bytes), then a fixed tail of three
unsigned 64-bit big-endian integers: the position of the first key, the number
of partitions, and the position of the trie root.

In ``mu``, the keys are raw data (see `Raw data`_). The fixed tail never spans
pages. It ends exactly at the start of the chunk table of the last page. A
reader finds it from the ``count`` of the last page. Zero padding fills the gap
between the keys and the tail.

Trie payloads
-------------

A node of a BTI trie has a 4-bit payload field. In ``ms`` and ``mt``, this field
holds a byte count together with a flag. In ``mu``, the field holds only a byte
count. A value of 0 still means that the node has no payload.

Rows.db: row index payload
~~~~~~~~~~~~~~~~~~~~~~~~~~

The payload points to the start of a block of rows. Let ``C_p`` be the chunk
position from the row index header, and let ``(C, N, O)`` be the position of
the block. The writer stores ``delta_C = C - C_p``, which is never negative.

Let ``T`` be 1 if the block is preceded by an open range tombstone, and 0
otherwise. The payload is:

* The value ``(delta_C << (W_N + W_O + 1)) | (LO << 1) | T``, as an unsigned
  big-endian integer of ``P`` bytes. The 4-bit payload field holds ``P``.
  The writer sets ``P = ceil_div(bit_width(delta_C) + W_N + W_O + 1, 8)``.
* If ``T`` is 1, the range tombstone, as in ``mt``: the 8-byte
  ``marked_for_delete_at`` followed by the 4-byte ``local_deletion_time``,
  both big-endian.

``N`` is computed from the absolute chunk position ``C``, not from ``delta_C``.

Partitions.db: partition index payload
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let ``P`` be the size of the payload in bytes. The 4-bit payload field holds
``P``. The payload is:

* One hash byte. It has the same value as in ``mt``.
* A value ``V``, as a big-endian integer of ``K = P - 1`` bytes.

The top bits of ``V`` are a prefix code, which selects the target:

.. list-table::
   :header-rows: 1

   * - Top bits
     - Target
     - Rest of ``V``
   * - ``0``
     - ``Rows.db``
     - The position of the row index header
   * - ``10``
     - ``Data.db``, inline
     - ``C``, then ``LO`` in the low ``W_N + W_O`` bits
   * - ``11``
     - ``Data.db``, through the chunk table
     - ``O``, then a slot in the low 9 bits

* If the top bit is 0, the partition has a row index. ``V`` is the offset in
  ``Rows.db`` of the row index header. The writer uses the smallest ``K`` that
  leaves the top bit 0.
* If the top bits are ``10``, the partition has no row index. ``V`` points to
  the start of the partition in ``Data.db``. Let ``(C, N, O)`` be that position.
  The low ``W_N + W_O`` bits of ``V`` hold ``LO``. The bits between ``LO`` and
  the top bits hold ``C``. The writer sets
  ``K = ceil_div(bit_width(C) + 2 + W_N + W_O, 8)``, and fills the unused high
  bits with 0.

  To decode ``C``, clear the top two bits of ``V`` and shift the result right
  by ``W_N + W_O`` bits.
* If the top bits are ``11``, the partition has no row index, and ``V`` points
  to the start of the partition in ``Data.db`` as above. The low 9 bits of
  ``V`` hold a slot of the chunk table of the page that holds the payload. The
  table entry in the slot holds ``C`` and ``N``. The bits between the slot and
  the top bits hold ``O``. The writer sets
  ``K = ceil_div(2 + W_O + 9, 8)``, so ``K`` is 3 for ``L <= 8 KiB`` and 4
  above. A reader must treat a slot that is not less than ``count`` as
  corruption.

  To decode ``O``, clear the top two bits of ``V`` and shift the result right
  by 9 bits.

A reader must accept either ``Data.db`` form for any partition. The current
writer uses the slot form for a partition without a row index if all of these
hold:

* The partition starts in the same chunk as the previous or the next
  partition, and that partition has no row index either.
* ``C`` fits in a table entry.
* Two slot payloads and one table entry are smaller than two inline payloads.

Otherwise it uses the inline form. Thus, a partition which shares no chunk
doesn't make the index larger. Within a page, the writer stores each chunk in
one slot only.

Scylla.db: incompatible features
--------------------------------

``Scylla.db`` of a ``mu`` SSTable always contains the ``incompatible_features``
subcomponent (tag 15). It is a set of feature bits. A set bit means that the
SSTable uses a feature that changes how it must be read. A reader must refuse
to open an SSTable with a set bit that it doesn't know. Thus, later Scylla
versions can add such features without a new format version. No such features
are defined yet, so the writer writes no set bits. See
`the Scylla.db format <https://github.com/scylladb/scylladb/blob/master/docs/dev/sstable-scylla-format.md>`_
for the layout.

Older Scylla versions skip unknown ``Scylla.db`` subcomponents. A version that
can't read ``mu`` refuses ``mu`` SSTables because of the format version, and
every version that can read ``mu`` checks ``incompatible_features``. Thus, the
check is reliable for ``mu`` and later formats, but not for earlier formats. A
writer must not set feature bits in an earlier format.

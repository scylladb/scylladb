.. _sstable-mu-format:

SSTable mu Format: Physical Index Positions
============================================

The ``mu`` format is the ``mt`` format with two changes. Both apply only to
compressed SSTables:

* Each compressed chunk in ``Data.db`` stores its own length, in a header and
  in a footer.
* The index components (``Partitions.db`` and ``Rows.db``) point to positions
  in the compressed file, not in the uncompressed data.

With these changes, a reader can find any row without the chunk offsets from
``CompressionInfo.db``. Thus, it does not need to keep those offsets in memory.

An uncompressed ``mu`` SSTable is identical to an uncompressed ``mt`` SSTable.
All components not described on this page are identical to ``mt``.

Notation and parameters
-----------------------

* ``L`` is the uncompressed chunk length, in bytes. It is the ``chunk_length``
  field of ``CompressionInfo.db``. It is a power of two.
* ``bit_width(x)`` is the number of bits needed to represent the unsigned
  integer ``x``. ``bit_width(0)`` is 0.
* ``ceil_div(a, b)`` is ``a / b`` rounded up to an integer.

All other quantities derive from ``L``:

``D_max = L + floor(L / 2)``
  The maximum length of the compressed data of one chunk.

``F = ceil_div(bit_width(D_max), 8)``
  The width, in bytes, of a chunk-length field.

``N_max = ceil_div(511 + D_max + 2 * F + 4, 512)``
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
     - ``F``
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
     - 2
     - 50
     - 6
     - 14
   * - 32
     - 49152
     - 2
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
  ``F`` bytes. The length ``D`` of the compressed data, as an unsigned
  little-endian integer.

``compressed data``
  ``D`` bytes. ``D`` is at most ``D_max``. A writer must not write a longer
  chunk. A reader must treat a larger ``D`` as corruption.

``checksum``
  4 bytes. The CRC32 of the header and the compressed data, as an unsigned
  big-endian integer. It does not cover the footer.

``footer``
  ``F`` bytes. The same value as the header.

The *extent* of a chunk is its size in ``Data.db``: ``D + 2 * F + 4`` bytes.

As in ``mt``, every chunk except the last decompresses to exactly ``L`` bytes.
The last chunk decompresses to at most ``L`` bytes.

A reader at the start of a chunk uses the header to find the end of the chunk.
A reader at the end of a chunk uses the footer to find the start of the chunk.
A footer is not covered by a checksum. If a footer is corrupt, the reader
computes a wrong chunk start, and the checksum of the data at that position
does not match.

``Digest.crc32`` holds the CRC32 of the whole ``Data.db`` file, including the
headers and footers.

``CompressionInfo.db`` has the same layout as in ``mt``. Its chunk offsets point
to the chunk headers.

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

``N`` (length hint)
  The number of 512-byte blocks that the chunk touches. Blocks are aligned to
  offset 0 of ``Data.db``. Thus,
  ``N = ceil_div((C mod 512) + extent, 512)``, and ``1 <= N <= N_max``.

A reader can load the whole chunk with one read of the byte range from ``C`` to
``C - (C mod 512) + 512 * N``. For the last chunk, this range can extend past
the end of ``Data.db``, so the reader must cut it off at the file size. A reader
must still take the chunk length from the header. It must use ``N`` only to
size its first read.

``N`` and ``O`` are often stored together, as one unsigned integer ``LO`` of
``W_N + W_O`` bits::

    LO = ((N - 1) << W_O) | O

Rows.db: row index header
-------------------------

In ``mt``, the header of a partition's row index stores the position of the
partition in ``Data.db`` as one unsigned vint. In ``mu``, this field holds the
physical position of the start of the partition:

* ``C``, as an unsigned vint.
* ``LO``, as an unsigned big-endian integer of ``ceil_div(W_N + W_O, 8)`` bytes.

The other header fields are unchanged. The base of the trie root delta is
still the first byte of the position field.

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

Let ``S`` be the size of the payload in bytes. The 4-bit payload field holds
``S``. The payload is:

* One hash byte. It has the same value as in ``mt``.
* A value ``V``, as a big-endian integer of ``K = S - 1`` bytes.

The top bit of ``V`` selects the target:

* If the top bit is 0, the partition has a row index. ``V`` is the offset in
  ``Rows.db`` of the row index header. The writer uses the smallest ``K`` that
  leaves the top bit 0.
* If the top bit is 1, the partition has no row index. ``V`` points to the
  start of the partition in ``Data.db``. Let ``(C, N, O)`` be that position. The
  low ``W_N + W_O`` bits of ``V`` hold ``LO``. The bits between ``LO`` and the
  top bit hold ``C``. The writer sets
  ``K = ceil_div(bit_width(C) + 1 + W_N + W_O, 8)``, and fills the unused high
  bits with 0.

  To decode ``C``, clear the top bit of ``V`` and shift the result right by
  ``W_N + W_O`` bits.

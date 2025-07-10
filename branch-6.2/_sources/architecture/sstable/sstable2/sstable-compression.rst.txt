SSTable Compression
===================

Chunked Compression of Data File
................................

SSTables compression allows to optionally compress the :doc:`SSTables Data
File </architecture/sstable/sstable2/sstable-data-file>`, which is the biggest part of the
SSTable (the other parts, like the index, cannot be compressed).

Because random-access read from the data file is important, Apache Cassandra implements
*chunked* compression: The uncompressed file is divided into chunks of a
configurable fixed size (usually 64 KB), and each of these chunks is
compressed separately and written to the compressed data file, followed
by a 4-byte checksum of the compressed chunk. The compressed chunks have
different lengths, so we need to remember their offsets so we can seek
to an arbitrary chunk containing our desired uncompressed data. This
list of offsets is stored in a separate Compression Info File, described
below.

The Compression Info File
.........................

    References: **CompressedRandomAccessReader.java**,
    **CompressionMetadata.java**, **CompressionParameters.java**.

The Compression Info File only exists if the data file is compressed.
The Compression Info File specifies the compression parameters that the
decompressor needs to know (such as the compression algorithm and chunk
size), and the list of offsets of the compressed chunks in the
compressed file:

.. code:: c

    struct short_string {
        be16 length;
        char value[length];
    };

::

    struct option {
        short_string key;
        short_string value;
    };

::

    struct compression_info {
        short_string compressor_name;
        be32 options_count;
        option options[options_count];
        be32 chunk_length;
        be64 data_length;
        be32 chunk_count;
        be64 chunk_offsets[chunk_count];
    };

The **compressor\_name** can be one of three strings supported by
Apache Cassandra: "LZ4Compressor", "SnappyCompressor" or "DeflateCompressor".
Cassandra defaults to LZ4Compressor, but a user could choose any one of
the three.

The **options** may contain additional options for the decompressor, But
usually no options are listed, and only one option exists:
"crc\_check\_chance", whose value is a floating-point string which
defaults (if the option doesn't appear) to "1.0", and determines the
probability that we verify the checksum of a compressed chunk we read.

**chunk\_length** is the length of the chunks into which the original
uncompressed data is divided before compression. The decompressor needs
to know this chunk size as well, so when given a desired byte offset in
the original uncompressed file, it can determine which chunk it needs to
uncompress. The chunk length defaults to 65536 bytes, but can be any
power of two.

**data\_length** is the total length of the uncompressed data.

**chunks\_offsets** is the list of offsets of the compressed chunks
inside the compressed file. To read data in position ``p`` in the
uncompressed file, we calculates ``p / chunk_length`` is the
uncompressed chunk number. The compressed version of this check is now
begins in ``chunk_offsets[p / chunk_length]``. The compressed chunk ends
4 bytes before the beginning of the next chunk (because, as explained
above, every compressed chunk is followed by a 4-byte checksum).

The Compressed Data File
........................

As explained above, the compressed data file is a sequence of compressed
chunked, each a compressed version of a fixed-sized (``chunk_length``
from the Compression Info File) uncompressed chunk. Each compressed
chunk is followed by a be32 (big-endian 4-byte integer) Adler32 checksum
of the *compressed* data, which can be used to verify the data hasn't
been corrupted

.. include:: /rst_include/architecture-index.rst

.. include:: /rst_include/apache-copyrights.rst


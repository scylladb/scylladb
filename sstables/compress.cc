/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <stdexcept>
#include <cstdlib>

#include "core/align.hh"
#include "core/unaligned.hh"

#include "compress.hh"

#include <lz4.h>
#include <zlib.h>
#include <snappy-c.h>

namespace sstables {

void compression::update(uint64_t compressed_file_length) {
    // FIXME: also process _compression.options (just for crc-check frequency)
     if (name.value == "LZ4Compressor") {
         _uncompress = uncompress_lz4;
     } else if (name.value == "SnappyCompressor") {
         _uncompress = uncompress_snappy;
     } else if (name.value == "DeflateCompressor") {
         _uncompress = uncompress_deflate;
     } else {
         throw std::runtime_error("unsupported compression type");
     }

     _compressed_file_length = compressed_file_length;
}

compression::chunk_and_offset
compression::locate(uint64_t position) const {
    auto ucl = uncompressed_chunk_length();
    auto chunk_index = position / ucl;
    decltype(ucl) chunk_offset = position % ucl;
    auto chunk_start = offsets.elements.at(chunk_index);
    auto chunk_end = (chunk_index + 1 == offsets.elements.size())
            ? _compressed_file_length
            : offsets.elements.at(chunk_index + 1);
    return { chunk_start, chunk_end - chunk_start, chunk_offset };
}

}

size_t uncompress_lz4(const char* input, size_t input_len,
        char* output, size_t output_len) {
    // We use LZ4_decompress_safe(). According to the documentation, the
    // function LZ4_decompress_fast() is slightly faster, but maliciously
    // crafted compressed data can cause it to overflow the output buffer.
    // Theoretically, our compressed data is created by us so is not malicious
    // (and accidental corruption is avoided by the compressed-data checksum),
    // but let's not take that chance for now, until we've actually measured
    // the performance benefit that LZ4_decompress_fast() would bring.

    // Cassandra's LZ4Compressor prepends to the chunk its uncompressed length
    // in 4 bytes little-endian (!) order. We don't need this information -
    // we already know the uncompressed data is at most the given chunk size
    // (and usually is exactly that, except in the last chunk). The advance
    // knowledge of the uncompressed size could be useful if we used
    // LZ4_decompress_fast(), but we prefer LZ4_decompress_safe() anyway...
    input += 4;
    input_len -= 4;

    auto ret = LZ4_decompress_safe(input, output, input_len, output_len);
    if (ret < 0) {
        throw std::runtime_error("LZ4 uncompression failure");
    }
    return ret;
}

size_t uncompress_deflate(const char* input, size_t input_len,
        char* output, size_t output_len) {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = 0;
    zs.next_in = Z_NULL;
    if (inflateInit(&zs) != Z_OK) {
        throw std::runtime_error("deflate uncompression init failure");
    }
    // yuck, zlib is not const-correct, and also uses unsigned char while we use char :-(
    zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(input));
    zs.avail_in = input_len;
    zs.next_out = reinterpret_cast<unsigned char*>(output);
    zs.avail_out = output_len;
    auto res = inflate(&zs, Z_FINISH);
    inflateEnd(&zs);
    if (res == Z_STREAM_END) {
        return output_len - zs.avail_out;
    } else {
        throw std::runtime_error("deflate uncompression failure");
    }
}

size_t uncompress_snappy(const char* input, size_t input_len,
        char* output, size_t output_len) {
    if (snappy_uncompress(input, input_len, output, &output_len)
            == SNAPPY_OK) {
        return output_len;
    } else {
        throw std::runtime_error("deflate uncompression failure");
    }
}

uint32_t checksum_adler32(const char* input, size_t input_len) {
    auto init = adler32(0, Z_NULL, 0);
    // yuck, zlib uses unsigned char while we use char :-(
    return adler32(init, reinterpret_cast<const unsigned char *>(input),
            input_len);
}

// Start one dma_read() operation to read the given given byte range from a
// file into a new temporary buffer.
//
// Because O_DIRECT requires the read start and end to be multiples of some
// block size, this function can read more than actually requested, but does
// not return the unrequested part. This leads to some inefficiency when two
// adjacent unaligned read_exactly() calls end up reading the same overlap
// block(s) twice.
static future<temporary_buffer<char>>
read_exactly(lw_shared_ptr<file> f, uint64_t start, uint64_t len) {
    // O_DIRECT reading requires that buffer, offset, and read length, are
    // all aligned. Alignment of 4096 was necessary in the past, but no longer
    // is - 512 is usually enough; But we'll need to use BLKSSZGET ioctl to
    // be sure it is really enough on this filesystem. 4096 is always safe.
    constexpr uint64_t alignment = 4096;
    auto front = start & (alignment - 1);
    start -= front;
    len += front;

    auto buf = temporary_buffer<char>::aligned(
            alignment, align_up(len, alignment));
    auto read = f->dma_read(start, buf.get_write(), buf.size());
    return read.then(
            [buf = std::move(buf), front, len] (size_t size) mutable {
        buf.trim(std::min(size, len));
        buf.trim_front(front);
        return std::move(buf);
    });
}

class compressed_file_data_source_impl : public data_source_impl {
    lw_shared_ptr<file> _file;
    sstables::compression* _compression_metadata;
    uint64_t _pos = 0;
public:
    compressed_file_data_source_impl(lw_shared_ptr<file> f,
            sstables::compression* cm, uint64_t pos)
            : _file(std::move(f)), _compression_metadata(cm),
              _pos(pos)
            {}
    virtual future<temporary_buffer<char>> get() override {
        if (_pos >= _compression_metadata->data_len) {
            return make_ready_future<temporary_buffer<char>>();
        }
        auto addr = _compression_metadata->locate(_pos);
        return read_exactly(_file, addr.chunk_start, addr.chunk_len).
            then([this, addr](temporary_buffer<char> buf) {
                if (addr.chunk_len != buf.size()) {
                    throw std::runtime_error("couldn't read entire chunk");
                }
                // The last 4 bytes of the chunk are the adler32 checksum
                // of the rest of the (compressed) chunk.
                auto compressed_len = addr.chunk_len - 4;
                // FIXME: Do not always calculate checksum - Cassandra has a
                // probability (defaulting to 1.0, but still...)
                uint32_t checksum = ntohl(*unaligned_cast<const uint32_t *>(
                        buf.get() + compressed_len));
                if (checksum != checksum_adler32(buf.get(), compressed_len)) {
                    throw std::runtime_error("compressed chunk failed checksum");
                }

                // We know that the uncompressed data will take exactly
                // chunk_length bytes (or less, if reading the last chunk).
                temporary_buffer<char> out(
                        _compression_metadata->uncompressed_chunk_length());
                // The compressed data is the whole chunk, minus the last 4
                // bytes (which contain the checksum verified above).
                auto len = _compression_metadata->uncompress(
                        buf.get(), compressed_len,
                        out.get_write(), out.size());
                out.trim(len);
                out.trim_front(addr.offset);
                _pos += out.size();
                return out;
        });
    }
};

class compressed_file_data_source : public data_source {
public:
    compressed_file_data_source(lw_shared_ptr<file> f,
            sstables::compression* cm, uint64_t offset)
        : data_source(std::make_unique<compressed_file_data_source_impl>(
                std::move(f), cm, offset))
        {}
};

input_stream<char> make_compressed_file_input_stream(
        lw_shared_ptr<file> f, sstables::compression* cm, uint64_t offset)
{
    return input_stream<char>(compressed_file_data_source(
            std::move(f), cm, offset));
}

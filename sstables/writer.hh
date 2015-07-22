/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "core/iostream.hh"
#include "core/fstream.hh"
#include "types.hh"
#include "compress.hh"

namespace sstables {

class file_writer {
    output_stream<char> _out;
    size_t _offset = 0;
public:
    file_writer(file f, size_t buffer_size = 8192)
        : _out(make_file_output_stream(std::move(f), buffer_size)) {}

    file_writer(output_stream<char>&& out)
        : _out(std::move(out)) {}

    virtual ~file_writer() = default;
    file_writer(file_writer&&) = default;

    virtual future<> write(const char* buf, size_t n) {
        _offset += n;
        return _out.write(buf, n);
    }
    virtual future<> write(const bytes& s) {
        _offset += s.size();
        return _out.write(s);
    }
    future<> flush() {
        return _out.flush();
    }
    future<> close() {
        return _out.close();
    }
    size_t offset() {
        return _offset;
    }

    friend class checksummed_file_writer;
};

class checksummed_file_writer : public file_writer {
    checksum _c;
    uint32_t _per_chunk_checksum;
    uint32_t _full_checksum;
    bool _checksum_file;
private:
    void close_checksum() {
        if (!_checksum_file) {
            return;
        }
        if ((_offset % _c.chunk_size) != 0) {
            _c.checksums.push_back(_per_chunk_checksum);
        }
    }

    // NOTE: adler32 is the algorithm used to compute checksum.
    // compute a checksum per chunk of size _c.chunk_size
    void compute_checksum(const char* buf, size_t n) {
        if (!_checksum_file) {
            _offset += n;
            _full_checksum = checksum_adler32(_full_checksum, buf, n);
            return;
        }

        size_t remaining = n;
        while (remaining) {
            // available means available space in the current chunk.
            size_t available = _c.chunk_size - (_offset % _c.chunk_size);

            if (remaining < available) {
                _offset += remaining;
                _per_chunk_checksum = checksum_adler32(_per_chunk_checksum, buf, remaining);
                remaining = 0;
            } else {
                _offset += available;
                _per_chunk_checksum = checksum_adler32(_per_chunk_checksum, buf, available);
                _full_checksum = checksum_adler32_combine(_full_checksum, _per_chunk_checksum, _c.chunk_size);
                _c.checksums.push_back(_per_chunk_checksum);
                _per_chunk_checksum = init_checksum_adler32();
                buf += available;
                remaining -= available;
            }
        }
    }
public:
    checksummed_file_writer(file f, size_t buffer_size = 8192, bool checksum_file = false)
            : file_writer(std::move(f), buffer_size) {
        _checksum_file = checksum_file;
        _c.chunk_size = DEFAULT_CHUNK_SIZE;
        _per_chunk_checksum = init_checksum_adler32();
        _full_checksum = init_checksum_adler32();
    }

    virtual future<> write(const char* buf, size_t n) {
        compute_checksum(buf, n);
        return _out.write(buf, n);
    }
    virtual future<> write(const bytes& s) {
        compute_checksum(reinterpret_cast<const char*>(s.c_str()), s.size());
        return _out.write(s);
    }
    checksum& finalize_checksum() {
        close_checksum();
        return _c;
    }
    uint32_t full_checksum() {
        if (!_checksum_file) {
            return _full_checksum;
        }
        return checksum_adler32_combine(_full_checksum, _per_chunk_checksum, _offset % _c.chunk_size);
    }
};

// compressed_file_data_sink_impl works as a filter for a file output stream,
// where the buffer flushed will be compressed and its checksum computed, then
// the result passed to a regular output stream.
class compressed_file_data_sink_impl : public data_sink_impl {
    output_stream<char> _out;
    sstables::compression* _compression_metadata;
    size_t _pos = 0;
public:
    compressed_file_data_sink_impl(file f, sstables::compression* cm, file_output_stream_options options)
            : _out(make_file_output_stream(std::move(f), options))
            , _compression_metadata(cm) {}

    future<> put(net::packet data) { abort(); }
    virtual future<> put(temporary_buffer<char> buf) override {
        auto output_len = _compression_metadata->compress_max_size(buf.size());
        // account space for checksum that goes after compressed data.
        temporary_buffer<char> compressed(output_len + 4);

        // compress flushed data.
        auto len = _compression_metadata->compress(buf.get(), buf.size(), compressed.get_write(), output_len);
        if (len > output_len) {
            throw std::runtime_error("possible overflow during compression");
        }

        _compression_metadata->offsets.elements.push_back(_pos);
        // account compressed data + 32-bit checksum.
        _pos += len + 4;
        _compression_metadata->set_compressed_file_length(_pos);
        // total length of the uncompressed data.
        _compression_metadata->data_len += buf.size();

        // compute 32-bit checksum for compressed data.
        uint32_t per_chunk_checksum = checksum_adler32(compressed.get(), len);
        _compression_metadata->update_full_checksum(per_chunk_checksum, len);

        // write checksum into buffer after compressed data.
        *unaligned_cast<uint32_t*>(compressed.get_write() + len) = htonl(per_chunk_checksum);

        compressed.trim(len + 4);

        auto f = _out.write(compressed.get(), compressed.size());
        return f.then([compressed = std::move(compressed)] {});
    }
    virtual future<> close() {
        return _out.close();
    }
};

class compressed_file_data_sink : public data_sink {
public:
    compressed_file_data_sink(file f, sstables::compression* cm, file_output_stream_options options)
        : data_sink(std::make_unique<compressed_file_data_sink_impl>(
                std::move(f), cm, options)) {}
};

static inline output_stream<char> make_compressed_file_output_stream(file f, sstables::compression* cm) {
    file_output_stream_options options;
    // buffer of output stream is set to chunk length, because flush must
    // happen every time a chunk was filled up.
    options.buffer_size = cm->uncompressed_chunk_length();
    return output_stream<char>(compressed_file_data_sink(std::move(f), cm, options), options.buffer_size, true);
}

}

/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "core/iostream.hh"
#include "types.hh"
#include "sstables.hh"
#include "compress.hh"

namespace sstables {

class file_writer {
    output_stream<char> _out;
    size_t _offset = 0;
public:
    file_writer(lw_shared_ptr<file> f, size_t buffer_size = 8192)
        : _out(make_file_output_stream(std::move(f), buffer_size)) {}

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
    void do_compute_checksum(const char* buf, size_t n) {
        uint32_t buf_checksum = checksum_adler32(buf, n);

        _offset += n;
        if (_checksum_file) {
            _per_chunk_checksum = checksum_adler32_combine(_per_chunk_checksum, buf_checksum, n);
        }
        _full_checksum = checksum_adler32_combine(_full_checksum, buf_checksum, n);
    }

    // compute a checksum per chunk of size _c.chunk_size
    void compute_checksum(const char* buf, size_t n) {
        if (!_checksum_file) {
            do_compute_checksum(buf, n);
            return;
        }

        size_t remaining = n;
        while (remaining) {
            // available means available space in the current chunk.
            size_t available = _c.chunk_size - (_offset % _c.chunk_size);

            if (remaining < available) {
                do_compute_checksum(buf, remaining);
                remaining = 0;
            } else {
                do_compute_checksum(buf, available);
                _c.checksums.push_back(_per_chunk_checksum);
                _per_chunk_checksum = init_checksum_adler32();
                buf += available;
                remaining -= available;
            }
        }
    }
public:
    checksummed_file_writer(lw_shared_ptr<file> f, size_t buffer_size = 8192, bool checksum_file = false)
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
        return _full_checksum;
    }
};

}

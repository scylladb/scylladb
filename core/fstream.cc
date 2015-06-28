/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "fstream.hh"
#include "align.hh"
#include <malloc.h>
#include <string.h>

class file_data_source_impl : public data_source_impl {
    lw_shared_ptr<file> _file;
    uint64_t _pos;
    std::experimental::optional<size_t> _fsize;
    size_t _buffer_size;
private:
    // Should be called only when _fsize is initialized
    future<temporary_buffer<char>> do_get() {
        using buf_type = temporary_buffer<char>;
        size_t read_size = _buffer_size;
        size_t fsize = _fsize.value();

        if (_pos >= fsize) {
            return make_ready_future<buf_type>(std::move(buf_type(0)));
        } else if (_pos + _buffer_size > fsize) {
            read_size = fsize - _pos;
        }

        return _file->dma_read_bulk<char>(_pos, read_size).then(
                [this] (buf_type buf) {
            _pos += buf.size();

            return std::move(buf);
        });
    }
public:
    file_data_source_impl(lw_shared_ptr<file> f, uint64_t pos, size_t buffer_size)
            : _file(std::move(f)), _pos(pos), _buffer_size(buffer_size) {}

    virtual future<temporary_buffer<char>> get() override {
        if (!_fsize){
            return _file->size().then(
                    [this] (size_t fsize) {
                _fsize = fsize;

                return do_get();
            });
        }

        return do_get();
    }
};

class file_data_source : public data_source {
public:
    file_data_source(lw_shared_ptr<file> f, uint64_t offset, size_t buffer_size)
        : data_source(std::make_unique<file_data_source_impl>(
                std::move(f), offset, buffer_size)) {}
};

input_stream<char> make_file_input_stream(
        lw_shared_ptr<file> f, uint64_t offset, size_t buffer_size) {
    return input_stream<char>(file_data_source(std::move(f), offset, buffer_size));
}

class file_data_sink_impl : public data_sink_impl {
    lw_shared_ptr<file> _file;
    file_output_stream_options _options;
    uint64_t _pos = 0;
    uint64_t _last_preallocation = 0;
public:
    file_data_sink_impl(lw_shared_ptr<file> f, file_output_stream_options options)
            : _file(std::move(f)), _options(options) {}
    future<> put(net::packet data) { abort(); }
    virtual temporary_buffer<char> allocate_buffer(size_t size) override {
        return temporary_buffer<char>::aligned(file::dma_alignment, size);
    }
    virtual future<> put(temporary_buffer<char> buf) override {
        // put() must usually be of chunks multiple of file::dma_alignment.
        // Only the last part can have an unaligned length. If put() was
        // called again with an unaligned pos, we have a bug in the caller.
        assert(!(_pos & (file::dma_alignment - 1)));
        bool truncate = false;
        auto pos = _pos;
        _pos += buf.size();
        auto p = static_cast<const char*>(buf.get());
        size_t buf_size = buf.size();

        if ((buf.size() & (file::dma_alignment - 1)) != 0) {
            // If buf size isn't aligned, copy its content into a new aligned buf.
            // This should only happen when the user calls output_stream::flush().
            auto tmp = allocate_buffer(align_up(buf.size(), file::dma_alignment));
            ::memcpy(tmp.get_write(), buf.get(), buf.size());
            buf = std::move(tmp);
            p = buf.get();
            buf_size = buf.size();
            truncate = true;
        }
        auto prealloc = make_ready_future<>();
        if (pos + buf_size > _last_preallocation) {
            auto old = _last_preallocation;
            _last_preallocation = align_down<uint64_t>(old + _options.preallocation_size, _options.preallocation_size);
            prealloc = _file->allocate(old, _last_preallocation - old);
        }
        return prealloc.then([this, pos, p, buf_size, truncate, buf = std::move(buf)] () mutable {
            return _file->dma_write(pos, p, buf_size).then(
                    [this, buf = std::move(buf), truncate] (size_t size) {
                if (truncate) {
                    return _file->truncate(_pos);
                }
                return make_ready_future<>();
            });
        });
    }
    future<> close() { return _file->flush(); }
};

class file_data_sink : public data_sink {
public:
    file_data_sink(lw_shared_ptr<file> f, file_output_stream_options options)
        : data_sink(std::make_unique<file_data_sink_impl>(
                std::move(f), options)) {}
};

output_stream<char> make_file_output_stream(lw_shared_ptr<file> f, size_t buffer_size) {
    file_output_stream_options options;
    options.buffer_size = buffer_size;
    return make_file_output_stream(std::move(f), options);
}

output_stream<char> make_file_output_stream(lw_shared_ptr<file> f, file_output_stream_options options) {
    return output_stream<char>(file_data_sink(std::move(f), options), options.buffer_size, true);
}


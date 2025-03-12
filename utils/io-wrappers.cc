/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "io-wrappers.hh"
#include <seastar/util/internal/iovec_utils.hh>

using namespace seastar;

class noop_file_impl : public file_impl {
public:
    [[noreturn]] void not_implemented() const {
        throw std::logic_error("unsupported operation");
    }
    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override {
        not_implemented();
    }
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        not_implemented();
    }
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        not_implemented();
    }
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
        not_implemented();
    }
    future<> flush(void) override {
        return make_ready_future<>();
    }
    future<struct stat> stat(void) override {
        not_implemented();
    }
    future<> truncate(uint64_t length) override {
        not_implemented();
    }
    future<> discard(uint64_t offset, uint64_t) override {
        not_implemented();
    }
    future<> allocate(uint64_t position, uint64_t) override {
        not_implemented();
    }
    future<uint64_t> size(void) override {
        not_implemented();
    }
    future<> close() override {
        return make_ready_future<>();
    }
    std::unique_ptr<seastar::file_handle_impl> dup() override {
        not_implemented();
    }
    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        not_implemented();
    }
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
        not_implemented();
    }
};

file create_file_for_sink(data_sink sink) {
    class data_sink_file_impl : public noop_file_impl {
        data_sink _sink;
        // keep a buffer to allow truncation and also
        // partially non-block sized writes (iovec)
        char _buffer[4096];
        uint64_t _pos = 0;
        uint64_t _bufpos = 0;
    public:
        data_sink_file_impl(data_sink sink)
            : _sink(std::move(sink))
        {
            assert(this->_disk_write_dma_alignment == sizeof(_buffer));
        }
        future<> put(const char* data, size_t len) {
            temporary_buffer<char> buf(data, len);
            return _sink.put(std::move(buf));
        }
        future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override {
            if (len < this->_disk_write_dma_alignment) {
                throw std::invalid_argument("Invalid buffer length: " + std::to_string(len));
            }
            return do_write_dma(pos, buffer, len);
        }
        future<size_t> do_write_dma(uint64_t pos, const void* buffer, size_t len) {
            if (pos != _pos) {
                throw std::logic_error("Non-sequential write to sink file");
            }

            auto res = len;
            const char* buf = reinterpret_cast<const char*>(buffer);

            // if we've added any data to buffer last put, we need to
            // fill as much as we can and send the active block
            if (_bufpos != 0) {
                auto add = std::min(len, sizeof(_buffer) - _bufpos);
                std::copy(buf, buf + add, _buffer + _bufpos);
                buf += add;
                len -= add;
                _bufpos += add;
                _pos += add;
                if (_bufpos == sizeof(_buffer) && len > 0) {
                    co_await put(_buffer, _bufpos);
                    _bufpos = 0;
                }
            }

            // more than one block left? send it along.
            if (len > this->_disk_write_dma_alignment) {
                auto size = len - this->_disk_write_dma_alignment;
                co_await put(buf, size);

                buf += size;
                pos += size;
                len -= size;
            }

            assert(len <= sizeof(_buffer));
            assert(_bufpos == 0 || len == 0);
            std::copy(buf, buf+len, _buffer);
            _pos += len;
            _bufpos += len;

            co_return res;
        }

        future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
            internal::sanitize_iovecs(iov, _disk_read_dma_alignment);
            size_t res = 0;
            for (auto& iv : iov) {
                res += co_await do_write_dma(pos + res, iv.iov_base, iv.iov_len);
            }
            co_return res;
        }
        future<> truncate(uint64_t length) override {
            if (length < _pos) {
                // bufpos should be a full buffer here.
                if (length < (_pos - _bufpos)) {
                    throw std::invalid_argument("Cannot truncate below one disk page of current position");
                }
                _bufpos -= (_pos - length);
            }
            if (_bufpos != 0) {
                co_await put(_buffer, _bufpos);
                _bufpos = 0;
            }
            // if we truncate upwards, just write zeroes.
            uint64_t bp = 0;
            while (length > _pos) {
                auto rem = std::min(length - _pos, sizeof(_buffer));
                if (rem > bp) {
                    std::fill(_buffer, _buffer + rem , 0);
                    bp = rem;
                }
                co_await put(_buffer, rem);
                _pos += rem;
            }
        }
        future<> close() override {
            co_await truncate(_pos);
            co_await _sink.flush();
            co_await _sink.close();
        }
    };
    return file{make_shared<data_sink_file_impl>(std::move(sink))};
}

file create_noop_file() {
    return file{make_shared<noop_file_impl>()};
}

data_sink create_memory_sink(std::vector<seastar::temporary_buffer<char>>& bufs) {
    // TODO: move to seastar. Based on memory_data_sink, but allowing us
    // to actually move away the buffers later. I don't want to modify
    // util classes in an enterprise patch.
    class buffer_data_sink_impl : public data_sink_impl {
        std::vector<temporary_buffer<char>>& _bufs;
    public:
        buffer_data_sink_impl(std::vector<temporary_buffer<char>>& bufs)
            : _bufs(bufs)
        {}
        future<> put(net::packet p) override {
            return put(p.release());
        }
        future<> put(std::vector<temporary_buffer<char>> bufs) override {
            for (auto&& buf : bufs) {
                _bufs.emplace_back(std::move(buf));
            }
            return make_ready_future<>();
        }
        future<> put(temporary_buffer<char> buf) override {
            _bufs.emplace_back(std::move(buf));
            return make_ready_future<>();
        }
        future<> flush() override {
            return make_ready_future<>();
        }
        future<> close() override {
            return make_ready_future<>();
        }
        size_t buffer_size() const noexcept override {
            return 128*1024;
        }
    };
    return data_sink(std::make_unique<buffer_data_sink_impl>(bufs));
}

data_source create_memory_source(std::vector<seastar::temporary_buffer<char>> bufs) {
    // TODO: move to seastar. Based on buffer_input... in utils, but
    // handles potential 1+ buffers
    class buffer_data_source_impl : public data_source_impl {
    private:
        std::vector<temporary_buffer<char>> _bufs;
        size_t _index = 0;
    public:
        buffer_data_source_impl(std::vector<temporary_buffer<char>>&& bufs)
            : _bufs(std::move(bufs))
        {}
        buffer_data_source_impl(buffer_data_source_impl&&) noexcept = default;
        buffer_data_source_impl& operator=(buffer_data_source_impl&&) noexcept = default;

        future<temporary_buffer<char>> get() override {
            if (_index < _bufs.size()) {
                return make_ready_future<temporary_buffer<char>>(std::move(_bufs.at(_index++)));
            }
            return make_ready_future<temporary_buffer<char>>();
        }
        future<temporary_buffer<char>> skip(uint64_t n) override {
            while (n > 0 && _index < _bufs.size()) {
                auto& buf = _bufs.at(_index);
                auto min = std::min(n, buf.size());
                buf.trim_front(min);
                if (buf.empty()) {
                    ++_index;
                }
                n -= min;
            }
            return get();
        }
    };
    return data_source(std::make_unique<buffer_data_source_impl>(std::move(bufs)));
}


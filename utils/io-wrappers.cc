/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "io-wrappers.hh"
#include "seekable_source.hh"
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
        future<> put(std::span<temporary_buffer<char>> bufs) override {
            for (auto&& buf : bufs) {
                _bufs.emplace_back(std::move(buf));
            }
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

seastar::file create_file_for_seekable_source(seekable_data_source src, seekable_data_source_shard_src src_func) {
    class seekable_data_source_file_impl : public noop_file_impl {
        seekable_data_source _source;
        seekable_data_source_shard_src _src_func;
    public:
        seekable_data_source_file_impl(seekable_data_source source, seekable_data_source_shard_src src_func)
            : _source(std::move(source))
            , _src_func(std::move(src_func))
        {}
        future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
            co_await _source.seek(pos);
            size_t res = 0;
            auto dst = reinterpret_cast<char*>(buffer);
            while (len) {
                auto buf = co_await _source.get(len);
                assert(buf.size() <= len);
                if (buf.empty()) {
                    break;
                }
                dst = std::copy(buf.begin(), buf.end(), dst);
                len -= buf.size();
                res += buf.size();
            }
            co_return res;
        }
        future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
            size_t res = 0;
            for (auto& iv : iov) {
                auto n = co_await read_dma(pos, iv.iov_base, iv.iov_len, intent);
                if (n == 0) {
                    break;
                }
                res += n;
            }
            co_return res;
        }
        future<uint64_t> size(void) override {
            return _source.size();
        }
        future<struct stat> stat(void) override {
            struct stat res = {};
            res.st_nlink = 1;
            res.st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
            res.st_size = co_await _source.size();
            res.st_blksize = 1 << 10; // huh?
            res.st_blocks = res.st_size >> 9;
            res.st_mtime = std::chrono::system_clock::to_time_t(co_await _source.timestamp());
            res.st_ctime = res.st_mtime;
            co_return res;
        }
        future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
            temporary_buffer<uint8_t> res(range_size);
            auto n = co_await read_dma(offset, res.get_write(), range_size, intent);
            res.trim(n);
            co_return res;
        }
        std::unique_ptr<seastar::file_handle_impl> dup() override {
            if (!_src_func) {
                not_implemented();
            }
            class my_file_handle_impl : public file_handle_impl {
                seekable_data_source_shard_src _func;
            public:
                my_file_handle_impl(seekable_data_source_shard_src func)
                    : _func(std::move(func))
                {}
                std::unique_ptr<file_handle_impl> clone() const override {
                    return std::make_unique<my_file_handle_impl>(_func);
                }
                shared_ptr<file_impl> to_file() && override {
                    auto src = _func();
                    return seastar::make_shared<seekable_data_source_file_impl>(std::move(src), std::move(_func));
                }
            };
            return std::make_unique<my_file_handle_impl>(_src_func);
        }
        future<> close() override {
            co_await _source.close();
        }
    };
    return file{seastar::make_shared<seekable_data_source_file_impl>(std::move(src), std::move(src_func))};
}

seastar::data_source create_ranged_source(data_source src, uint64_t offset, std::optional<uint64_t> len) {
    class ranged_data_source : public data_source_impl {
        data_source _src;
        uint64_t _offset;
        uint64_t _len;
        uint64_t _read;
    public:
        ranged_data_source(data_source src, uint64_t offset, std::optional<uint64_t> len)
            : _src(std::move(src))
            , _offset(offset)
            , _len(len.value_or(std::numeric_limits<uint64_t>::max()))
            , _read(0)
        {}

        temporary_buffer<char> trim(temporary_buffer<char> buf) {
            auto max = _len - _read;
            if (buf.size() > max) {
                buf.trim(max);
            }
            _read += buf.size();
            return buf;
        }
        future<temporary_buffer<char>> skip(uint64_t n) override {
            if (_read == _len) {
                co_return temporary_buffer<char>{};
            }
            if (auto skip = std::exchange(_offset, 0); (skip + n) > 0) {
                co_return trim(co_await _src.skip(skip + n));
            }
            co_return trim(co_await _src.get());
        }
        future<temporary_buffer<char>> get() override {
            return skip(0);
        }
        future<> close() override {
            return _src.close();
        }
    };

    if (offset == 0 && !len.has_value()) {
        return src;
    }
    return data_source(std::make_unique<ranged_data_source>(std::move(src), offset, len));
}

seastar::data_source create_prefetch_source(data_source src) {
    class prefetch_data_source : public data_source_impl {
        data_source _src;
        future<temporary_buffer<char>> _fetch;
    public:
        prefetch_data_source(data_source src)
            : _src(std::move(src))
            , _fetch(_src.get())
        {}
        void start_fetch() {
            _fetch = _src.get();
        }
        future<temporary_buffer<char>> skip(uint64_t n) override {
            auto res = co_await std::move(_fetch);
            auto trim = std::min(n, res.size());
            res.trim_front(trim);
            n -= trim;
            if (n > 0) {
                res = co_await _src.skip(n);
            } else if (res.empty() && trim > 0) {
                res = co_await _src.get();
            }
            if (res.empty()) {
                _fetch = make_ready_future<temporary_buffer<char>>();
            } else {
                _fetch = _src.get();
            }
            co_return res;
        }
        future<temporary_buffer<char>> get() override {
            return skip(0);
        }
        future<> close() override {
            return _src.close();
        }
    };

    return data_source(std::make_unique<prefetch_data_source>(std::move(src)));
}

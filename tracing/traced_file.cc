/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/future.hh>
#include <seastar/core/print.hh>

#include "tracing/traced_file.hh"

using namespace seastar;

class traced_file_impl : public file_impl {
private:
    file _f;

    tracing::trace_state_ptr _trace_state;
    sstring _trace_prefix;

public:
    traced_file_impl(file, tracing::trace_state_ptr, sstring);

    virtual future<size_t> write_dma(uint64_t pos, const void* buf, size_t len, const io_priority_class& pc) override;
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override;
    virtual future<size_t> read_dma(uint64_t pos, void* buf, size_t len, const io_priority_class& pc) override;
    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override;
    virtual future<> flush(void) override;
    virtual future<struct stat> stat(void) override;
    virtual future<> truncate(uint64_t length) override;
    virtual future<> discard(uint64_t offset, uint64_t length) override;
    virtual future<> allocate(uint64_t position, uint64_t length) override;
    virtual future<uint64_t> size(void) override;
    virtual future<> close() override;
    virtual std::unique_ptr<seastar::file_handle_impl> dup() override;
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override;
    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override;
};

traced_file_impl::traced_file_impl(file f, tracing::trace_state_ptr trace_state, sstring trace_prefix)
        : file_impl(*get_file_impl(f)), _f(std::move(f)), _trace_state(std::move(trace_state)), _trace_prefix(std::move(trace_prefix)) {
}

future<size_t>
traced_file_impl::write_dma(uint64_t pos, const void* buf, size_t len, const io_priority_class& pc) {
    tracing::trace(_trace_state, "{} scheduling DMA write of {} bytes at position {}", _trace_prefix, len, pos);
    return get_file_impl(_f)->write_dma(pos, buf, len, pc).then_wrapped([this, pos, len] (future<size_t> f) {
        try {
            auto ret = f.get0();
            tracing::trace(_trace_state, "{} finished DMA write of {} bytes at position {}, successfully wrote {} bytes", _trace_prefix, len, pos, ret);
            return ret;
        } catch (...) {
            tracing::trace(_trace_state, "{} failed DMA write of {} bytes at position {}: {}", _trace_prefix, len, pos, std::current_exception());
            throw;
        }
    });
}

future<size_t>
traced_file_impl::write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) {
    tracing::trace(_trace_state, "{} scheduling DMA write at position {}", _trace_prefix, pos);
    return get_file_impl(_f)->write_dma(pos, std::move(iov), pc).then_wrapped([this, pos] (future<size_t> f) {
        try {
            auto ret = f.get0();
            tracing::trace(_trace_state, "{} finished DMA write at position {}, successfully wrote {} bytes", _trace_prefix, pos, ret);
            return ret;
        } catch (...) {
            tracing::trace(_trace_state, "{} failed DMA write of at position {}: {}", _trace_prefix, pos, std::current_exception());
            throw;
        }
    });
}

future<size_t>
traced_file_impl::read_dma(uint64_t pos, void* buf, size_t len, const io_priority_class& pc) {
    tracing::trace(_trace_state, "{} scheduling DMA read of {} bytes at position {}", _trace_prefix, len, pos);
    return get_file_impl(_f)->read_dma(pos, buf, len, pc).then_wrapped([this, pos, len] (future<size_t> f) {
        try {
            auto ret = f.get0();
            tracing::trace(_trace_state, "{} finished DMA read of {} bytes at position {}, successfully read {} bytes", _trace_prefix, len, pos, ret);
            return ret;
        } catch (...) {
            tracing::trace(_trace_state, "{} failed DMA read of {} bytes at position {}: {}", _trace_prefix, len, pos, std::current_exception());
            throw;
        }
    });
}

future<size_t>
traced_file_impl::read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) {
    tracing::trace(_trace_state, "{} scheduling DMA read at position {}", _trace_prefix, pos);
    return get_file_impl(_f)->read_dma(pos, std::move(iov), pc).then_wrapped([this, pos] (future<size_t> f) {
        try {
            auto ret = f.get0();
            tracing::trace(_trace_state, "{} finished DMA read at position {}, successfully read {} bytes", _trace_prefix, pos, ret);
            return ret;
        } catch (...) {
            tracing::trace(_trace_state, "{} failed DMA read at position {}: {}", _trace_prefix, pos, std::current_exception());
            throw;
        }
    });
}

future<>
traced_file_impl::flush(void) {
    tracing::trace(_trace_state, "{} scheduling flush", _trace_prefix);
    return get_file_impl(_f)->flush().then_wrapped([this] (future<> f) {
        try {
            f.get();
            tracing::trace(_trace_state, "{} finished flush", _trace_prefix);
        } catch (...) {
            tracing::trace(_trace_state, "{} failed flush: {}", _trace_prefix, std::current_exception());
            throw;
        }
    });
}

future<struct stat>
traced_file_impl::stat(void) {
    tracing::trace(_trace_state, "{} scheduling file status retrieval", _trace_prefix);
    return get_file_impl(_f)->stat().then_wrapped([this] (future<struct stat> f) {
        try {
            auto s = f.get0();
            tracing::trace(_trace_state, "{} finished file status retrieval", _trace_prefix);
            return s;
        } catch (...) {
            tracing::trace(_trace_state, "{} failed to retrieve file status: {}", _trace_prefix, std::current_exception());
            throw;
        }
    });
}

future<>
traced_file_impl::truncate(uint64_t length) {
    tracing::trace(_trace_state, "{} scheduling truncate to {} bytes", _trace_prefix, length);
    return get_file_impl(_f)->truncate(length).then_wrapped([this, length] (future<> f) {
        try {
            f.get();
            tracing::trace(_trace_state, "{} finished truncate to {} bytes", _trace_prefix, length);
        } catch (...) {
            tracing::trace(_trace_state, "{} failed to truncate file to {} bytes: {}", _trace_prefix, length, std::current_exception());
            throw;
        }
    });
}

future<>
traced_file_impl::discard(uint64_t offset, uint64_t length) {
    tracing::trace(_trace_state, "{} scheduling discard of {} bytes at offset {}", _trace_prefix, length, offset);
    return get_file_impl(_f)->discard(offset, length).then_wrapped([this, offset, length] (future<> f) {
        try {
            f.get();
            tracing::trace(_trace_state, "{} finished discard of {} bytes at offset {}", _trace_prefix, length, offset);
        } catch (...) {
            tracing::trace(_trace_state, "{} failed to discard {} bytes at offset {}: {}", _trace_prefix, length, offset, std::current_exception());
            throw;
        }
    });
}

future<>
traced_file_impl::allocate(uint64_t position, uint64_t length) {
    tracing::trace(_trace_state, "{} scheduling allocation of {} bytes at position {}", _trace_prefix, length, position);
    return get_file_impl(_f)->allocate(position, length).then_wrapped([this, position, length] (future<> f) {
        try {
            f.get();
            tracing::trace(_trace_state, "{} finished allocation of {} bytes at position {}", _trace_prefix, length, position);
        } catch (...) {
            tracing::trace(_trace_state, "{} failed to allocate {} bytes at position {}: {}", _trace_prefix, length, position, std::current_exception());
            throw;
        }
    });
}

future<uint64_t>
traced_file_impl::size(void) {
    tracing::trace(_trace_state, "{} scheduling size retrieval", _trace_prefix);
    return get_file_impl(_f)->size().then_wrapped([this] (future<uint64_t> f) {
        try {
            auto ret = f.get0();
            tracing::trace(_trace_state, "{} retrieved size = {}", _trace_prefix, ret);
            return ret;
        } catch (...) {
            tracing::trace(_trace_state, "{} failed to retrieve size: {}", _trace_prefix, std::current_exception());
            throw;
        }
    });
}

future<>
traced_file_impl::close() {
    tracing::trace(_trace_state, "{} scheduling close", _trace_prefix);
    return get_file_impl(_f)->close().then_wrapped([this] (future<> f) {
        try {
            f.get();
            tracing::trace(_trace_state, "{} closed file", _trace_prefix);
        } catch (...) {
            tracing::trace(_trace_state, "{} failed to close: {}", _trace_prefix, std::current_exception());
            throw;
        }
    });
}

std::unique_ptr<seastar::file_handle_impl>
traced_file_impl::dup() {
    return get_file_impl(_f)->dup();
}

subscription<directory_entry>
traced_file_impl::list_directory(std::function<future<> (directory_entry de)> next) {
    return get_file_impl(_f)->list_directory(std::move(next));
}

future<temporary_buffer<uint8_t>>
traced_file_impl::dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) {
    tracing::trace(_trace_state, "{} scheduling bulk DMA read of size {} at offset {}", _trace_prefix, range_size, offset);
    return get_file_impl(_f)->dma_read_bulk(offset, range_size, pc).then_wrapped([this, offset, range_size] (future<temporary_buffer<uint8_t>> f) {
        try {
            auto ret = f.get0();
            tracing::trace(_trace_state, "{} finished bulk DMA read of size {} at offset {}, successfully read {} bytes",
                    _trace_prefix, range_size, offset, ret.size());
            return ret;
        } catch (...) {
            tracing::trace(_trace_state, "{} failed a bulk DMA read of size {} at offset {}: {}",
                    _trace_prefix, range_size, offset, std::current_exception());
            throw;
        }
    });
}

namespace tracing {

file make_traced_file(file f, trace_state_ptr trace_state, sstring trace_prefix) {
    return file(::make_shared<traced_file_impl>(std::move(f), std::move(trace_state), std::move(trace_prefix)));
}

} // namespace tracing

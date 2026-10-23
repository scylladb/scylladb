/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option): a seastar::file implementation which reads an
// sstable component through the cache. It is used for the index components,
// which the sstable index readers read through a seastar::file rather than a
// data_source. See sstables/object_storage_page_cache_file.hh.

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <seastar/core/layered_file.hh>
#include <seastar/util/memory-data-source.hh>
#include "sstables/object_storage_page_cache_file.hh"
#include "sstables/object_storage_page_cache_geometry.hh"
#include "sstables/object_storage_page_cache_source.hh"
#include "utils/log.hh"

namespace sstables {

extern logging::logger sstlog;

namespace {

class page_cached_file_impl : public layered_file_impl {
    shared_ptr<object_storage_page_cache> _cache;
    sstable_id _sst;
    component_type _component;
    populate_disk_cache _populate;
    lw_shared_ptr<object_storage_page_cache_table_stats> _table_stats;
    // Created on the first read, from the size of the underlying file.
    shared_ptr<const page_geometry> _geometry;
public:
    page_cached_file_impl(shared_ptr<object_storage_page_cache> cache, sstable_id sst, component_type component, file underlying,
            populate_disk_cache populate, lw_shared_ptr<object_storage_page_cache_table_stats> table_stats)
        : layered_file_impl(std::move(underlying))
        , _cache(std::move(cache))
        , _sst(sst)
        , _component(component)
        , _populate(populate)
        , _table_stats(std::move(table_stats))
    {}

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) override {
        auto geometry = co_await get_geometry();
        // Like any seastar::file, return less than requested at the end of the file.
        if (offset >= geometry->file_size() || range_size == 0) {
            co_return temporary_buffer<uint8_t>();
        }
        uint64_t len = std::min<uint64_t>(range_size, geometry->file_size() - offset);
        auto src = make_page_cached_source(*_cache, _sst, _component, geometry, offset, len,
                [f = _underlying_file] (uint64_t offset, uint64_t length) mutable -> future<data_source> {
            // A single request to the object store, for the whole range.
            auto buf = co_await f.dma_read_bulk<char>(offset, length);
            std::vector<temporary_buffer<char>> bufs;
            bufs.push_back(std::move(buf));
            co_return data_source(std::make_unique<util::memory_data_source>(std::move(bufs)));
        }, _populate, _table_stats);
        std::exception_ptr ex;
        temporary_buffer<uint8_t> result;
        try {
            result = co_await read_all(src, len);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await src.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
        co_return result;
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) override {
        auto buf = co_await dma_read_bulk(pos, len, intent);
        std::copy_n(buf.get(), buf.size(), reinterpret_cast<uint8_t*>(buffer));
        co_return buf.size();
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        size_t len = 0;
        for (auto& v : iov) {
            len += v.iov_len;
        }
        auto buf = co_await dma_read_bulk(pos, len, intent);
        size_t off = 0;
        for (auto& v : iov) {
            auto n = std::min(v.iov_len, buf.size() - off);
            std::copy_n(buf.get() + off, n, reinterpret_cast<uint8_t*>(v.iov_base));
            off += n;
        }
        co_return off;
    }

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent* intent) override {
        return get_file_impl(_underlying_file)->write_dma(pos, buffer, len, intent);
    }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        return get_file_impl(_underlying_file)->write_dma(pos, std::move(iov), intent);
    }
    virtual future<> flush() override {
        return _underlying_file.flush();
    }
    virtual future<struct stat> stat() override {
        return _underlying_file.stat();
    }
    virtual future<> truncate(uint64_t length) override {
        return _underlying_file.truncate(length);
    }
    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return _underlying_file.discard(offset, length);
    }
    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return _underlying_file.allocate(position, length);
    }
    virtual future<uint64_t> size() override {
        return _underlying_file.size();
    }
    virtual future<> close() override {
        return _underlying_file.close();
    }
    // Resharding (on startup with a different number of shards, or when
    // loading sstables from the upload directory) opens an sstable which spans
    // several shards on each of them, with duplicates of its files (see
    // sstable::get_open_info()). The default file_impl::dup() refuses, which
    // would fail the resharding, so return a duplicate of the underlying file.
    // It reads directly from the object store, without the cache: resharding
    // reads the sstable once to rewrite it, which should not be cached, and
    // the cache of another shard is out of reach anyway.
    virtual std::unique_ptr<file_handle_impl> dup() override {
        // Count and log it, in case the duplicate is ever used for reads which
        // should use the cache.
        _cache->get_stats().dup_bypasses++;
        sstlog.debug("{} component of sstable {} is duplicated for another shard, which will read it without the object storage page cache",
                _component, _sst);
        return get_file_impl(_underlying_file)->dup();
    }
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return _underlying_file.list_directory(std::move(next));
    }

private:
    future<shared_ptr<const page_geometry>> get_geometry() {
        if (!_geometry) {
            auto size = co_await _underlying_file.size();
            if (!_geometry) {
                _geometry = make_shared<fixed_size_page_geometry>(size);
            }
        }
        co_return _geometry;
    }

    // Reads all of the source, which has exactly "len" bytes, into one buffer.
    static future<temporary_buffer<uint8_t>> read_all(data_source& src, uint64_t len) {
        auto first = co_await src.get();
        if (first.size() == len) {
            // The common case of a read of a single page: no need to copy.
            co_return temporary_buffer<uint8_t>(reinterpret_cast<uint8_t*>(first.get_write()), first.size(), first.release());
        }
        temporary_buffer<uint8_t> result(len);
        size_t off = 0;
        for (auto buf = std::move(first); !buf.empty(); buf = co_await src.get()) {
            std::copy_n(buf.get(), buf.size(), result.get_write() + off);
            off += buf.size();
        }
        co_return result;
    }
};

} // anonymous namespace

file make_page_cached_file(shared_ptr<object_storage_page_cache> cache, sstable_id sst, component_type component, file underlying,
        populate_disk_cache populate, lw_shared_ptr<object_storage_page_cache_table_stats> table_stats) {
    return file(make_shared<page_cached_file_impl>(std::move(cache), sst, component, std::move(underlying), populate,
            std::move(table_stats)));
}

} // namespace sstables

/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option): a data_source which reads a byte range of an
// sstable component through the cache. See
// sstables/object_storage_page_cache_source.hh.

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <fmt/format.h>
#include <seastar/core/coroutine.hh>
#include "sstables/object_storage_page_cache_source.hh"
#include "sstables/sstables_manager.hh"
#include "utils/log.hh"

namespace sstables {

extern logging::logger sstlog;

// A cache which fails - say, because its disk is full or broken - might
// fail every read, so its errors are logged at most once a minute.
static thread_local logging::logger::rate_limit cache_error_rate_limit{std::chrono::minutes(1)};

namespace {

class page_cached_source_impl : public data_source_impl {
    object_storage_page_cache& _cache;
    sstable_id _sst;
    component_type _component;
    shared_ptr<const page_geometry> _geometry;
    object_storage_range_reader _read_range;
    populate_disk_cache _populate;
    // The next byte to return, and the end of the requested range.
    uint64_t _pos;
    uint64_t _end;
    // The end of the requested range, extended to the end of its last page.
    uint64_t _pages_end;
    // The cursor which reads the cache, from the first page which the source
    // looks up in the cache until it misses, or fails.
    std::unique_ptr<object_storage_page_cache::page_cursor> _cursor;
    // The request to the object store, once it has started, and the position
    // of the next byte it will return, which is always at a page boundary.
    std::optional<input_stream<char>> _object;
    uint64_t _object_pos = 0;
public:
    page_cached_source_impl(object_storage_page_cache& cache, sstable_id sst, component_type component,
            shared_ptr<const page_geometry> geometry, uint64_t offset, uint64_t length,
            object_storage_range_reader read_range, populate_disk_cache populate)
        : _cache(cache)
        , _sst(sst)
        , _component(component)
        , _geometry(std::move(geometry))
        , _read_range(std::move(read_range))
        , _populate(populate)
        , _pos(offset)
        , _end(offset + length)
        , _pages_end(length ? _geometry->pages_covering(offset, offset + length).end : _end)
    {}

    virtual future<temporary_buffer<char>> get() override {
        if (_pos >= _end) {
            co_return temporary_buffer<char>();
        }
        auto page = _geometry->page_at(_pos);
        temporary_buffer<char> data;
        if (_object && page.start - _object_pos > max_read_through_skip) {
            // A long skip: close the request, rather than download the pages
            // it skipped, and continue from the cache.
            co_await _object->close();
            _object.reset();
        }
        if (!_object) {
            data = co_await get_cached_page(page);
            if (data.empty()) {
                // The source reads the rest of the range from the object
                // store, so it doesn't need the cursor - unless it skips far,
                // and then it looks up the cache with a new cursor.
                co_await close_cursor();
                _object = input_stream<char>(co_await _read_range(page.start, _pages_end - page.start));
                _object_pos = page.start;
            }
        }
        if (_object) {
            // Read the pages that a short skip() passed over, to cache them
            // too (the request to the object store returns them anyway).
            while (_object_pos < page.start) {
                co_await read_page_from_object(_geometry->page_at(_object_pos));
            }
            data = co_await read_page_from_object(page);
        }
        // Return only the part of the page inside the requested range.
        uint64_t len = std::min(page.end, _end) - _pos;
        data.trim_front(_pos - page.start);
        data.trim(len);
        _pos += len;
        co_return data;
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        // Like data_source_impl's default skip(), refuse to skip beyond the end.
        if (n > _end - _pos) {
            throw std::runtime_error("premature end of stream");
        }
        _pos += n;
        co_return temporary_buffer<char>();
    }

    virtual future<> close() override {
        co_await close_cursor();
        if (_object) {
            co_await _object->close();
        }
    }

private:
    // Returns the given page from the cache, or an empty buffer if it is not
    // cached. Starts a cursor at the first page which it looks up, and reads
    // the following pages with it.
    //
    // The object store holds all the pages, so a failure to read the cache
    // doesn't fail the read: the page is read from the object store, as if
    // it wasn't cached.
    future<temporary_buffer<char>> get_cached_page(page_geometry::page_bounds page) {
        temporary_buffer<char> data;
        std::exception_ptr ex;
        try {
            if (!_cursor) {
                _cursor = _cache.read_pages(_sst, _component, page.start, _pages_end, _populate);
            }
            data = co_await _cursor->get_page(page.start);
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            sstlog.log(log_level::warn, cache_error_rate_limit,
                    "Failed to read the object storage page cache of {} component of sstable {}, reading from the object store: {}",
                    _component, _sst, ex);
            co_await close_cursor();
        }
        // A cached page of the wrong size cannot be a page of this component,
        // so it is treated as missing, and is read again from the object store.
        if (data.size() != page.end - page.start) {
            data = {};
        }
        co_return data;
    }

    future<> close_cursor() noexcept {
        if (_cursor) {
            co_await _cursor->close();
            _cursor.reset();
        }
    }

    // Reads the given page, which starts at _object_pos, from the object
    // store, and caches it, unless this source must not populate the cache.
    future<temporary_buffer<char>> read_page_from_object(page_geometry::page_bounds page) {
        auto size = page.end - page.start;
        auto data = co_await _object->read_exactly(size);
        if (data.size() != size) {
            throw std::runtime_error(fmt::format("{} component of sstable {} ended at {}, before the end of the page [{}, {})",
                    _component, _sst, page.start + data.size(), page.start, page.end));
        }
        _object_pos = page.end;
        if (_populate) {
            // A failure to cache the page doesn't fail the read, which has the
            // page anyway - but this source stops caching pages.
            try {
                co_await _cache.put_page(_sst, _component, page.start, data.share());
            } catch (...) {
                _populate = populate_disk_cache::no;
                sstlog.log(log_level::warn, cache_error_rate_limit,
                        "Failed to write the object storage page cache of {} component of sstable {}: {}",
                        _component, _sst, std::current_exception());
            }
        }
        co_return data;
    }
};

} // anonymous namespace

data_source make_page_cached_source(object_storage_page_cache& cache, sstable_id sst, component_type component,
        shared_ptr<const page_geometry> geometry, uint64_t offset, uint64_t length,
        object_storage_range_reader read_range, populate_disk_cache populate) {
    return data_source(std::make_unique<page_cached_source_impl>(cache, sst, component, std::move(geometry),
            offset, length, std::move(read_range), populate));
}

} // namespace sstables

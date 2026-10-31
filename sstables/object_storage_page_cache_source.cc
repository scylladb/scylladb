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
#include <deque>
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
    // About how many bytes to read from the cache in one query. Each query
    // has a fixed overhead, so reading one page at a time would be slow, but
    // a range can be a whole sstable, which must not be held in memory at
    // once. So read as much as the sstable reader reads at once by default.
    static constexpr uint64_t bytes_per_read = default_sstable_buffer_size;

    object_storage_page_cache& _cache;
    sstable_id _sst;
    component_type _component;
    shared_ptr<const page_geometry> _geometry;
    object_storage_range_reader _read_range;
    populate_disk_cache _populate;
    // The statistics of hits and misses of this kind of read, and the
    // statistics of the sstable's table (if given).
    object_storage_page_cache_read_stats& _read_stats;
    lw_shared_ptr<object_storage_page_cache_table_stats> _table_stats;
    // The permit of the read, if given, which tracks the memory of the
    // pages in _cached.
    std::optional<reader_permit> _permit;
    // The next byte to return, and the end of the requested range.
    uint64_t _pos;
    uint64_t _end;
    // The end of the requested range, extended to the end of its last page.
    uint64_t _pages_end;
    // How many pages to read from the cache in one query: about
    // bytes_per_read, judging by the average page size.
    size_t _pages_per_read;
    // Pages which were read from the cache, but not yet returned, and their
    // memory, tracked in _permit (if given).
    std::deque<object_storage_page_cache::page> _cached;
    std::optional<reader_permit::resource_units> _cached_units;
    // The request to the object store, once it has started, and the position
    // of the next byte it will return, which is always at a page boundary.
    std::optional<input_stream<char>> _object;
    uint64_t _object_pos = 0;
public:
    page_cached_source_impl(object_storage_page_cache& cache, sstable_id sst, component_type component,
            shared_ptr<const page_geometry> geometry, uint64_t offset, uint64_t length,
            object_storage_range_reader read_range, populate_disk_cache populate,
            lw_shared_ptr<object_storage_page_cache_table_stats> table_stats, std::optional<reader_permit> permit)
        : _cache(cache)
        , _sst(sst)
        , _component(component)
        , _geometry(std::move(geometry))
        , _read_range(std::move(read_range))
        , _populate(populate)
        , _read_stats(populate ? cache.get_stats().populating : cache.get_stats().non_populating)
        , _table_stats(std::move(table_stats))
        , _permit(std::move(permit))
        , _pos(offset)
        , _end(offset + length)
        , _pages_end(length ? _geometry->pages_covering(offset, offset + length).end : _end)
        , _pages_per_read(std::max<uint64_t>(1, bytes_per_read / _geometry->average_page_size()))
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
                _cached.clear();
                track_cached();
                _cache.get_stats().object_reads++;
                _cache.get_stats().object_read_bytes += _pages_end - page.start;
                if (_table_stats) {
                    _table_stats->object_reads++;
                    _table_stats->object_read_bytes += _pages_end - page.start;
                }
                _object = input_stream<char>(co_await _read_range(page.start, _pages_end - page.start));
                _object_pos = page.start;
            } else {
                _read_stats.hits++;
                if (_table_stats) {
                    table_read_stats().hits++;
                }
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
        if (_object) {
            co_await _object->close();
        }
    }

private:
    object_storage_page_cache_read_stats& table_read_stats() {
        return &_read_stats == &_cache.get_stats().populating ? _table_stats->populating : _table_stats->non_populating;
    }

    // Returns the given page from the cache, or an empty buffer if it is not
    // cached. Reads from the cache only when the pages it returned last time
    // are used up, and then reads a batch of pages from this one onward.
    //
    // The object store holds all the pages, so a failure to read the cache
    // doesn't fail the read: the page is read from the object store, as if
    // it wasn't cached.
    future<temporary_buffer<char>> get_cached_page(page_geometry::page_bounds page) {
        while (!_cached.empty() && _cached.front().offset < page.start) {
            _cached.pop_front();
        }
        if (_cached.empty()) {
            std::vector<object_storage_page_cache::page> pages;
            try {
                pages = co_await _cache.get_pages(_sst, _component, page.start, _pages_end, _pages_per_read, _populate);
            } catch (...) {
                _cache.get_stats().read_errors++;
                sstlog.log(log_level::warn, cache_error_rate_limit,
                        "Failed to read the object storage page cache of {} component of sstable {}, reading from the object store: {}",
                        _component, _sst, std::current_exception());
            }
            _cached.assign(std::make_move_iterator(pages.begin()), std::make_move_iterator(pages.end()));
        }
        // A cached page of the wrong size cannot be a page of this component,
        // so it is treated as missing, and is read again from the object store.
        temporary_buffer<char> data;
        if (!_cached.empty() && _cached.front().offset == page.start
                && _cached.front().data.size() == page.end - page.start) {
            data = std::move(_cached.front().data);
            _cached.pop_front();
        }
        track_cached();
        co_return data;
    }

    // Tracks the memory of the pages in _cached in the read's permit, if
    // given, without waiting for memory. The pages which the source returned
    // are no longer its own: whoever holds them tracks them (see
    // make_page_cached_source()).
    void track_cached() {
        if (!_permit) {
            return;
        }
        ssize_t bytes = 0;
        for (const auto& p : _cached) {
            bytes += p.data.size();
        }
        if (_cached_units) {
            _cached_units->reset_to(reader_resources::with_memory(bytes));
        } else {
            _cached_units = _permit->consume_memory(bytes);
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
        _read_stats.misses++;
        if (_table_stats) {
            table_read_stats().misses++;
        }
        if (_populate) {
            // A failure to cache the page doesn't fail the read, which has the
            // page anyway - but this source stops caching pages.
            try {
                co_await _cache.put_page(_sst, _component, page.start, data.share());
            } catch (...) {
                _cache.get_stats().write_errors++;
                _populate = populate_disk_cache::no;
                sstlog.log(log_level::warn, cache_error_rate_limit,
                        "Failed to write the object storage page cache of {} component of sstable {}: {}",
                        _component, _sst, std::current_exception());
            }
        }
        co_return data;
    }
};

class permit_tracked_source_impl : public data_source_impl {
    data_source _source;
    reader_permit _permit;
public:
    permit_tracked_source_impl(data_source source, reader_permit permit)
        : _source(std::move(source)), _permit(std::move(permit)) {}
    virtual future<temporary_buffer<char>> get() override {
        auto buf = co_await _source.get();
        auto units = _permit.consume_memory(buf.size());
        co_return make_tracked_temporary_buffer(std::move(buf), std::move(units));
    }
    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        auto buf = co_await _source.skip(n);
        auto units = _permit.consume_memory(buf.size());
        co_return make_tracked_temporary_buffer(std::move(buf), std::move(units));
    }
    virtual future<> close() override {
        return _source.close();
    }
};

} // anonymous namespace

data_source make_page_cached_source(object_storage_page_cache& cache, sstable_id sst, component_type component,
        shared_ptr<const page_geometry> geometry, uint64_t offset, uint64_t length,
        object_storage_range_reader read_range, populate_disk_cache populate,
        lw_shared_ptr<object_storage_page_cache_table_stats> table_stats, std::optional<reader_permit> permit) {
    return data_source(std::make_unique<page_cached_source_impl>(cache, sst, component, std::move(geometry),
            offset, length, std::move(read_range), populate, std::move(table_stats), std::move(permit)));
}

data_source make_permit_tracked_source(data_source source, reader_permit permit) {
    return data_source(std::make_unique<permit_tracked_source_impl>(std::move(source), std::move(permit)));
}

} // namespace sstables

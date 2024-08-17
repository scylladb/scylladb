/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "reader_permit.hh"
#include "utils/assert.hh"
#include "utils/div_ceil.hh"
#include "utils/bptree.hh"
#include "utils/logalloc.hh"
#include "utils/lru.hh"
#include "utils/error_injection.hh"
#include "tracing/trace_state.hh"
#include "utils/cached_file_stats.hh"

#include <seastar/core/file.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

using namespace seastar;

/// \brief A read-through cache of a file.
///
/// Caches contents with page granularity (4 KiB).
/// Cached pages are evicted by the LRU or manually using the invalidate_*() method family, or when the object is destroyed.
///
/// Concurrent reading is allowed.
///
/// The object is movable but this is only allowed before readers are created.
///
class cached_file {
public:
    // Must be aligned to _file.disk_read_dma_alignment(). 4K is always safe.
    static constexpr size_t page_size = 4096;

    // The content of the underlying file (_file) is divided into pages
    // of equal size (page_size). This type is used to identify pages.
    // Pages are assigned consecutive identifiers starting from 0.
    using page_idx_type = uint64_t;

    // Represents the number of pages in a sequence
    using page_count_type = uint64_t;

    using offset_type = uint64_t;

private:
    class cached_page final : public index_evictable {
    public:
        cached_file* parent;
        page_idx_type idx;
        logalloc::lsa_buffer _lsa_buf;
        temporary_buffer<char> _buf; // Empty when not shared. May mirror _lsa_buf when shared.
        size_t _use_count = 0;
    public:
        struct cached_page_del {
            void operator()(cached_page* cp) {
                if (--cp->_use_count == 0) {
                    cp->parent->_metrics.bytes_in_std -= cp->_buf.size();
                    cp->_buf = {};
                    cp->parent->_lru.add(*cp);
                }
            }
        };

        using ptr_type = std::unique_ptr<cached_page, cached_page_del>;

        // As long as any ptr_type is alive, this cached_page will not be destroyed
        // because it will not be linked in the LRU.
        ptr_type share() noexcept {
            if (_use_count++ == 0) {
                if (is_linked()) {
                    parent->_lru.remove(*this);
                }
            }
            return std::unique_ptr<cached_page, cached_page_del>(this);
        }
    public:
        explicit cached_page(cached_file* parent, page_idx_type idx, temporary_buffer<char> buf)
            : parent(parent)
            , idx(idx)
            , _buf(std::move(buf))
        {
            _lsa_buf = parent->_region.alloc_buf(_buf.size());
            parent->_metrics.bytes_in_std += _buf.size();
            std::copy(_buf.begin(), _buf.end(), _lsa_buf.get());
        }

        cached_page(cached_page&&) noexcept {
            // The move constructor is required by allocation_strategy::construct() due to generic bplus::tree,
            // but the object is always allocated in the standard allocator context so never actually moved.
            // We cannot properly implement the move constructor because "this" is captured in various places.
            abort();
        }

        ~cached_page() {
            SCYLLA_ASSERT(!_use_count);
        }

        void on_evicted() noexcept override;

        temporary_buffer<char> get_buf() {
            auto self = share();
            if (!_buf) {
                _buf = temporary_buffer<char>(_lsa_buf.size());
                parent->_metrics.bytes_in_std += _lsa_buf.size();
                std::copy(_lsa_buf.get(), _lsa_buf.get() + _lsa_buf.size(), _buf.get_write());
            }
            // Holding to a temporary buffer holds the cached page so that the buffer can be reused by concurrent hits.
            // Also, sharing cached_page keeps the temporary_buffer's storage alive.
            return temporary_buffer<char>(_buf.get_write(), _buf.size(), make_deleter([self = std::move(self)] {}));
        }

        // Returns a buffer which reflects contents of this page.
        // The buffer will not prevent eviction.
        // The buffer is invalidated when the page is evicted or when the owning LSA region invalidates references.
        temporary_buffer<char> get_buf_weak() {
            return temporary_buffer<char>(_lsa_buf.get(), _lsa_buf.size(), deleter());
        }

        size_t size_in_allocator() {
            // lsa_buf occupies 4K in LSA even if the buf size is smaller.
            // _buf is transient and not accounted here.
            return page_size;
        }
    };

    struct page_idx_less_comparator {
        bool operator()(page_idx_type lhs, page_idx_type rhs) const noexcept {
            return lhs < rhs;
        }
    };

    file _file;
    sstring _file_name; // for logging / tracing
    cached_file_stats& _metrics;
    lru& _lru;
    logalloc::region& _region;
    logalloc::allocating_section _as;

    using cache_type = bplus::tree<page_idx_type, cached_page, page_idx_less_comparator, 12, bplus::key_search::linear>;
    cache_type _cache;

    const offset_type _size;
    offset_type _cached_bytes = 0;

    offset_type _last_page_size;
    page_idx_type _last_page;
private:
    future<cached_page::ptr_type> get_page_ptr(page_idx_type idx,
            page_count_type read_ahead,
            tracing::trace_state_ptr trace_state) {
        auto i = _cache.lower_bound(idx);
        if (i != _cache.end() && i->idx == idx) {
            ++_metrics.page_hits;
            tracing::trace(trace_state, "page cache hit: file={}, page={}", _file_name, idx);
            cached_page& cp = *i;
            return make_ready_future<cached_page::ptr_type>(cp.share());
        }
        tracing::trace(trace_state, "page cache miss: file={}, page={}, readahead={}", _file_name, idx, read_ahead);
        ++_metrics.page_misses;
        size_t size = (idx + read_ahead) > _last_page
                ? (_last_page_size + (_last_page - idx) * page_size)
                : read_ahead * page_size;
        return _file.dma_read_exactly<char>(idx * page_size, size)
            .then([this, idx] (temporary_buffer<char>&& buf) mutable {
                cached_page::ptr_type first_page;
                while (buf.size()) {
                    auto this_size = std::min(page_size, buf.size());
                    // _cache.emplace() needs to run under allocating section even though it lives in the std space
                    // because bplus::tree operations are not reentrant, so we need to prevent memory reclamation.
                    auto [cp, missed] = _as(_region, [&] {
                        auto this_buf = buf.share();
                        this_buf.trim(this_size);
                        return _cache.emplace(idx, this, idx, std::move(this_buf));
                    });
                    buf.trim_front(this_size);
                    ++idx;
                    if (missed) {
                        ++_metrics.page_populations;
                        _metrics.cached_bytes += cp->size_in_allocator();
                        _cached_bytes += cp->size_in_allocator();
                    }
                    // pages read ahead will be placed into LRU, as there's no guarantee they will be fetched later.
                    cached_page::ptr_type ptr = cp->share();
                    if (!first_page) {
                        first_page = std::move(ptr);
                    }
                }
                utils::get_local_injector().inject("cached_file_get_first_page", []() {
                    throw std::bad_alloc();
                });
                return first_page;
            });
    }
    future<temporary_buffer<char>> get_page(page_idx_type idx,
                                            page_count_type count,
                                            tracing::trace_state_ptr trace_state) {
        return get_page_ptr(idx, count, std::move(trace_state)).then([] (cached_page::ptr_type cp) {
            return cp->get_buf();
        });
    }
public:
    class page_view {
        cached_page::ptr_type _page;
        size_t _offset;
        size_t _size;
        std::optional<reader_permit::resource_units> _units;
    public:
        page_view() = default;
        page_view(size_t offset, size_t size, cached_page::ptr_type page, std::optional<reader_permit::resource_units> units)
                : _page(std::move(page))
                , _offset(offset)
                , _size(size)
                , _units(std::move(units))
        {}

        // The returned buffer is valid only until the LSA region associated with cached_file invalidates references.
        temporary_buffer<char> get_buf() {
            auto buf = _page->get_buf_weak();
            buf.trim(_size);
            buf.trim_front(_offset);
            return buf;
        }

        operator bool() const { return bool(_page); }
    };

    // Generator of subsequent pages of data reflecting the contents of the file.
    // Single-user.
    class stream {
        cached_file* _cached_file;
        std::optional<reader_permit> _permit;
        page_idx_type _page_idx;
        offset_type _offset_in_page;
        offset_type _size_hint;
        tracing::trace_state_ptr _trace_state;
    private:
        std::optional<reader_permit::resource_units> get_page_units(size_t size = page_size) {
            return _permit
                ? std::make_optional(_permit->consume_memory(size))
                : std::nullopt;
        }
    public:
        // Creates an empty stream.
        stream()
            : _cached_file(nullptr)
        { }

        stream(cached_file& cf, std::optional<reader_permit> permit, tracing::trace_state_ptr trace_state,
                page_idx_type start_page, offset_type start_offset_in_page, offset_type size_hint)
            : _cached_file(&cf)
            , _permit(std::move(permit))
            , _page_idx(start_page)
            , _offset_in_page(start_offset_in_page)
            , _size_hint(size_hint)
            , _trace_state(std::move(trace_state))
        { }

        // Yields the next chunk of data.
        // Returns empty buffer when end-of-stream is reached.
        // Calls must be serialized.
        // This instance must be kept alive until the returned future resolves.
        future<temporary_buffer<char>> next() {
            if (!_cached_file || _page_idx > _cached_file->_last_page) {
                return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>());
            }
            auto units = get_page_units(_size_hint);
            page_count_type readahead = div_ceil(_size_hint, page_size);
            _size_hint = page_size;
            return _cached_file->get_page(_page_idx, readahead, _trace_state).then(
                    [units = std::move(units), this] (temporary_buffer<char> page) mutable {
                if (_page_idx == _cached_file->_last_page) {
                    page.trim(_cached_file->_last_page_size);
                }
                if (units) {
                    units = get_page_units();
                    page = temporary_buffer<char>(page.get_write(), page.size(),
                                                  make_object_deleter(page.release(), std::move(*units)));
                }
                page.trim_front(_offset_in_page);
                _offset_in_page = 0;
                ++_page_idx;
                return page;
            });
        }

        // Yields the next chunk of data.
        // Returns empty buffer when end-of-stream is reached.
        // Calls must be serialized.
        // This instance must be kept alive until the returned future resolves.
        future<page_view> next_page_view() {
            if (!_cached_file || _page_idx > _cached_file->_last_page) {
                return make_ready_future<page_view>(page_view());
            }
            auto units = get_page_units(_size_hint);
            page_count_type readahead = div_ceil(_size_hint, page_size);
            _size_hint = page_size;
            return _cached_file->get_page_ptr(_page_idx, readahead, _trace_state).then(
                    [this, units = std::move(units)] (cached_page::ptr_type page) mutable {
                size_t size = _page_idx == _cached_file->_last_page
                        ? _cached_file->_last_page_size
                        : page_size;
                units = get_page_units(page_size);
                page_view buf(_offset_in_page, size, std::move(page), std::move(units));
                _offset_in_page = 0;
                ++_page_idx;
                return buf;
            });
        }
    };

    void on_evicted(cached_page& p) {
        _metrics.cached_bytes -= p.size_in_allocator();
        _cached_bytes -= p.size_in_allocator();
        ++_metrics.page_evictions;
    }

    size_t evict_range(cache_type::iterator start, cache_type::iterator end) noexcept {
      return with_allocator(standard_allocator(), [&] {
        size_t count = 0;
        auto disposer = [] (auto* p) noexcept {};
        while (start != end) {
            if (start->is_linked()) {
                ++count;
                _lru.remove(*start);
                on_evicted(*start);
                start = start.erase_and_dispose(disposer, page_idx_less_comparator());
            } else {
                ++start;
            }
        }
        return count;
      });
    }
public:
    /// \brief Constructs a cached_file.
    ///
    /// The cached area will reflect subset of f from the byte range [start, start + size).
    ///
    /// \param m Metrics object which should be updated from operations on this object.
    ///          The metrics object can be shared by many cached_file instances, in which case it
    ///          will reflect the sum of operations on all cached_file instances.
    cached_file(file f, cached_file_stats& m, lru& l, logalloc::region& reg, offset_type size, sstring file_name = {})
        : _file(std::move(f))
        , _file_name(std::move(file_name))
        , _metrics(m)
        , _lru(l)
        , _region(reg)
        , _cache(page_idx_less_comparator())
        , _size(size)
    {
        offset_type last_byte_offset = _size ? (_size - 1) : 0;
        _last_page_size = (last_byte_offset % page_size) + (_size ? 1 : 0);
        _last_page = last_byte_offset / page_size;
    }

    cached_file(cached_file&&) = delete; // captured this
    cached_file(const cached_file&) = delete;

    ~cached_file() {
        evict_range(_cache.begin(), _cache.end());
        SCYLLA_ASSERT(_cache.empty());
    }

    /// \brief Invalidates [start, end) or less.
    ///
    /// Invariants:
    ///
    ///   - all bytes outside [start, end) which were cached before the call will still be cached.
    ///
    void invalidate_at_most(offset_type start, offset_type end, tracing::trace_state_ptr trace_state = {}) {
        auto lo_page = start / page_size
                       // If start is 0 then we can drop the containing page
                       // Otherwise we cannot drop the page.
                       + bool(start % page_size) * bool(start != 0);

        auto hi_page = (end) / page_size;

        if (lo_page < hi_page) {
            auto count = evict_range(_cache.lower_bound(lo_page), _cache.lower_bound(hi_page));
            if (count) {
                tracing::trace(trace_state, "page cache: evicted {} page(s) in [{}, {}), file={}", count,
                    lo_page, hi_page, _file_name);
            }
        }
    }

    /// \brief Equivalent to \ref invalidate_at_most(0, end).
    void invalidate_at_most_front(offset_type end, tracing::trace_state_ptr trace_state = {}) {
        auto hi_page = end / page_size;
        auto count = evict_range(_cache.begin(), _cache.lower_bound(hi_page));
        if (count) {
            tracing::trace(trace_state, "page cache: evicted {} page(s) in [0, {}), file={}", count,
                hi_page, _file_name);
        }
    }

    /// \brief Read from the file
    ///
    /// Returns a stream with data which starts at position pos in the area managed by this instance.
    /// This cached_file instance must outlive the returned stream and buffers returned by the stream.
    /// The stream does not do any read-ahead.
    ///
    /// \param pos The offset of the first byte to read, relative to the cached file area.
    /// \param permit Holds reader_permit under which returned buffers should be accounted.
    ///               When disengaged, no accounting is done.
    stream read(offset_type global_pos, std::optional<reader_permit> permit,
                tracing::trace_state_ptr trace_state = {},
                size_t size_hint = page_size) {
        if (global_pos >= _size) {
            return stream();
        }
        auto offset = global_pos % page_size;
        auto page_idx = global_pos / page_size;
        return stream(*this, std::move(permit), std::move(trace_state), page_idx, offset, size_hint);
    }

    /// \brief Returns the number of bytes in the area managed by this instance.
    offset_type size() const {
        return _size;
    }

    /// \brief Returns the number of bytes cached.
    size_t cached_bytes() const {
        return _cached_bytes;
    }

    /// \brief Returns the underlying file.
    file& get_file() {
        return _file;
    }

    logalloc::region& region() {
        return _region;
    }

    // Evicts all unused pages.
    // Pages which are used are not removed.
    future<> evict_gently() {
        auto i = _cache.begin();
        while (i != _cache.end()) {
            if (i->is_linked()) {
                _lru.remove(*i);
                on_evicted(*i);
                i = i.erase(page_idx_less_comparator());
            } else {
                ++i;
            }
            if (need_preempt() && i != _cache.end()) {
                auto key = i->idx;
                co_await coroutine::maybe_yield();
                i = _cache.lower_bound(key);
            }
        }
    }
};

inline
void cached_file::cached_page::on_evicted() noexcept {
    parent->on_evicted(*this);
    with_allocator(standard_allocator(), [this] {
        cached_file::cache_type::iterator it(this);
        it.erase(page_idx_less_comparator());
    });
}

class cached_file_impl : public file_impl {
    cached_file& _cf;
    tracing::trace_state_ptr _trace_state;
private:
    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation");
    }
public:
    cached_file_impl(cached_file& cf, tracing::trace_state_ptr trace_state = {})
        : file_impl(*get_file_impl(cf.get_file()))
        , _cf(cf)
        , _trace_state(std::move(trace_state))
    { }

    // unsupported
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override { unsupported(); }
    virtual future<> flush(void) override { unsupported(); }
    virtual future<> truncate(uint64_t length) override { unsupported(); }
    virtual future<> discard(uint64_t offset, uint64_t length) override { unsupported(); }
    virtual future<> allocate(uint64_t position, uint64_t length) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override { unsupported(); }

    // delegating
    virtual future<struct stat> stat(void) override { return _cf.get_file().stat(); }
    virtual future<uint64_t> size(void) override { return _cf.get_file().size(); }
    virtual future<> close() override { return _cf.get_file().close(); }
    virtual std::unique_ptr<seastar::file_handle_impl> dup() override { return get_file_impl(_cf.get_file())->dup(); }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t size, io_intent* intent) override {
        return do_with(_cf.read(offset, std::nullopt, _trace_state, size), size, temporary_buffer<uint8_t>(),
                [this, size] (cached_file::stream& s, size_t& size_left, temporary_buffer<uint8_t>& result) {
            if (size_left == 0) {
                return make_ready_future<temporary_buffer<uint8_t>>(std::move(result));
            }
            return repeat([this, &s, &size_left, &result, size] {
                return s.next().then([this, &size_left, &result, size] (temporary_buffer<char> buf) {
                    if (!buf) {
                        throw seastar::file::eof_error();
                    }
                    if (!result) {
                        if (buf.size() >= size_left) {
                            result = temporary_buffer<uint8_t>(reinterpret_cast<uint8_t*>(buf.get_write()), size_left, buf.release());
                            return stop_iteration::yes;
                        }
                        result = temporary_buffer<uint8_t>::aligned(_memory_dma_alignment, size_left);
                    }
                    size_t this_len = std::min(buf.size(), size_left);
                    std::copy(buf.begin(), buf.begin() + this_len, result.get_write() + (size - size_left));
                    size_left -= this_len;
                    return stop_iteration(size_left == 0);
                });
            }).then([&] {
                return std::move(result);
            });
        });
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        unsupported(); // FIXME
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
        unsupported(); // FIXME
    }
};

// Creates a seastar::file object which will read through a given cached_file instance.
// The cached_file object must be kept alive as long as the file is in use.
inline
file make_cached_seastar_file(cached_file& cf) {
    return file(make_shared<cached_file_impl>(cf));
}

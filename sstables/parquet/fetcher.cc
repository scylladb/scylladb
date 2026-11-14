/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/fetcher.hh"

#include <algorithm>
#include <limits>

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/defer.hh>

#include "sstables/exceptions.hh"
#include "utils/exceptions.hh"
#include "utils/log.hh"

namespace sstables::parquet {

static logging::logger fetcher_logger("parquet_fetcher");

static constexpr std::pair<uint32_t, uint32_t> no_page{std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max()};
// The longest extension of a single-partition read, in steps.
static constexpr uint64_t max_window_steps = 1 << 20;
// The most row groups without data in the projection that one extension
// of the window passes. Each costs a check per projected column. Read-ahead
// is optional, so the window stops there, and goes on at the next
// extension or restarts at the next miss.
static constexpr uint64_t max_skipped_row_groups = 64;

// Whether a read failed with std::bad_alloc, which is also how the reader
// concurrency semaphore fails the memory requests of a read that it
// registers as inactive. Such a read is retried once when a caller asks
// for its data.
static bool failed_for_memory(std::exception_ptr e) noexcept {
    return try_catch<std::bad_alloc>(e) != nullptr;
}

fetcher::fetcher(file f, const file_summary& s, metadata_cache& cache, std::vector<leaf_index> columns,
        read_direction direction, read_hint hint, fetch_policy policy, std::optional<reader_permit> permit)
    : _file(std::move(f))
    , _summary(s)
    , _cache(cache)
    , _permit(std::move(permit))
    , _columns(std::move(columns))
    , _direction(direction)
    , _hint(hint)
    , _policy(policy)
    , _alignment(std::max<uint64_t>(_file.disk_read_dma_alignment(), 1))
    , _range_end(s.rows())
{
    std::ranges::sort(_columns);
    if (std::ranges::adjacent_find(_columns) != _columns.end()) {
        on_internal_error(fetcher_logger, "Parquet fetcher: a column is projected twice");
    }
    for (auto c : _columns) {
        if (c >= s.columns().size()) {
            on_internal_error(fetcher_logger, seastar::format("Parquet fetcher: column {} is not in the file", c));
        }
    }
    if (!std::has_single_bit(_alignment)) {
        on_internal_error(fetcher_logger, seastar::format("Parquet fetcher: the read alignment {} is not a power of two", _alignment));
    }
    _policy.max_request_size = std::max(align_down(_policy.max_request_size, _alignment), _alignment);
    _next_page.resize(_columns.size(), -1);
    _taken.resize(_columns.size(), no_page);
}

fetcher::~fetcher() {
    if (_gate.get_count()) {
        on_internal_error_noexcept(fetcher_logger, "Parquet fetcher destroyed without close()");
    }
}

std::optional<size_t> fetcher::column_position(leaf_index c) const noexcept {
    auto it = std::ranges::lower_bound(_columns, c);
    if (it == _columns.end() || *it != c) {
        return std::nullopt;
    }
    return it - _columns.begin();
}

bool fetcher::has_data(uint32_t g, leaf_index c) const noexcept {
    return _summary.in_place(g, c) && !_summary.all_null(g, c);
}

uint64_t fetcher::budget(uint32_t g) const noexcept {
    uint64_t n = 0;
    for (auto c : _columns) {
        n += has_data(g, c);
    }
    return std::min(std::max(n * _policy.budget_per_column, _policy.min_budget), _summary.region_size(g));
}

void fetcher::set_range(uint64_t begin, uint64_t end) noexcept {
    _range_begin = begin;
    _range_end = end;
    _pending.clear();
    _pending_bytes = 0;
    _wanted.clear();
    _wanted_bytes = 0;
    _window_valid = false;
    _frontier_index = nullptr;
    _window_steps = 1;
    _stalled_steps = 0;
    drop_unused_indexes();
}

void fetcher::want(page_key key, uint64_t offset, uint32_t size, bool now) {
    if (!_pending.contains(key)) {
        _wanted.push_back(wanted_page{key, offset, size, now});
        _wanted_bytes += size;
    }
}

void fetcher::want_page(uint32_t g, leaf_index c, const row_group_index& idx, uint32_t j, bool now) {
    // The page that the column's cursor holds is not fetched again.
    if (auto pos = column_position(c); pos && _taken[*pos] == std::pair(g, j)) {
        return;
    }
    const auto p = idx.page_at(c, j);
    want(page_key{c, g, j + 1}, p.offset, p.size, now);
}

void fetcher::want_dictionary(uint32_t g, leaf_index c, const row_group_index& idx, bool now) {
    if (_summary.has_dictionary(g, c) && !_cache.contains(metadata_cache::dictionary_key(g, c))) {
        want(page_key{c, g, 0}, idx.chunk_start(c), idx.dictionary_size(c), now);
    }
}

void fetcher::issue_wanted() {
    if (_wanted.empty()) {
        return;
    }
    // A page wanted now and ahead is kept as wanted now.
    std::ranges::sort(_wanted, {}, [] (const wanted_page& w) { return std::tuple(w.offset, w.key, !w.now); });
    auto [first, last] = std::ranges::unique(_wanted, {}, &wanted_page::key);
    _wanted.erase(first, last);
    auto clear = defer([this] () noexcept {
        _wanted.clear();
        _wanted_bytes = 0;
    });
    // The requests and the pages are made first, and the reads are started
    // once nothing can fail. Then the pages are recorded. A page that fails
    // to be recorded is read again if a cursor asks for it.
    utils::chunked_vector<io_request_ptr> requests;
    utils::chunked_vector<std::pair<page_key, pending_page>> pages;
    pages.reserve(_wanted.size());
    for (size_t i = 0; i < _wanted.size();) {
        // The pages [i, k) are read as one range, cut into requests. Ranges
        // whose aligned extents overlap or touch are merged: the bytes
        // between them are in blocks that separate requests would read too.
        const uint64_t start = _wanted[i].offset;
        uint64_t end = start + _wanted[i].size;
        size_t k = i + 1;
        while (k < _wanted.size() && align_down(_wanted[k].offset, _alignment) <= align_up(end, _alignment)) {
            end = std::max(end, _wanted[k].offset + _wanted[k].size);
            ++k;
        }
        const size_t q0 = requests.size();
        make_requests(start, end, false, requests);
        for (size_t q = q0; i < k; ++i) {
            const auto& w = _wanted[i];
            pending_page p{.offset = w.offset, .size = w.size};
            while (requests[q]->offset + requests[q]->size <= w.offset) {
                ++q;
            }
            for (size_t r = q; r < requests.size() && requests[r]->offset < w.offset + w.size; ++r) {
                p.requests.push_back(requests[r]);
                requests[r]->now |= w.now;
            }
            pages.emplace_back(w.key, std::move(p));
        }
    }
    // The requests that a cursor waits for ask for memory first.
    for (const bool now : {true, false}) {
        for (const auto& r : requests) {
            if (r->now == now) {
                start_read(r);
            }
        }
    }
    for (auto& [key, p] : pages) {
        const uint32_t size = p.size;
        if (_pending.emplace(key, std::move(p)).second) {
            _pending_bytes += size;
        }
    }
    _stats.max_pending_bytes = std::max(_stats.max_pending_bytes, _pending_bytes);
}

// Returns a request that holds the aligned block at `block`, and that can
// supply the bytes up to `end` that it holds, or null.
fetcher::io_request_ptr fetcher::reusable_request(uint64_t block, uint64_t end) noexcept {
    auto it = _live_requests.upper_bound(block);
    if (it == _live_requests.begin()) {
        return nullptr;
    }
    io_request& r = *--it;
    if (r.offset + r.size <= block) {
        return nullptr;
    }
    const uint64_t needed_end = std::min(end, r.offset + r.size);
    if (r.done.available()) {
        // A failed or short read is not reused, so that a new read reports its own error.
        if (r.error || r.data.size() < needed_end - r.offset) {
            return nullptr;
        }
    } else {
        // read() checks the size of the read against needed_end when it completes.
        r.needed_end = std::max(r.needed_end, needed_end);
    }
    // A pending page, a taken page, or the read owns the request.
    return io_request_ptr(r.shared_from_this());
}

void fetcher::make_requests(uint64_t start, uint64_t end, bool index, utils::chunked_vector<io_request_ptr>& requests) {
    uint64_t o = align_down(start, _alignment);
    uint64_t aligned_end = align_up(end, _alignment);
    if (auto head = reusable_request(o, end)) {
        o = head->offset + head->size;
        requests.push_back(std::move(head));
    }
    io_request_ptr tail;
    if (o < end) {
        tail = reusable_request(align_down(end - 1, _alignment), end);
        if (tail && tail->offset >= o) {
            aligned_end = tail->offset;
        } else {
            tail = nullptr;
        }
    }
    while (o < aligned_end) {
        const uint64_t next = std::min(aligned_end, o + _policy.max_request_size);
        auto r = make_lw_shared<io_request>();
        r->offset = o;
        r->size = next - o;
        r->needed_end = std::min(end, next);
        r->index = index;
        _live_requests.insert(*r);
        requests.push_back(std::move(r));
        o = next;
    }
    if (tail) {
        requests.push_back(std::move(tail));
    }
}

void fetcher::start_read(io_request_ptr r) noexcept {
    // A reused request is read once.
    if (r->issued) {
        return;
    }
    r->issued = true;
    try {
        // read() fails only to start, and sets r->done otherwise.
        (void)read(r, _gate.hold());
    } catch (...) {
        r->error = std::current_exception();
        r->done.set_value();
    }
}

future<> fetcher::read(io_request_ptr r, gate::holder) {
    try {
        std::optional<reader_permit::resource_units> units;
        if (_permit) {
            units = co_await _permit->request_memory(r->size);
        }
        // While the request waited for memory, the fetcher may have closed,
        // or dropped every page of the request. Then nothing reads it.
        check_open();
        if (r.use_count() == 1) {
            co_return;
        }
        ++_stats.reads;
        _stats.index_reads += r->index;
        _stats.bytes_read += r->size;
        _stats.largest_request = std::max(_stats.largest_request, r->size);
        auto data = co_await _file.dma_read_bulk<char>(r->offset, r->size, &_intent);
        if (data.size() < r->needed_end - r->offset) {
            throw malformed_sstable_exception(seastar::format("Parquet file is malformed: a read of {} bytes at {} returned {}",
                    r->size, r->offset, data.size()));
        }
        if (units) {
            data = make_tracked_temporary_buffer(std::move(data), std::move(*units));
        }
        r->data = std::move(data);
    } catch (...) {
        r->error = std::current_exception();
    }
    r->done.set_value();
}

future<fragmented_temporary_buffer> fetcher::take(page_key key, bool retry) {
    auto it = _pending.find(key);
    if (it == _pending.end()) {
        on_internal_error(fetcher_logger, "Parquet fetcher: taking a page that was not fetched");
    }
    pending_page p = std::move(it->second);
    _pending.erase(it);
    _pending_bytes -= p.size;
    std::vector<temporary_buffer<char>> pieces;
    pieces.reserve(p.requests.size());
    for (const auto& r : p.requests) {
        co_await r->done.get_shared_future();
        if (r->error) {
            if (retry && failed_for_memory(r->error)) {
                // The request may be read-ahead whose memory request failed
                // while the read was inactive. The caller waits now, so the
                // page is fetched again, with new requests.
                pieces.clear();
                want(key, p.offset, p.size, true);
                issue_wanted();
                co_return co_await take(key, false);
            }
            co_return coroutine::exception(r->error);
        }
        const uint64_t a = std::max(p.offset, r->offset);
        const uint64_t b = std::min(p.offset + p.size, r->offset + r->size);
        // The contiguous piece keeps the request, which owns the buffer, so
        // that a later range can reuse the request while the buffer exists.
        pieces.push_back(temporary_buffer<char>(r->data.get_write() + (a - r->offset), b - a, make_object_deleter(r)));
    }
    co_return fragmented_temporary_buffer(std::move(pieces), p.size);
}

void fetcher::drop_behind(page_key key) {
    auto drop = [this] (auto from, auto to) {
        for (auto it = from; it != to; ++it) {
            _pending_bytes -= it->second.size;
        }
        _pending.erase(from, to);
    };
    const auto c = key.column;
    if (_direction == read_direction::forward) {
        // The dictionary of the same row group is kept: the cursor asks for it after the page.
        drop(_pending.lower_bound(page_key{c, 0, 0}), _pending.lower_bound(page_key{c, key.row_group, 0}));
        drop(_pending.lower_bound(page_key{c, key.row_group, 1}), _pending.lower_bound(key));
    } else {
        drop(_pending.upper_bound(key), _pending.lower_bound(page_key{c + 1, 0, 0}));
    }
}

void fetcher::enter_row_group(uint32_t g, row_group_index_ptr idx, uint64_t row_in_group, bool now) {
    _frontier_row_group = g;
    _frontier_budget = budget(g);
    for (size_t k = 0; k < _columns.size(); ++k) {
        const auto c = _columns[k];
        if (!has_data(g, c)) {
            _next_page[k] = -1;
            continue;
        }
        if (_direction == read_direction::forward) {
            _next_page[k] = row_in_group ? idx->page_of(c, row_in_group) : 0;
        } else {
            _next_page[k] = row_in_group < idx->rows() ? idx->page_of(c, row_in_group - 1) : idx->pages(c) - 1;
        }
        want_dictionary(g, c, *idx, now);
    }
    _frontier_index = std::move(idx);
    drop_unused_indexes();
}

void fetcher::reset_window(uint32_t g, row_group_index_ptr idx, uint64_t row) {
    _window_valid = true;
    _stalled_steps = 0;
    const uint64_t first = _summary.row_group_first_row(g);
    _frontier = _direction == read_direction::forward ? row : row + 1;
    enter_row_group(g, std::move(idx), _frontier - first, true);
}

fetcher::step_result fetcher::step(bool now) {
    const bool forward = _direction == read_direction::forward;
    if (forward ? _frontier >= _summary.rows() : _frontier == 0) {
        return step_result::end;
    }
    if (!_frontier_index) {
        const uint32_t g = _frontier_row_group;
        if (std::ranges::none_of(_columns, [&] (leaf_index c) { return has_data(g, c); })) {
            // Nothing to read in this row group, and no need for its OffsetIndexes.
            if (forward) {
                _frontier = _summary.row_group_end_row(g);
                ++_frontier_row_group;
            } else {
                _frontier = _summary.row_group_first_row(g);
                --_frontier_row_group;
            }
            return step_result::skipped;
        }
        // A load from the cache may complete at once.
        auto load = start_index_load(g);
        if (!load->index) {
            return step_result::waiting_for_index;
        }
        enter_row_group(g, load->index, forward ? 0 : _summary.row_group_rows(g), now);
    }
    const uint32_t g = _frontier_row_group;
    const auto& idx = *_frontier_index;
    const uint64_t first = _summary.row_group_first_row(g);
    const uint64_t r = _frontier - first;
    if (forward) {
        uint64_t boundary = idx.rows();
        for (size_t k = 0; k < _columns.size(); ++k) {
            int64_t& j = _next_page[k];
            if (j < 0) {
                continue;
            }
            const auto c = _columns[k];
            if (idx.page_at(c, j).first_row <= r) {
                want_page(g, c, idx, j, now);
                j = j + 1 < idx.pages(c) ? j + 1 : -1;
            }
            if (j >= 0) {
                boundary = std::min(boundary, idx.page_at(c, j).first_row);
            }
        }
        _frontier = first + boundary;
        if (boundary == idx.rows()) {
            ++_frontier_row_group;
            _frontier_index = nullptr;
        }
    } else {
        uint64_t boundary = 0;
        for (size_t k = 0; k < _columns.size(); ++k) {
            int64_t& j = _next_page[k];
            if (j < 0) {
                continue;
            }
            const auto c = _columns[k];
            if (idx.page_end_row(c, j) >= r) {
                want_page(g, c, idx, j, now);
                --j;
            }
            if (j >= 0) {
                boundary = std::max(boundary, idx.page_end_row(c, j));
            }
        }
        _frontier = first + boundary;
        if (boundary == 0) {
            --_frontier_row_group;
            _frontier_index = nullptr;
        }
    }
    return step_result::stepped;
}

void fetcher::extend(bool on_miss) {
    if (!_window_valid) {
        return;
    }
    uint64_t steps = std::numeric_limits<uint64_t>::max();
    if (_hint == read_hint::single_partition) {
        steps = on_miss ? _window_steps : 0;
        if (on_miss) {
            _window_steps = std::min(_window_steps * 2, max_window_steps);
        }
    }
    // A scan refills the window only once half of it was taken, so that
    // each refill makes requests of a useful size.
    if (!on_miss && _pending_bytes >= _frontier_budget / 2) {
        return;
    }
    advance(steps, on_miss);
}

void fetcher::advance(uint64_t steps, bool on_miss) {
    const bool forward = _direction == read_direction::forward;
    _stalled_steps = 0;
    _advancing = true;
    auto done = defer([this] () noexcept { _advancing = false; });
    uint64_t skipped = 0;
    for (bool first = on_miss; steps; first = false, --steps) {
        // A miss always takes one step, which covers the page asked for,
        // and the pages of the other cursors at the same row.
        if (!first) {
            if (forward ? _frontier >= _range_end : _frontier <= _range_begin) {
                break;
            }
            if (_pending_bytes + _wanted_bytes >= _frontier_budget) {
                break;
            }
        }
        const auto r = step(first);
        if (r == step_result::waiting_for_index) {
            _stalled_steps = steps;
            break;
        }
        if (r == step_result::end || (r == step_result::skipped && ++skipped == max_skipped_row_groups)) {
            break;
        }
    }
    issue_wanted();
}

void fetcher::resume_window() noexcept {
    if (_closing || _advancing || !_stalled_steps || !_window_valid || _frontier_index) {
        return;
    }
    // A load that is in progress resumes the window when it completes, and
    // a load that failed stops it. A load that failed for memory, or was
    // dropped, is started again.
    if (auto it = _index_loads.find(_frontier_row_group); it != _index_loads.end()) {
        const auto& load = *it->second;
        if (!load.done.available() || (load.error && !failed_for_memory(load.error))) {
            return;
        }
    }
    try {
        advance(_stalled_steps, false);
    } catch (...) {
        // Read-ahead is optional. The window restarts at the next miss.
        fetcher_logger.debug("Parquet fetcher: read-ahead failed: {}", std::current_exception());
        _wanted.clear();
        _wanted_bytes = 0;
        _window_valid = false;
        _stalled_steps = 0;
    }
}

future<fragmented_temporary_buffer> fetcher::get_page(uint32_t g, leaf_index c, row_group_index_ptr idx, uint32_t j, uint64_t row) {
    consumer_call call(*this);
    resume_window();
    ++_stats.pages_taken;
    const page_key key{c, g, j + 1};
    drop_behind(key);
    const bool miss = !_pending.contains(key);
    if (miss) {
        ++_stats.page_misses;
        reset_window(g, idx, row);
    }
    extend(miss);
    if (!_pending.contains(key)) {
        // The column is not one that the window covers.
        const auto p = idx->page_at(c, j);
        want(key, p.offset, p.size, true);
        issue_wanted();
    }
    if (auto pos = column_position(c)) {
        _taken[*pos] = {g, j};
    }
    co_return co_await take(key);
}

future<fragmented_temporary_buffer> fetcher::get_dictionary_page(uint32_t g, leaf_index c, const row_group_index& idx) {
    consumer_call call(*this);
    resume_window();
    const page_key key{c, g, 0};
    if (!_pending.contains(key)) {
        want(key, idx.chunk_start(c), idx.dictionary_size(c), true);
        issue_wanted();
    }
    co_return co_await take(key);
}

lw_shared_ptr<fetcher::index_load> fetcher::start_index_load(uint32_t g) {
    auto it = _index_loads.find(g);
    if (it != _index_loads.end()) {
        const auto& old = *it->second;
        // A load that was dropped, or that failed for memory, which may be
        // because the read was inactive, is started again.
        if (!old.done.available() || old.index || (old.error && !failed_for_memory(old.error))) {
            return it->second;
        }
    }
    auto load = make_lw_shared<index_load>();
    if (it != _index_loads.end()) {
        it->second = load;
    } else {
        _index_loads.emplace(g, load);
    }
    try {
        // load_index() fails only to start, and sets load->done otherwise.
        (void)load_index(g, load, _gate.hold());
    } catch (...) {
        load->error = std::current_exception();
        load->done.set_value();
    }
    return load;
}

future<> fetcher::load_index(uint32_t g, lw_shared_ptr<index_load> load, gate::holder) {
    try {
        load->index = co_await read_index(g);
    } catch (...) {
        load->error = std::current_exception();
    }
    // A failed load stays, so that get_index() reports its error.
    load->done.set_value();
    // Without a call in progress, the read may be inactive, so the window
    // goes on at the next call.
    if (load->index && may_request_memory()) {
        resume_window();
    }
}

// Every memory request of a load starts before its first suspension, so
// within the call that starts the load, apart from a read of a block whose
// cache entry was evicted while the load waited. Returns null if the load
// was dropped.
//
// A hit asks for the memory of its copy, which holds the projected columns
// only. A miss asks for a bound on the memory of the decoder, reads the
// block, decodes it into the compact form, adds that to the cache, and
// copies the projected columns out of it.
future<row_group_index_ptr> fetcher::read_index(uint32_t g) {
    const auto& rg = _summary.row_group(g);
    {
        auto cached_index = _cache.get_index(_summary, g, _columns, _permit);
        std::optional<metadata_cache::cached_index> cached;
        bool waited = false;
        if (cached_index.available()) {
            cached = cached_index.get();
        } else {
            waited = true;
            cached = co_await std::move(cached_index);
            check_open();
        }
        if (cached) {
            co_return make_lw_shared<const charged_row_group_index>(charged_row_group_index{std::move(cached->index), std::move(cached->memory)});
        }
        if (waited && !may_request_memory()) {
            // The entry was evicted while the copy waited, and the read
            // may be inactive now. The next call that needs the block
            // loads it again.
            co_return nullptr;
        }
    }
    const uint64_t start = rg.offset_index_offset;
    const uint64_t end = start + rg.offset_index_length;
    // The memory of the decoder, which the index takes over.
    auto bound_units = _permit
            ? futurize_invoke([&] { return _permit->request_memory(row_group_index_decode_memory(_summary, g)); })
                    .then([] (reader_permit::resource_units u) { return std::optional(std::move(u)); })
            : make_ready_future<std::optional<reader_permit::resource_units>>();
    // The bytes of the block, which read() charges.
    fragmented_temporary_buffer block;
    std::exception_ptr ex;
    try {
        utils::chunked_vector<io_request_ptr> requests;
        make_requests(start, end, true, requests);
        for (const auto& r : requests) {
            start_read(r);
        }
        std::vector<temporary_buffer<char>> pieces;
        for (const auto& r : requests) {
            co_await r->done.get_shared_future();
            if (r->error) {
                std::rethrow_exception(r->error);
            }
            const uint64_t a = std::max(start, r->offset);
            const uint64_t b = std::min(end, r->offset + r->size);
            pieces.push_back(r->data.share(a - r->offset, b - a));
        }
        requests.clear();
        block = fragmented_temporary_buffer(std::move(pieces), rg.offset_index_length);
    } catch (...) {
        ex = std::current_exception();
    }
    auto bound = co_await std::move(bound_units);
    if (ex) {
        std::rethrow_exception(ex);
    }
    check_open();
    auto compact = co_await decode_offset_index_block(_summary, g, std::move(block));
    check_open();
    _cache.insert(metadata_cache::index_key(g), byte_array_view(compact));
    auto idx = co_await make_row_group_index(_summary, g, _columns, compact);
    compact = {};
    check_open();
    // The bound covers the decoder's peak, which is above the size of the
    // index, and is loose. So the bound is released, and the index is
    // charged at its size. This lowers the charge, so it need not wait,
    // and it asks for no memory, which the read may not do now if it is
    // inactive.
    std::optional<reader_permit::resource_units> memory;
    if (bound) {
        bound.reset();
        memory = _permit->consume_memory(idx.memory_usage());
    }
    co_return make_lw_shared<const charged_row_group_index>(charged_row_group_index{std::move(idx), std::move(memory)});
}

void fetcher::check_open() const {
    if (_closing) {
        throw std::runtime_error("Parquet fetcher: closed");
    }
}

void fetcher::drop_unused_indexes() noexcept {
    std::erase_if(_index_loads, [] (const auto& kv) {
        const auto& load = kv.second;
        return load.use_count() == 1 && load->index && load->index.use_count() == 1;
    });
}

future<row_group_index_ptr> fetcher::get_index(uint32_t g) {
    consumer_call call(*this);
    resume_window();
    // A load that started before this call may have been dropped, or may
    // have failed for memory while the read was inactive. Then the block
    // is loaded again, once. That load starts within this call, so it is
    // not dropped, and its error is reported.
    for (bool first = true;; first = false) {
        auto load = start_index_load(g);
        co_await load->done.get_shared_future();
        if (load->index) {
            co_return load->index;
        }
        if (first && (!load->error || failed_for_memory(load->error))) {
            continue;
        }
        if (!load->error) {
            on_internal_error(fetcher_logger, seastar::format("Parquet fetcher: the load of the OffsetIndex block of row group {} was dropped", g));
        }
        if (auto it = _index_loads.find(g); it != _index_loads.end() && it->second == load) {
            _index_loads.erase(it);
        }
        co_return coroutine::exception(load->error);
    }
}

future<> fetcher::close() noexcept {
    _closing = true;
    _pending.clear();
    _pending_bytes = 0;
    _wanted.clear();
    _wanted_bytes = 0;
    _window_valid = false;
    _stalled_steps = 0;
    _intent.cancel();
    co_await _gate.close();
    _frontier_index = nullptr;
    _index_loads.clear();
}

} // namespace sstables::parquet

/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/reader.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/when_all.hh>

#include "utils/log.hh"

namespace sstables::parquet {

static logging::logger reader_logger("parquet_reader");

static std::vector<leaf_index> present_columns(const std::vector<std::optional<leaf_index>>& projection) {
    std::vector<leaf_index> r;
    for (const auto& c : projection) {
        if (c) {
            r.push_back(*c);
        }
    }
    return r;
}

reader::reader(file f, const file_summary& summary, metadata_cache& cache, const std::vector<std::optional<leaf_index>>& projection,
        reader_options options, std::optional<reader_permit::resource_units> fixed_memory)
    : _file(f)
    , _summary(summary)
    , _cache(cache)
    , _options(options)
    , _fetcher(std::move(f), summary, cache, present_columns(projection), options.direction, options.hint, options.policy, options.permit)
    , _fixed_memory(std::move(fixed_memory))
{
    if (_options.permit && !_fixed_memory) {
        _fixed_memory = _options.permit->consume_memory(reader::fixed_memory(projection.size()));
    }
    _cursors.reserve(projection.size());
    for (const auto& c : projection) {
        _cursors.push_back(std::make_unique<column_cursor>(*this, c));
    }
    fast_forward_to(_options.begin, _options.end);
}

size_t reader::fixed_memory(size_t columns) noexcept {
    return sizeof(reader) + columns * fixed_memory_per_column();
}

size_t reader::fixed_memory_per_column() noexcept {
    // The cursor, its pointer in _cursors, its page compressor (an empty
    // object whose allocation takes at least 16 bytes), and the fetcher's
    // state per column.
    return sizeof(column_cursor) + sizeof(std::unique_ptr<column_cursor>) + 16 + fetcher::memory_per_column();
}

reader::~reader() = default;

void reader::fast_forward_to(uint64_t begin, uint64_t end) {
    _options.end = std::min(end, _summary.rows());
    _options.begin = std::min(begin, _options.end);
    _fetcher.set_range(_options.begin, _options.end);
}

future<> reader::seek_all(uint64_t row) {
    // The first cursor that misses fetches the window for all columns, so
    // the others find their pages fetched or in flight.
    for (auto& c : _cursors) {
        co_await c->seek(row);
    }
}

void reader::release_pages() noexcept {
    for (auto& c : _cursors) {
        c->release();
    }
    _fetcher.set_range(_options.begin, _options.end);
}

future<> reader::close() noexcept {
    return _fetcher.close();
}

column_cursor::column_cursor(reader& r, std::optional<leaf_index> column)
    : _reader(r)
    , _column(column)
{
    if (_column) {
        _compressor = make_page_compressor(_reader._summary.column(*_column).codec);
    }
}

column_cursor::~column_cursor() = default;

void column_cursor::check_row(uint64_t row) const {
    if (row >= _reader._summary.rows()) {
        on_internal_error(reader_logger, seastar::format("Parquet column cursor: row {} is beyond the {} rows of the file", row, _reader._summary.rows()));
    }
}

// The most row groups that try_null_region() looks at on each side of the
// row group of the row. A run of entirely null row groups may be longer.
// The region then ends early, and the cursor finds the next part of the
// run when it gets there. This bounds the work of a call, which has no
// preemption point.
static constexpr uint32_t max_null_region_row_groups = 256;

// If `row` is in a run of row groups where the column is entirely null,
// moves to the part of the run around `row` and returns true.
bool column_cursor::try_null_region(uint64_t row) {
    const auto& s = _reader._summary;
    if (!_column) {
        _region_begin = 0;
        _region_end = s.rows();
    } else {
        const auto c = *_column;
        const uint32_t g = s.row_group_of(row);
        if (!s.all_null(g, c)) {
            return false;
        }
        uint32_t first = g;
        while (first > 0 && g - first < max_null_region_row_groups && s.all_null(first - 1, c)) {
            --first;
        }
        uint32_t last = g;
        while (last + 1 < s.row_groups() && last - g < max_null_region_row_groups && s.all_null(last + 1, c)) {
            ++last;
        }
        _region_begin = s.row_group_first_row(first);
        _region_end = s.row_group_end_row(last);
    }
    _state = state::null_region;
    drop_page();
    _row = row;
    return true;
}

void column_cursor::drop_page() noexcept {
    _page.reset();
    _page_memory.reset();
}

void column_cursor::drop_row_group() noexcept {
    _index = nullptr;
    _dictionary = nullptr;
    _dictionary_memory.reset();
}

void column_cursor::release() noexcept {
    drop_page();
    drop_row_group();
    _state = state::unpositioned;
}

bool column_cursor::try_seek_far(uint64_t row) {
    check_row(row);
    if (_state != state::unpositioned && row >= _region_begin && row < _region_end) {
        if (_state == state::page) {
            if (row == _row + 1) {
                _page->next_row(_pos);
            } else if (row + 1 == _row) {
                _page->previous_row(_pos);
            } else if (row != _row) {
                _pos = _page->row_start(row - _region_begin);
            }
        }
        _row = row;
        return true;
    }
    return try_null_region(row);
}

bool column_cursor::try_next() {
    if (_state == state::page && _row + 1 < _region_end) {
        _page->next_row(_pos);
        ++_row;
        return true;
    }
    if (_state == state::null_region && _row + 1 < _region_end) {
        ++_row;
        return true;
    }
    return try_seek(_row + 1);
}

bool column_cursor::try_previous() {
    if (_row == 0) {
        on_internal_error(reader_logger, "Parquet column cursor: moving before the first row");
    }
    if (_state == state::page && _row > _region_begin) {
        _page->previous_row(_pos);
        --_row;
        return true;
    }
    if (_state == state::null_region && _row > _region_begin) {
        --_row;
        return true;
    }
    return try_seek(_row - 1);
}

future<> column_cursor::seek(uint64_t row) {
    if (try_seek(row)) {
        return make_ready_future<>();
    }
    return load(row);
}

future<> column_cursor::next() {
    if (try_next()) {
        return make_ready_future<>();
    }
    return load(_row + 1);
}

future<> column_cursor::previous() {
    if (try_previous()) {
        return make_ready_future<>();
    }
    return load(_row - 1);
}

// Marks the reader's permit as waiting for I/O while it lives, as the mx
// reader does around its reads. The reader concurrency semaphore admits a
// new read only while few admitted reads need the CPU, so a read that waits
// for I/O must not count as one.
static std::optional<reader_permit::awaits_guard> awaiting_io(const std::optional<reader_permit>& permit) {
    std::optional<reader_permit::awaits_guard> guard;
    if (permit) {
        guard.emplace(*permit);
    }
    return guard;
}

// The dictionary of the cursor's column in row group `g`: a copy of the
// cached compact form, or the dictionary page as fetched.
future<column_cursor::dictionary_page_bytes> column_cursor::dictionary_page(uint32_t g) {
    const auto c = *_column;
    if (auto cached = co_await _reader._cache.get(metadata_cache::dictionary_key(g, c), _reader._options.permit)) {
        co_return dictionary_page_bytes{std::move(*cached), true};
    }
    co_return dictionary_page_bytes{{co_await _reader._fetcher.get_dictionary_page(g, c, *_index)}, false};
}

future<> column_cursor::load_dictionary(uint32_t g, dictionary_page_bytes page) {
    const auto& s = _reader._summary;
    const auto c = *_column;
    const auto& col = s.column(c);
    if (page.cached) {
        // The copy is the dictionary, and its charge covers it.
        _dictionary = make_lw_shared<const any_dictionary>(make_dictionary(col, std::move(page.bytes.bytes)));
        _dictionary_memory = std::move(page.bytes.memory);
        co_return;
    }
    const auto checked = co_await check_dictionary_page(col, c, std::move(page.bytes.bytes), s.row_group(g).max_chunk_uncompressed_size);
    std::optional<reader_permit::resource_units> memory;
    auto& permit = _reader._options.permit;
    if (permit) {
        memory = co_await permit->request_memory(checked.decode_memory());
    }
    auto dictionary = co_await decode_dictionary_page(col, c, checked, *_compressor);
    _reader._cache.insert(metadata_cache::dictionary_key(g, c), byte_array_view(compact_dictionary(dictionary)));
    if (memory) {
        // The request covers the decompressed body too, which is gone. So
        // the dictionary is charged at its size. This lowers the charge, so
        // it need not wait.
        memory.reset();
        memory = permit->consume_memory(dictionary_memory_usage(dictionary));
    }
    _dictionary = make_lw_shared<const any_dictionary>(std::move(dictionary));
    _dictionary_memory = std::move(memory);
}

future<> column_cursor::load(uint64_t row) {
    _state = state::unpositioned;
    drop_page();
    const auto& s = _reader._summary;
    const auto c = *_column;
    const auto& col = s.column(c);
    const uint32_t g = s.row_group_of(row);
    if (g != _row_group || !_index) {
        drop_row_group();
        _row_group = g;
        auto awaits = awaiting_io(_reader._options.permit);
        _index = co_await _reader._fetcher.get_index(g);
    }
    const uint64_t first = s.row_group_first_row(g);
    const uint32_t j = _index->page_of(c, row - first);
    const uint64_t page_begin = first + _index->page_at(c, j).first_row;
    const uint64_t page_end = first + _index->page_end_row(c, j);
    // Ask for the data page first, so that the fetcher reads the dictionary page with it.
    auto page_bytes = _reader._fetcher.get_page(g, c, _index, j, row);
    const bool need_dictionary = s.has_dictionary(g, c) && !_dictionary;
    auto dictionary_bytes = need_dictionary ? dictionary_page(g) : make_ready_future<dictionary_page_bytes>();
    auto awaits = awaiting_io(_reader._options.permit);
    auto [page, dictionary] = co_await when_all_succeed(std::move(page_bytes), std::move(dictionary_bytes));
    awaits.reset();
    if (need_dictionary) {
        co_await load_dictionary(g, std::move(dictionary));
    }
    const auto& rg = s.row_group(g);
    auto checked = co_await decoded_page::check(col, c, std::move(page), page_end - page_begin, rg.max_chunk_uncompressed_size, rg.max_chunk_values);
    std::optional<reader_permit::resource_units> memory;
    if (auto& permit = _reader._options.permit) {
        memory = co_await permit->request_memory(checked.decode_memory());
    }
    _page = co_await decoded_page::decode(col, c, std::move(checked), _dictionary, *_compressor);
    if (memory) {
        // The fetched bytes are charged by the fetcher. The request is
        // a bound, so it covers the page, and the difference cannot be
        // known before the page is decoded. The charge is kept as it is,
        // because shrinking a charge means consuming the new amount before
        // releasing the old one. If the bound ever falls short, the rest
        // is asked for here, with a waiting call like the request.
        const size_t used = _page->memory_usage() - _page->fetched_bytes();
        const size_t charged = memory->resources().memory;
        if (used > charged) {
            memory->add(co_await _reader._options.permit->request_memory(used - charged));
        }
        _page_memory = std::move(memory);
    }
    _region_begin = page_begin;
    _region_end = page_end;
    _pos = _page->row_start(row - page_begin);
    _row = row;
    _state = state::page;
}

uint64_t column_cursor::null_rows_forward() const noexcept {
    switch (_state) {
    case state::null_region: return _region_end - _row;
    case state::page: return _page->null_rows_forward(_pos);
    default: return 0;
    }
}

uint64_t column_cursor::null_rows_backward() const noexcept {
    switch (_state) {
    case state::null_region: return _row - _region_begin + 1;
    case state::page: return _page->null_rows_backward(_pos);
    default: return 0;
    }
}

} // namespace sstables::parquet

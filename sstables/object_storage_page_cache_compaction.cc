/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option): compaction-resistant caching. See
// sstables/object_storage_page_cache_compaction.hh.

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include "sstables/object_storage_page_cache_compaction.hh"
#include "sstables/compress.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/storage.hh"
#include "utils/error_injection.hh"
#include "utils/log.hh"

namespace sstables {

extern logging::logger sstlog;

// A cache which fails - say, because its disk is full or broken - might fail
// every compaction, so its errors are logged at most once a minute.
static thread_local logging::logger::rate_limit compaction_error_rate_limit{std::chrono::minutes(1)};

// The size of the pages of the components which aren't split into compressed
// chunks (see object_storage_page_cache_geometry.hh).
static constexpr uint64_t page_size = 4096;

void page_cache_hotness::merge(const page_cache_hotness& other) noexcept {
    if (!other.hot) {
        return;
    }
    if (!hot) {
        *this = other;
        return;
    }
    if (other.last_hit && (!last_hit || *other.last_hit > *last_hit)) {
        last_hit = other.last_hit;
    }
}

void page_cache_hot_ranges::add(uint64_t start, uint64_t end, std::optional<db_clock::time_point> last_hit) {
    if (!_ranges.empty() && _ranges.back().end == start && _ranges.back().last_hit == last_hit) {
        _ranges.back().end = end;
        return;
    }
    _ranges.push_back(range{start, end, last_hit});
}

page_cache_hotness page_cache_hot_ranges::hotness(uint64_t from, uint64_t to) {
    while (!_ranges.empty() && _ranges.front().end <= from) {
        _ranges.pop_front();
    }
    page_cache_hotness ret;
    for (const auto& r : _ranges) {
        if (r.start >= to) {
            break;
        }
        ret.merge(page_cache_hotness{true, r.last_hit});
    }
    return ret;
}

page_cache_compaction_input::page_cache_compaction_input(object_storage_page_cache& cache, sstable_id sst, schema_ptr schema,
        const compression* compression, bool fixed_size_pages)
    : _cache(cache)
    , _sst(sst)
    , _schema(std::move(schema))
    , _compression(compression)
    , _fixed_size_pages(fixed_size_pages)
{}

template <typename Func>
future<> page_cache_compaction_input::for_each_unit(uint64_t start, uint64_t end, Func func) {
    if (_compression) {
        // A compressed chunk is the smallest unit of data whose position in
        // the stored component is known.
        uint64_t chunk_length = _compression->uncompressed_chunk_length();
        auto offsets = _compression->offsets.get_accessor();
        uint64_t chunks = _compression->offsets.size();
        for (uint64_t c = start / chunk_length; c < chunks && c * chunk_length < end; c++) {
            uint64_t stored_end = c + 1 < chunks ? offsets.at(c + 1) : _compression->compressed_file_length();
            co_await func(c * chunk_length, (c + 1) * chunk_length, offsets.at(c), stored_end);
        }
    } else {
        // An uncompressed component is stored as it is.
        for (uint64_t p = start - start % page_size; p < end; p += page_size) {
            co_await func(p, p + page_size, p, p + page_size);
        }
    }
}

future<> page_cache_compaction_input::fetch_hits_until(uint64_t stored_end) {
    // A row of pagecache_hits is small, so fetching many at a time costs
    // little memory, and few queries.
    constexpr size_t hits_per_fetch = 1000;
    while (_next_hits && *_next_hits < stored_end) {
        auto hits = co_await _cache.get_page_hits(_sst, component_type::Data, *_next_hits, hits_per_fetch);
        _next_hits = hits.size() < hits_per_fetch ? std::nullopt : std::optional<uint64_t>(hits.back().offset + 1);
        for (auto& h : hits) {
            if (h.size) {
                _hits.push_back(std::move(h));
            }
        }
    }
}

future<> page_cache_compaction_input::on_read(uint64_t start, uint64_t end) noexcept {
    if (_failed) {
        co_return;
    }
    // The units which the previous range ended in the middle of were
    // already looked up.
    start = std::max(start, _read_until);
    if (start >= end) {
        co_return;
    }
    try {
        co_await for_each_unit(start, end, [this] (uint64_t data_start, uint64_t data_end, uint64_t stored_start, uint64_t stored_end) -> future<> {
            if (!_hits_started) {
                // The first page which may overlap the unit starts at the
                // unit, if it is a compressed chunk cached as a page, or
                // otherwise at the 4 KiB page which contains its start.
                _hits_started = true;
                _next_hits = _compression && !_fixed_size_pages ? stored_start : stored_start - stored_start % page_size;
            }
            co_await fetch_hits_until(stored_end);
            while (!_hits.empty() && _hits.front().offset + _hits.front().size <= stored_start) {
                _hits.pop_front();
            }
            page_cache_hotness hotness;
            for (const auto& h : _hits) {
                if (h.offset >= stored_end) {
                    break;
                }
                hotness.merge(page_cache_hotness{true, h.last_hit});
            }
            if (hotness.hot) {
                _hot.add(data_start, data_end, hotness.last_hit);
            }
            _read_until = data_end;
        });
    } catch (...) {
        _failed = true;
        _cache.get_stats().compaction_errors++;
        sstlog.log(log_level::warn, compaction_error_rate_limit,
                "Failed to look up the object storage page cache of sstable {} for compaction, which won't cache its data: {}",
                _sst, std::current_exception());
    }
}

void page_cache_compaction_input::on_fragment(const dht::decorated_key& key, position_in_partition_view pos, uint64_t offset) {
    auto hotness = _hot.hotness(offset, offset + 1);
    if (!hotness.hot) {
        return;
    }
    _hot_fragments.push_back(hot_fragment{key, position_in_partition(pos), hotness});
    if (_hot_fragments.size() > max_hot_fragments) {
        _hot_fragments.pop_front();
    }
}

page_cache_hotness page_cache_compaction_input::hotness_at(const dht::decorated_key& key, position_in_partition_view pos) {
    position_in_partition::tri_compare cmp(*_schema);
    while (!_hot_fragments.empty()) {
        const auto& f = _hot_fragments.front();
        auto order = f.key.tri_compare(*_schema, key);
        if (order == 0) {
            order = cmp(f.position, pos);
        }
        if (order > 0) {
            return {};
        }
        if (order == 0) {
            return f.hotness;
        }
        _hot_fragments.pop_front();
    }
    return {};
}

page_cache_compaction::page_cache_compaction(object_storage_page_cache& cache, schema_ptr schema)
    : _cache(cache)
    , _schema(std::move(schema))
{}

future<> page_cache_compaction::start(std::vector<sstable_id> inputs) {
    try {
        for (auto sst : inputs) {
            // A row of pagecache_hits may be left without a page, if a hit
            // was recorded after the page was evicted.
            constexpr size_t hits_per_fetch = 100;
            std::optional<uint64_t> from = 0;
            while (from) {
                auto hits = co_await _cache.get_page_hits(sst, component_type::Data, *from, hits_per_fetch);
                if (std::ranges::any_of(hits, [] (const auto& h) { return h.size != 0; })) {
                    co_return;
                }
                from = hits.size() < hits_per_fetch ? std::nullopt : std::optional<uint64_t>(hits.back().offset + 1);
            }
        }
    } catch (...) {
        _cache.get_stats().compaction_errors++;
        sstlog.log(log_level::warn, compaction_error_rate_limit,
                "Failed to look up the object storage page cache of sstables for compaction, which won't cache their data: {}",
                std::current_exception());
    }
    _nothing_cached = true;
}

future<> page_cache_compaction::start(const std::vector<shared_sstable>& inputs) {
    std::vector<sstable_id> ids;
    for (const auto& sst : inputs) {
        if (sst->get_storage().caches_pages() && sst->sstable_identifier()) {
            ids.push_back(*sst->sstable_identifier());
        }
    }
    return start(std::move(ids));
}

page_cache_compaction_input& page_cache_compaction::add_input(sstable_id sst, const compression* compression, bool fixed_size_pages) {
    _inputs.push_back(std::make_unique<page_cache_compaction_input>(_cache, sst, _schema, compression, fixed_size_pages));
    return *_inputs.back();
}

shared_ptr<page_cache_compaction_output> page_cache_compaction::make_output(sstable_id sst) {
    return make_shared<page_cache_compaction_output>(shared_from_this(), sst);
}

// Whether a file I/O extension (encryption) transformed the sstable, so the
// cache holds its Data component in 4 KiB pages, even if it is compressed
// (see object_storage_base::maybe_make_page_cached_source()).
static bool transformed(const sstable& sst) {
    return std::ranges::any_of(sst.manager().file_io_extensions(), [&] (file_io_extension* ext) {
        return !ext->get_attributes(sst).empty();
    });
}

page_cache_compaction_input* page_cache_compaction::add_input(const sstable& sst) {
    if (_nothing_cached || !sst.get_storage().caches_pages() || !sst.sstable_identifier()) {
        return nullptr;
    }
    return &add_input(*sst.sstable_identifier(), sst.get_compression() ? &sst.get_compression() : nullptr, transformed(sst));
}

shared_ptr<page_cache_compaction_output> page_cache_compaction::make_output(const sstable& sst) {
    if (_nothing_cached || !sst.get_storage().caches_pages() || !sst.sstable_identifier()) {
        return nullptr;
    }
    return make_output(*sst.sstable_identifier());
}

lw_shared_ptr<page_cache_compaction> make_page_cache_compaction(const std::vector<shared_sstable>& inputs, schema_ptr schema) {
    if (inputs.empty() || !inputs.front()->get_storage().caches_pages()) {
        return nullptr;
    }
    // The cache is not plugged if its experimental feature was disabled
    // after the keyspace was created.
    auto cache = inputs.front()->manager().get_object_storage_page_cache();
    if (!cache) {
        return nullptr;
    }
    return make_lw_shared<page_cache_compaction>(*cache, std::move(schema));
}

page_cache_hotness page_cache_compaction::hotness_at(const dht::decorated_key& key, position_in_partition_view pos) {
    page_cache_hotness ret;
    for (auto& input : _inputs) {
        ret.merge(input->hotness_at(key, pos));
    }
    return ret;
}

// The outputs which exist on this shard, by their sstables' ids.
static thread_local std::unordered_map<sstable_id, page_cache_compaction_output*> page_cache_compaction_outputs;

page_cache_compaction_output::page_cache_compaction_output(lw_shared_ptr<page_cache_compaction> compaction, sstable_id sst)
    : _compaction(std::move(compaction))
    , _sst(sst)
{
    page_cache_compaction_outputs[_sst] = this;
    // The pages of the Data component, and the intervals of the Index, are
    // decided as they are written.
    _data.set_decided_until(0);
    _index.set_decided_until(0);
}

page_cache_compaction_output::~page_cache_compaction_output() {
    auto it = page_cache_compaction_outputs.find(_sst);
    if (it != page_cache_compaction_outputs.end() && it->second == this) {
        page_cache_compaction_outputs.erase(it);
    }
}

shared_ptr<page_cache_compaction_output> page_cache_compaction_output::find(sstable_id sst) noexcept {
    auto it = page_cache_compaction_outputs.find(sst);
    return it == page_cache_compaction_outputs.end() ? nullptr : it->second->shared_from_this();
}

void page_cache_compaction_output::set_data_compression(const compression* compression) noexcept {
    _data_page_size = compression ? compression->uncompressed_chunk_length() : page_size;
}

void page_cache_compaction_output::on_data_fragment(const dht::decorated_key& key, position_in_partition_view pos, uint64_t offset) {
    count_data_until(offset);
    _previous_fragment_start = std::exchange(_last_fragment_offset, offset);
    if (pos.is_partition_start()) {
        if (!_blocks.empty() && !current_block().end) {
            end_block(offset);
        }
        _undecided_partitions.push_back(partition{.first_block = _first_block + _blocks.size()});
        _partition_reported_blocks = 0;
        // The partition's first block has no row before it.
        start_block(offset, offset);
    }
    _fragment_hotness = _compaction->hotness_at(key, pos);
    current_block().hotness.merge(_fragment_hotness);
}

void page_cache_compaction_output::on_row_index_block(uint64_t offset) {
    // The partition's first block is read from the partition's start.
    if (_partition_reported_blocks++) {
        end_block(offset);
        start_block(offset, _previous_fragment_start);
    }
}

void page_cache_compaction_output::start_block(uint64_t start, uint64_t previous_row_start) {
    _blocks.push_back(block{start, previous_row_start});
}

void page_cache_compaction_output::end_block(uint64_t end) {
    auto& b = current_block();
    b.end = end;
    if (b.copied) {
        copy_block(b);
    }
}

void page_cache_compaction_output::copy_block(block& b) {
    b.copied = true;
    uint64_t end = b.end ? *b.end + next_row_bytes : _fragment_start;
    copy_data(b.previous_row_start, end, b.hotness);
    // The pages which aren't written yet.
    if (end > _copied_until) {
        _copied_until = end;
    }
    _copied_until_hotness.merge(b.hotness);
}

void page_cache_compaction_output::copy_data(uint64_t start, uint64_t end, const page_cache_hotness& hotness) {
    for (auto& page : _pending_pages) {
        if (page.start < end && start < page.end) {
            page.copy = true;
            page.hotness.merge(hotness);
        }
    }
}

void page_cache_compaction_output::count_data_until(uint64_t offset) {
    while (_fragment_start < offset) {
        uint64_t page_end = (_data_page + 1) * _data_page_size;
        uint64_t end = std::min(offset, page_end);
        if (_fragment_hotness.hot) {
            _data_page_hot_bytes += end - _fragment_start;
            _data_page_hotness.merge(_fragment_hotness);
            if (!_data_page_first_hot_block) {
                _data_page_first_hot_block = current_block_number();
            }
        }
        _fragment_start = end;
        if (end == page_end) {
            end_data_page(page_end);
        }
    }
}

void page_cache_compaction_output::end_data_page(uint64_t end) {
    uint64_t start = _data_page * _data_page_size;
    data_page page{start, end, _data_page_hotness};
    if (start < _copied_until) {
        page.copy = true;
        page.hotness.merge(_copied_until_hotness);
    }
    if (end >= _copied_until) {
        _copied_until_hotness = {};
    }
    _pending_pages.push_back(std::move(page));
    if (_data_page_hot_bytes && _data_page_hot_bytes * min_hot_fraction_denominator >= end - start) {
        _pending_pages.back().copy = true;
        // Copy the blocks with hot data in the page.
        for (auto n = *_data_page_first_hot_block; n <= current_block_number(); n++) {
            auto& b = _blocks[n - _first_block];
            if (b.hotness.hot && !b.copied) {
                copy_block(b);
            }
        }
    }
    _data_page++;
    _data_page_hot_bytes = 0;
    _data_page_hotness = {};
    _data_page_first_hot_block.reset();
    decide();
}

void page_cache_compaction_output::on_partition_end(uint64_t end,
        noncopyable_function<void(uint32_t mark, std::span<const uint32_t> block_marks)> add_to_index) {
    end_block(end);
    auto& p = _undecided_partitions.back();
    p.ended = true;
    p.index_interval = _first_index_interval + _index_intervals.size() - 1;
    if (!_index_intervals.empty()) {
        _index_intervals.back().undecided_partitions++;
    }
    p.add_to_index = std::move(add_to_index);
    decide();
}

void page_cache_compaction_output::decide() {
    // The first block which may still be copied: the current one - until
    // the next block starts - or the first one with hot data in the page
    // being written, which may yet be copied.
    uint64_t first_undecided_block = std::numeric_limits<uint64_t>::max();
    uint64_t undecided_from = std::numeric_limits<uint64_t>::max();
    if (!_data_ended && !_blocks.empty()) {
        first_undecided_block = std::min(current_block_number(), _data_page_first_hot_block.value_or(current_block_number()));
        undecided_from = _blocks[first_undecided_block - _first_block].previous_row_start;
    }
    while (!_pending_pages.empty() && _pending_pages.front().end <= undecided_from) {
        auto& page = _pending_pages.front();
        if (page.copy) {
            _data.add(page.start, page.end, page.hotness.last_hit);
        }
        _pending_pages.pop_front();
    }
    _data.set_decided_until(_pending_pages.empty() ? _data_page * _data_page_size : _pending_pages.front().start);

    while (!_undecided_partitions.empty() && _undecided_partitions.front().ended) {
        auto& p = _undecided_partitions.front();
        uint64_t blocks_end = _undecided_partitions.size() > 1 ? _undecided_partitions[1].first_block : _first_block + _blocks.size();
        if (blocks_end > first_undecided_block) {
            break;
        }
        // The partition's entries in the index are hot if it has copied
        // blocks with hot data, and so are its blocks' entries in its row
        // index.
        page_cache_hotness hotness;
        std::vector<uint32_t> block_marks;
        for (auto n = p.first_block; n < blocks_end; n++) {
            const auto& b = _blocks[n - _first_block];
            auto block_hotness = b.copied ? b.hotness : page_cache_hotness{};
            hotness.merge(block_hotness);
            block_marks.push_back(mark(block_hotness));
        }
        if (p.add_to_index) {
            p.add_to_index(mark(hotness), block_marks);
        }
        if (!_index_intervals.empty() && p.index_interval >= _first_index_interval) {
            auto& interval = _index_intervals[p.index_interval - _first_index_interval];
            interval.hotness.merge(hotness);
            interval.last_partition_hotness = hotness;
            interval.undecided_partitions--;
        }
        while (_first_block < blocks_end) {
            _blocks.pop_front();
            _first_block++;
        }
        _undecided_partitions.pop_front();
    }
    decide_index_intervals();
}

void page_cache_compaction_output::on_data_end(uint64_t offset) {
    count_data_until(offset);
    if (!_blocks.empty() && !current_block().end) {
        end_block(offset);
    }
    if (offset > _data_page * _data_page_size) {
        // The last page is shorter.
        end_data_page(offset);
    }
    _data_ended = true;
    decide();
    _data.set_all_decided();
}

void page_cache_compaction_output::abandon() noexcept {
    _undecided_partitions.clear();
}

void page_cache_compaction_output::on_index_interval(uint64_t offset) {
    if (!_index_intervals.empty()) {
        _index_intervals.back().end = offset;
    }
    _index_intervals.push_back(index_interval{.start = offset});
    decide_index_intervals();
}

void page_cache_compaction_output::on_index_end(uint64_t offset) {
    if (!_index_intervals.empty()) {
        _index_intervals.back().end = offset;
    }
    decide_index_intervals();
    if (_index_intervals.empty()) {
        _index.set_all_decided();
    }
}

void page_cache_compaction_output::decide_index_intervals() {
    while (!_index_intervals.empty() && _index_intervals.front().end && !_index_intervals.front().undecided_partitions) {
        auto& interval = _index_intervals.front();
        // An interval is also read with the previous one, if the previous
        // interval's last partition is read.
        auto hotness = interval.hotness;
        hotness.merge(_last_decided_partition_hotness);
        if (hotness.hot) {
            _index.add(interval.start, *interval.end, hotness.last_hit);
        }
        _index.set_decided_until(*interval.end);
        _last_decided_partition_hotness = interval.last_partition_hotness;
        _index_intervals.pop_front();
        _first_index_interval++;
    }
}

// A mark is 0 for data which isn't hot, 1 for hot data which was never
// hit, and otherwise 2 more than the seconds of the last hit - so the
// larger of two marks is that of their merged hotness. (Marks have 32 bits,
// which the trie writer keeps at no cost, so the last hit of a copied index
// page is rounded down to a second - a page's last hit is only known to
// within a pass of the hand anyway.)
uint32_t page_cache_compaction_output::mark(const page_cache_hotness& hotness) noexcept {
    if (!hotness.hot) {
        return 0;
    }
    if (!hotness.last_hit) {
        return 1;
    }
    auto seconds = hotness.last_hit->time_since_epoch() / std::chrono::seconds(1);
    return 2 + uint32_t(std::clamp<int64_t>(seconds, 0, std::numeric_limits<uint32_t>::max() - 2));
}

page_cache_hotness page_cache_compaction_output::hotness_of_mark(uint32_t mark) noexcept {
    if (mark == 0) {
        return {};
    }
    if (mark == 1) {
        return {true, std::nullopt};
    }
    return {true, db_clock::time_point(std::chrono::seconds(mark - 2))};
}

void page_cache_compaction_output::on_trie_node(component_type type, uint64_t pos, uint32_t mark) {
    auto* ranges = hot_ranges(type);
    if (!ranges) {
        return;
    }
    // A node is never split between pages, so the page which holds its
    // first byte holds all of it.
    ranges->add(pos, pos + 1, hotness_of_mark(mark).last_hit);
}

void page_cache_compaction_output::on_rows_entry_start(uint64_t offset) noexcept {
    // The header is marked only after it is written.
    _rows.set_decided_until(offset);
}

void page_cache_compaction_output::on_rows_entry_end(std::optional<uint64_t> header_start, uint64_t end, uint32_t mark) {
    auto hotness = hotness_of_mark(mark);
    if (header_start && hotness.hot) {
        _rows.add(*header_start, end, hotness.last_hit);
    }
    _rows.set_all_decided();
}

page_cache_hot_ranges* page_cache_compaction_output::hot_ranges(component_type type) noexcept {
    switch (type) {
    case component_type::Data:
        return &_data;
    case component_type::Index:
        return &_index;
    case component_type::Partitions:
        return &_partitions;
    case component_type::Rows:
        return &_rows;
    default:
        return nullptr;
    }
}

namespace {

class page_cache_compaction_source_impl : public data_source_impl {
    input_stream<char> _in;
    page_cache_compaction_input& _input;
    uint64_t _offset;
public:
    page_cache_compaction_source_impl(input_stream<char> in, page_cache_compaction_input& input, uint64_t offset)
        : _in(std::move(in)), _input(input), _offset(offset) {}

    virtual future<temporary_buffer<char>> get() override {
        auto buf = co_await _in.read();
        co_await _input.on_read(_offset, _offset + buf.size());
        _offset += buf.size();
        co_return buf;
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        co_await _in.skip(n);
        _offset += n;
        co_return temporary_buffer<char>();
    }

    virtual future<> close() override {
        return _in.close();
    }
};

class page_cache_compaction_sink_impl : public data_sink_impl {
    data_sink _sink;
    shared_ptr<page_cache_compaction_output> _output;
    page_cache_hot_ranges& _ranges;
    sstable_id _sst;
    component_type _type;
    // See make_page_cache_compaction_sink(). Whether the pages have a fixed
    // size is asked when the first page starts, and remembered.
    const compression* _compression;
    std::function<bool()> _fixed_size_pages_func;
    std::optional<bool> _fixed_size_pages;
    // The stored offset of the next byte written.
    uint64_t _pos = 0;
    // The page being written: where it starts and ends, and its bytes so
    // far. _page_end is 0 between pages.
    uint64_t _page_start = 0;
    uint64_t _page_end = 0;
    std::vector<temporary_buffer<char>> _page_bufs;
    // The next compressed chunk whose stored range may overlap a page.
    uint64_t _chunk = 0;
    // The pages which were written, whose data range isn't known to be hot
    // or not yet (see page_cache_hot_ranges::decided()).
    struct written_page {
        uint64_t start;
        uint64_t data_start;
        uint64_t data_end;
        std::vector<temporary_buffer<char>> bufs;
    };
    std::deque<written_page> _written;
    // Set after a failure to copy a page, after which no more are copied.
    bool _failed = false;
public:
    page_cache_compaction_sink_impl(data_sink sink, shared_ptr<page_cache_compaction_output> output, page_cache_hot_ranges& ranges,
            component_type type, const compression* compression, std::function<bool()> fixed_size_pages)
        : _sink(std::move(sink))
        , _output(std::move(output))
        , _ranges(ranges)
        , _sst(_output->sstable())
        , _type(type)
        , _compression(compression)
        , _fixed_size_pages_func(std::move(fixed_size_pages))
    {}

    virtual future<> put(std::span<temporary_buffer<char>> bufs) override {
        return data_sink_impl::fallback_put(bufs, [this] (temporary_buffer<char>&& buf) {
            return do_put(std::move(buf));
        });
    }

    virtual future<> flush() override {
        return _sink.flush();
    }

    virtual future<> close() override {
        // The last page ends at the end of the component, and all the data
        // was written, so all of it is decided.
        if (_page_end) {
            _page_end = _pos;
            end_page();
        }
        _ranges.set_all_decided();
        co_await copy_decided_pages();
        co_await _sink.close();
    }

    virtual size_t buffer_size() const noexcept override {
        return _sink.buffer_size();
    }

    virtual bool can_batch_flushes() const noexcept override {
        return _sink.can_batch_flushes();
    }

    virtual void on_batch_flush_error() noexcept override {
        _sink.on_batch_flush_error();
    }

private:
    future<> do_put(temporary_buffer<char> buf) {
        if (!_failed) {
            add(buf.share());
        }
        co_await _sink.put(std::move(buf));
        co_await copy_decided_pages();
    }

    // Splits the written bytes into pages.
    void add(temporary_buffer<char> buf) {
        while (!buf.empty()) {
            if (!_page_end) {
                start_page();
            }
            auto n = std::min<uint64_t>(buf.size(), _page_end - _pos);
            _page_bufs.push_back(buf.share(0, n));
            buf.trim_front(n);
            _pos += n;
            if (_pos == _page_end) {
                end_page();
            }
        }
    }

    // The stored range of the given compressed chunk. The compressed sink
    // records where each chunk starts before writing it, and the compressed
    // length after it, so a chunk's range is known once any of its bytes
    // are written.
    std::pair<uint64_t, uint64_t> chunk_range(uint64_t c) const {
        auto offsets = _compression->offsets.get_accessor();
        uint64_t chunks = _compression->offsets.size();
        return {offsets.at(c), c + 1 < chunks ? offsets.at(c + 1) : _compression->compressed_file_length()};
    }

    bool fixed_size_pages() {
        if (!_fixed_size_pages) {
            _fixed_size_pages = _fixed_size_pages_func();
        }
        return *_fixed_size_pages;
    }

    void start_page() {
        _page_start = _pos;
        if (_compression && !fixed_size_pages()) {
            _page_end = chunk_range(_chunk).second;
        } else {
            _page_end = _pos + page_size;
        }
    }

    void end_page() {
        uint64_t data_start;
        uint64_t data_end;
        if (!_compression) {
            data_start = _page_start;
            data_end = _pos;
        } else {
            // The chunks which overlap the page: one, if the pages are the
            // chunks; otherwise, the chunks which overlap its stored range.
            uint64_t chunk_length = _compression->uncompressed_chunk_length();
            uint64_t chunks = _compression->offsets.size();
            while (_chunk + 1 < chunks && chunk_range(_chunk).second <= _page_start) {
                _chunk++;
            }
            data_start = _chunk * chunk_length;
            uint64_t last = _chunk;
            while (last + 1 < chunks && chunk_range(last).second < _pos) {
                last++;
            }
            data_end = (last + 1) * chunk_length;
            if (!fixed_size_pages()) {
                _chunk++;
            }
        }
        _written.push_back(written_page{_page_start, data_start, data_end, std::exchange(_page_bufs, {})});
        _page_end = 0;
    }

    future<> copy_decided_pages() {
        while (!_written.empty() && _ranges.decided(_written.front().data_end)) {
            auto page = std::move(_written.front());
            _written.pop_front();
            auto hotness = _ranges.hotness(page.data_start, page.data_end);
            if (!hotness.hot || _failed) {
                continue;
            }
            uint64_t size = 0;
            for (const auto& b : page.bufs) {
                size += b.size();
            }
            temporary_buffer<char> data(size);
            auto out = data.get_write();
            for (const auto& b : page.bufs) {
                out = std::copy(b.begin(), b.end(), out);
            }
            auto& cache = _output->cache();
            try {
                // Lets tests stop a compaction while it copies pages.
                co_await utils::get_local_injector().inject("object_storage_page_cache_compaction_copy",
                        utils::wait_for_message(std::chrono::minutes(5)), false);
                co_await cache.put_copied_page(_sst, _type, page.start, std::move(data), hotness.last_hit);
                cache.get_stats().compaction_copied_pages++;
                cache.get_stats().compaction_copied_bytes += size;
            } catch (...) {
                _failed = true;
                _written.clear();
                cache.get_stats().compaction_errors++;
                sstlog.log(log_level::warn, compaction_error_rate_limit,
                        "Failed to copy pages of the {} component of sstable {}, written by compaction, to the object storage page cache: {}",
                        _type, _sst, std::current_exception());
            }
        }
    }
};

} // anonymous namespace

data_source make_page_cache_compaction_source(input_stream<char> in, page_cache_compaction_input& input, uint64_t offset) {
    return data_source(std::make_unique<page_cache_compaction_source_impl>(std::move(in), input, offset));
}

data_sink make_page_cache_compaction_sink(data_sink sink, shared_ptr<page_cache_compaction_output> output,
        component_type type, const compression* compression, std::function<bool()> fixed_size_pages) {
    auto* ranges = output->hot_ranges(type);
    if (!ranges) {
        return sink;
    }
    return data_sink(std::make_unique<page_cache_compaction_sink_impl>(std::move(sink), std::move(output), *ranges,
            type, compression, std::move(fixed_size_pages)));
}

data_sink maybe_make_page_cache_compaction_sink(const sstable& sst, component_type type, data_sink sink) {
    if (!sst.sstable_identifier()) {
        return sink;
    }
    auto output = page_cache_compaction_output::find(*sst.sstable_identifier());
    if (!output) {
        return sink;
    }
    // The sink is made before the writer sets up the compression of the
    // Data component, and before a file I/O extension transforms the
    // component (which it does on the sink made here), so only whether the
    // sstable is compressed is known here, and whether it is transformed is
    // asked when its first page is written.
    auto* compression = type == component_type::Data && sst.has_component(component_type::CompressionInfo)
            ? &sst.get_compression() : nullptr;
    return make_page_cache_compaction_sink(std::move(sink), std::move(output), type, compression,
            [&sst] { return transformed(sst); });
}

} // namespace sstables

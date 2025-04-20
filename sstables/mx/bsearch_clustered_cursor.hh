/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "sstables/index_entry.hh"
#include "sstables/column_translation.hh"
#include "parsers.hh"
#include "schema/schema.hh"
#include "utils/assert.hh"
#include "utils/cached_file.hh"
#include "utils/to_string.hh"

#include <seastar/core/byteorder.hh>
#include <seastar/core/on_internal_error.hh>

#include <optional>
#include <fmt/std.h>

namespace sstables {

extern logging::logger sstlog;

}

namespace sstables::mc {

/// A read-through cache of promoted index entries.
///
/// Designed for a single user. Methods must not be invoked concurrently.
///
/// All methods provide basic exception guarantee.
class cached_promoted_index {
public:
    using pi_index_type = uint32_t;  // promoted index block sequence number, 0 .. _blocks_count
    using pi_offset_type = uint32_t; // Offset into the promoted index region in the index file, relative
                                     // to the start of the promoted index.

    // Can be in one of the three states, with increasing number of fields being valid:
    //
    //  l0) start is not engaged: only index and offset is valid
    //  l1) start is engaged: in addition to the above, start is valid
    //  l2) end is engaged: all fields are valid
    //
    // This is in order to save on CPU by avoiding parsing the whole block during binary search,
    // which only needs the "start" field.
    struct promoted_index_block {
        pi_index_type index;
        pi_offset_type offset;
        std::optional<position_in_partition> start;
        std::optional<position_in_partition> end;
        std::optional<deletion_time> end_open_marker;
        uint64_t data_file_offset;
        uint64_t width;

        promoted_index_block(pi_index_type index, pi_offset_type offset)
            : index(index)
            , offset(offset)
        {}

        bool operator<(const promoted_index_block& other) const { return index < other.index; }
        bool operator==(const promoted_index_block& other) const { return index == other.index; }

        /// \brief Returns the amount of memory occupied by this object and all its contents.
        size_t memory_usage() const {
            size_t result = sizeof(promoted_index_block);
            if (!start) {
                return result;
            }
            result += start->external_memory_usage();
            if (!end) {
                return result;
            }
            result += end->external_memory_usage();
            return result;
        }
    };

    struct metrics {
        uint64_t hits_l0 = 0; // Number of requests for promoted_index_block in state l0
                              // which didn't have to go to the page cache
        uint64_t hits_l1 = 0; // Number of requests for promoted_index_block in state l1
                              // which didn't have to go to the page cache
        uint64_t hits_l2 = 0; // Number of requests for promoted_index_block in state l2
                              // which didn't have to go to the page cache
        uint64_t misses_l0 = 0; // Number of requests for promoted_index_block in state l0
                                // which didn't have to go to the page cache
        uint64_t misses_l1 = 0; // Number of requests for promoted_index_block in state l1
                                // which didn't have to go to the page cache
        uint64_t misses_l2 = 0; // Number of requests for promoted_index_block in state l2
                                // which didn't have to go to the page cache

        uint64_t evictions = 0; // Number of promoted_index_blocks which got evicted
        uint64_t populations = 0; // Number of promoted_index_blocks which got inserted
        uint64_t block_count = 0; // Number of promoted_index_blocks currently cached
        uint64_t used_bytes = 0; // Number of bytes currently used by promoted_index_blocks
    };

    struct block_comparator {
        using is_transparent = void; // for std::set to see the lower_bound() overload which takes any key.

        const schema& _s;

        bool operator()(const promoted_index_block& lhs, position_in_partition_view rhs) const {
            SCYLLA_ASSERT(lhs.start);
            position_in_partition::less_compare less(_s);
            return less(*lhs.start, rhs);
        }

        bool operator()(position_in_partition_view lhs, const promoted_index_block& rhs) const {
            SCYLLA_ASSERT(rhs.start);
            position_in_partition::less_compare less(_s);
            return less(lhs, *rhs.start);
        }

        bool operator()(const promoted_index_block& lhs, const promoted_index_block& rhs) const {
            return lhs < rhs;
        }

        bool operator()(const promoted_index_block& lhs, pi_index_type rhs) const {
            return lhs.index < rhs;
        }

        bool operator()(pi_index_type lhs, const promoted_index_block& rhs) const {
            return lhs < rhs.index;
        }
    };
private:
    // Cache of the parsed promoted index blocks.
    //
    // Why have it? We could have cached only at the cached_file level
    // and parse on each get_block_*(). Caching parsed blocks is still
    // useful for implementing upper_bound_cache_only(), which can leverage
    // materialized _blocks to find approximation of the upper bound.
    //
    // Index lookups are I/O bound and amortized by reading from the data file,
    // so extra CPU overhead to maintain the blocks is not noticeable and
    // savings in CPU time from less over-reads more than compensate
    // for it.
    //
    using block_set_type = std::set<promoted_index_block, block_comparator>;
    block_set_type _blocks;
private:
    using Buffer = cached_file::page_view;

    struct u32_parser {
        data_consumer::primitive_consumer_impl<Buffer>& parser;

        void reset() {
            parser.read_32();
        }

        data_consumer::read_status consume(Buffer& buf) {
            return parser.consume_u32(buf);
        }

        uint32_t value() const {
            return parser._u32;
        }
    };
public:
    const schema& _s;
    uint64_t _promoted_index_start;
    uint64_t _promoted_index_size;
    metrics& _metrics;
    const pi_index_type _blocks_count;
    cached_file& _cached_file;
    data_consumer::primitive_consumer_impl<Buffer> _primitive_parser;
    u32_parser _u32_parser;
    column_translation _ctr;
    clustering_parser<Buffer> _clustering_parser;
    promoted_index_block_parser<Buffer> _block_parser;
    reader_permit _permit;
    cached_file::stream _stream;
    logalloc::allocating_section _as;
private:
    template <typename Consumer>
    future<> read(cached_file::offset_type pos, tracing::trace_state_ptr trace_state, Consumer& c) {
        struct retry_exception : std::exception {};
        _stream = _cached_file.read(pos, _permit, trace_state);
        c.reset();
        return repeat([this, pos, trace_state, &c] {
            return _stream.next_page_view().then([this, &c] (cached_file::page_view&& page) {
                if (!page) {
                    on_internal_error(sstlog, "End of stream while parsing");
                }
                bool retry = false;
                return _as(_cached_file.region(), [&] {
                    if (retry) {
                        throw retry_exception();
                    }
                    retry = true;

                    auto status = c.consume(page);

                    utils::get_local_injector().inject("cached_promoted_index_parsing_invalidate_buf_across_page", [&page] {
                        page.release_and_scramble();
                    });

                    utils::get_local_injector().inject("cached_promoted_index_bad_alloc_parsing_across_page", [this] {
                        // Prevent reserve explosion in testing.
                        _as.set_lsa_reserve(1);
                        _as.set_std_reserve(1);
                        throw std::bad_alloc();
                    });

                    return stop_iteration(status == data_consumer::read_status::ready);
                });
            }).handle_exception_type([this, pos, trace_state, &c] (const retry_exception& e) {
                _stream = _cached_file.read(pos, _permit, trace_state);
                c.reset();
                return stop_iteration::no;
            });
        });
    }

    // Returns offset of the entry in the offset map for the promoted index block of the index idx.
    // The offset is relative to the promoted index start in the index file.
    // idx must be in the range 0..(_blocks_count-1)
    pi_offset_type get_offset_entry_pos(pi_index_type idx) const {
        return _promoted_index_size - (_blocks_count - idx) * sizeof(pi_offset_type);
    }

    future<pi_offset_type> read_block_offset(pi_index_type idx, tracing::trace_state_ptr trace_state);

    // Postconditions:
    //   - block.start is engaged and valid.
    future<> read_block_start(promoted_index_block& block, tracing::trace_state_ptr trace_state);

    // Postconditions:
    //   - block.end is engaged, all fields in the block are valid
    future<> read_block(promoted_index_block& block, tracing::trace_state_ptr trace_state);

public:
    /// \brief Returns a pointer to promoted_index_block entry which has at least offset and index fields valid.
    future<promoted_index_block*> get_block_only_offset(pi_index_type idx, tracing::trace_state_ptr trace_state) {
        auto i = _blocks.lower_bound(idx);
        if (i != _blocks.end() && i->index == idx) {
            ++_metrics.hits_l0;
            return make_ready_future<promoted_index_block*>(const_cast<promoted_index_block*>(&*i));
        }
        ++_metrics.misses_l0;
        return read_block_offset(idx, trace_state).then([this, idx, hint = i] (pi_offset_type offset) {
            auto i = this->_blocks.emplace_hint(hint, idx, offset);
            _metrics.used_bytes += sizeof(promoted_index_block);
            ++_metrics.block_count;
            ++_metrics.populations;
            return const_cast<promoted_index_block*>(&*i);
        });
    }

private:
    void erase_range(block_set_type::iterator begin, block_set_type::iterator end) {
        while (begin != end) {
            --_metrics.block_count;
            ++_metrics.evictions;
            _metrics.used_bytes -= begin->memory_usage();
            begin = _blocks.erase(begin);
        }
    }
public:
    cached_promoted_index(const schema& s,
            uint64_t promoted_index_start,
            uint64_t promoted_index_size,
            metrics& m,
            reader_permit permit,
            column_translation ctr,
            cached_file& f,
            pi_index_type blocks_count)
        : _blocks(block_comparator{s})
        , _s(s)
        , _promoted_index_start(promoted_index_start)
        , _promoted_index_size(promoted_index_size)
        , _metrics(m)
        , _blocks_count(blocks_count)
        , _cached_file(f)
        , _primitive_parser(permit)
        , _u32_parser(_primitive_parser)
        , _ctr(std::move(ctr))
        , _clustering_parser(s, permit, _ctr.clustering_column_value_fix_legths(), true)
        , _block_parser(s, permit, _ctr.clustering_column_value_fix_legths())
        , _permit(std::move(permit))
    { }

    ~cached_promoted_index() {
        _metrics.block_count -= _blocks.size();
        _metrics.evictions += _blocks.size();
        for (auto&& b : _blocks) {
            _metrics.used_bytes -= b.memory_usage();
        }
    }

    /// \brief Returns a pointer to promoted_index_block entry which has at least offset, index and start fields valid.
    future<promoted_index_block*> get_block_with_start(pi_index_type idx, tracing::trace_state_ptr trace_state) {
        return get_block_only_offset(idx, trace_state).then([this, trace_state] (promoted_index_block* block) {
            if (block->start) {
                ++_metrics.hits_l1;
                return make_ready_future<promoted_index_block*>(block);
            }
            ++_metrics.misses_l1;
            return read_block_start(*block, trace_state).then([block] { return block; });
        });
    }

    /// \brief Returns a pointer to promoted_index_block entry which has all the fields valid.
    future<promoted_index_block*> get_block(pi_index_type idx, tracing::trace_state_ptr trace_state) {
        return get_block_only_offset(idx, trace_state).then([this, trace_state] (promoted_index_block* block) {
            if (block->end) {
                ++_metrics.hits_l2;
                return make_ready_future<promoted_index_block*>(block);
            }
            ++_metrics.misses_l2;
            return read_block(*block, trace_state).then([block] { return block; });
        });
    }

    /// \brief Returns a data file offset into the partition such that all fragments
    /// that follow have strictly greater positions than pos.
    ///
    /// The returned information can be useful in determining the I/O boundary for read-ahead.
    ///
    /// \note There may be still elements with positions > pos before the returned position,
    /// so this does not return an exact upper bound.
    ///
    /// Resolving with std::nullopt means the position is not known. The caller should
    /// use the end of the partition as the upper bound.
    future<std::optional<uint64_t>> upper_bound_cache_only(position_in_partition_view pos,
                                                           tracing::trace_state_ptr trace_state,
                                                           sstable_enabled_features features) {
        auto i = _blocks.upper_bound(pos);
        if (i == _blocks.end()) {
            if (i != _blocks.begin() && features.is_enabled(CorrectLastPiBlockWidth)) [[likely]] {
                i--;
                auto& block = const_cast<promoted_index_block&>(*i);
                if (block.index == _blocks_count - 1) {
                    if (!block.end) {
                        return read_block(block, trace_state).then([&block] {
                            return make_ready_future<std::optional<uint64_t>>(block.data_file_offset + block.width);
                        });
                    }
                    tracing::trace(trace_state, "upper_bound_cache_only({}): past last block", pos);
                    return make_ready_future<std::optional<uint64_t>>(block.data_file_offset + block.width);
                }
            }
            tracing::trace(trace_state, "upper_bound_cache_only({}): no upper bound", pos);
            return make_ready_future<std::optional<uint64_t>>(std::nullopt);
        }
        auto& block = const_cast<promoted_index_block&>(*i);
        if (!block.end) {
            return read_block(block, trace_state).then([&block] {
                return make_ready_future<std::optional<uint64_t>>(block.data_file_offset);
            });
        }
        tracing::trace(trace_state, "upper_bound_cache_only({}): index={}, offset={}", pos, block.index, block.data_file_offset);
        return make_ready_future<std::optional<uint64_t>>(block.data_file_offset);
    }

    // Invalidates information about blocks with smaller indexes than a given block.
    void invalidate_prior(promoted_index_block* block, tracing::trace_state_ptr trace_state) {
        erase_range(_blocks.begin(), _blocks.lower_bound(block->index));
    }

    void clear() {
        erase_range(_blocks.begin(), _blocks.end());
    }

    cached_file& file() { return _cached_file; }
};
} // namespace sstables::mc

template <>
struct fmt::formatter<sstables::mc::cached_promoted_index::promoted_index_block> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const sstables::mc::cached_promoted_index::promoted_index_block& b, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
        return fmt::format_to(
            ctx.out(),
            "{{idx={}, offset={}, start={}, end={}, end_open_marker={}, datafile_offset={}, width={}}}",
            b.index, b.offset, b.start, b.end, b.end_open_marker, b.data_file_offset, b.width);
    }
};

namespace sstables::mc {

inline
future<cached_promoted_index::pi_offset_type>
cached_promoted_index::read_block_offset(pi_index_type idx, tracing::trace_state_ptr trace_state) {
    return read(_promoted_index_start + get_offset_entry_pos(idx), trace_state, _u32_parser).then([idx, this] {
        sstlog.trace("cached_promoted_index {}: read_block_offset: idx: {}, offset: {}", fmt::ptr(this), idx, _u32_parser.value());
        return _u32_parser.value();
    });
}

inline
future<> cached_promoted_index::read_block_start(promoted_index_block& block, tracing::trace_state_ptr trace_state) {
    return read(_promoted_index_start + block.offset, trace_state, _clustering_parser).then([this, &block] {
        auto mem_before = block.memory_usage();
        block.start.emplace(_clustering_parser.get_and_reset());
        sstlog.trace("cached_promoted_index {}: read_block_start: {}", fmt::ptr(this), block);
        _metrics.used_bytes += block.memory_usage() - mem_before;
    });
}

inline
future<> cached_promoted_index::read_block(promoted_index_block& block, tracing::trace_state_ptr trace_state) {
    return read(_promoted_index_start + block.offset, trace_state, _block_parser).then([this, &block] {
        auto mem_before = block.memory_usage();
        block.start.emplace(std::move(_block_parser.start()));
        block.end.emplace(std::move(_block_parser.end()));
        block.end_open_marker = _block_parser.end_open_marker();
        block.data_file_offset = _block_parser.offset();
        block.width = _block_parser.width();
        _metrics.used_bytes += block.memory_usage() - mem_before;
        sstlog.trace("cached_promoted_index {}: read_block: {}", fmt::ptr(this), block);
    });
}

/// Cursor implementation which does binary search over index entries.
///
/// Memory consumption: O(log(N))
///
/// Worst-case lookup cost:
///
///    comparisons: O(log(N))
///    I/O:         O(log(N))
///
/// N = number of index entries
///
class bsearch_clustered_cursor : public clustered_index_cursor {
    using pi_offset_type = cached_promoted_index::pi_offset_type;
    using pi_index_type = cached_promoted_index::pi_index_type;
    using promoted_index_block = cached_promoted_index::promoted_index_block;

    const schema& _s;
    const pi_index_type _blocks_count;
    seastar::shared_ptr<cached_file> _cached_file;
    cached_promoted_index _promoted_index;

    // Points to the block whose start is greater than the position of the cursor (its upper bound).
    pi_index_type _current_idx = 0;

    // Used internally by advance_to_upper_bound() to avoid allocating state.
    pi_index_type _upper_idx;

    // Points to the upper bound of the cursor.
    std::optional<position_in_partition> _current_pos;

    skip_info _skip_info = {0, tombstone(), position_in_partition::before_all_clustered_rows()};

    tracing::trace_state_ptr _trace_state;
    sstable_enabled_features _features;
private:
    // Advances the cursor to the nearest block whose start position is > pos.
    //
    // upper_idx should be the index of the block which is known to have start position > pos.
    // upper_idx can be set to _blocks_count if no such entry is known.
    //
    // Async calls must be serialized.
    future<> advance_to_upper_bound(position_in_partition_view pos) {
        // Binary search over blocks.
        //
        // Post conditions:
        //
        //   pos < get_block_start(_current_idx)
        //   For each i < _current_idx: pos >= get_block_start(i)
        //
        // Invariants:
        //
        //   pos < get_block_start(_upper_idx) [*]
        //   pos >= get_block_start(_current_idx)
        //
        // [*] Assuming get_block_start(_blocks_count) == position_in_partition::after_all_clustered_rows().
        //
        //     get_block_start(x) = *_promoted_index.get_block_with_start(x).start
        //
        // Eventually _current_idx will reach _upper_idx.

        _upper_idx = _blocks_count;
        return repeat([this, pos] {
            if (_current_idx >= _upper_idx) {
                if (_current_idx == _blocks_count) {
                    _current_pos = position_in_partition::after_all_clustered_rows();
                }
                tracing::trace(_trace_state, "mc_bsearch_clustered_cursor: bisecting done, current=[{}] .start={}", _current_idx, _current_pos);
                sstlog.trace("mc_bsearch_clustered_cursor {}: bisecting done, current=[{}] .start={}", fmt::ptr(this), _current_idx, _current_pos);
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }

            auto mid = _current_idx + (_upper_idx - _current_idx) / 2;
            tracing::trace(_trace_state, "mc_bsearch_clustered_cursor: bisecting range [{}, {}], mid={}", _current_idx, _upper_idx, mid);
            sstlog.trace("mc_bsearch_clustered_cursor {}: bisecting range [{}, {}], mid={}", fmt::ptr(this), _current_idx, _upper_idx, mid);
            return _promoted_index.get_block_with_start(mid, _trace_state).then([this, mid, pos] (promoted_index_block* block) {
                position_in_partition::less_compare less(_s);
                sstlog.trace("mc_bsearch_clustered_cursor {}: compare with [{}] .start={}", fmt::ptr(this), mid, block->start);
                if (less(pos, *block->start)) {
                    // Eventually _current_idx will reach _upper_idx, so _current_pos only needs to be
                    // updated whenever _upper_idx changes.
                    _current_pos = *block->start;
                    _upper_idx = mid;
                } else {
                    _current_idx = mid + 1;
                }
                return stop_iteration::no;
            });
        });
    }
public:
    bsearch_clustered_cursor(const schema& s,
            uint64_t promoted_index_start,
            uint64_t promoted_index_size,
            cached_promoted_index::metrics& metrics,
            reader_permit permit,
            column_translation ctr,
            seastar::shared_ptr<cached_file> f,
            pi_index_type blocks_count,
            tracing::trace_state_ptr trace_state,
            sstable_enabled_features features)
        : _s(s)
        , _blocks_count(blocks_count)
        , _cached_file(std::move(f))
        , _promoted_index(s,
            promoted_index_start,
            promoted_index_size,
            metrics,
            std::move(permit),
            std::move(ctr),
            *_cached_file,
            blocks_count)
        , _trace_state(std::move(trace_state))
        , _features(features)
    { }

    cached_promoted_index& promoted_index() { return _promoted_index; }

    skip_info current_block() override {
        return _skip_info;
    }

    future<std::optional<skip_info>> advance_to(position_in_partition_view pos) override {
        position_in_partition::less_compare less(_s);

        sstlog.trace("mc_bsearch_clustered_cursor {}: advance_to({}), _current_pos={}, _current_idx={}, cached={}",
            fmt::ptr(this), pos, _current_pos, _current_idx, _promoted_index.file().cached_bytes());
        tracing::trace(_trace_state, "mc_bsearch_clustered_cursor {}: advance_to({}), _current_pos={}, _current_idx={}",
            fmt::ptr(this), pos, _current_pos, _current_idx);

        if (_current_pos) {
            if (less(pos, *_current_pos)) {
                sstlog.trace("mc_bsearch_clustered_cursor {}: same block", fmt::ptr(this));
                return make_ready_future<std::optional<skip_info>>(std::nullopt);
            }
            ++_current_idx;
        }

        return advance_to_upper_bound(pos).then([this, pos] {
            if (_current_idx == 0) {
                sstlog.trace("mc_bsearch_clustered_cursor {}: same block", fmt::ptr(this));
                return _promoted_index.get_block(_current_idx, _trace_state).then([this] (promoted_index_block* block) {
                    _skip_info = skip_info{block->data_file_offset, tombstone(), position_in_partition::before_all_clustered_rows()};
                    sstlog.trace("mc_bsearch_clustered_cursor {}: {}", fmt::ptr(this), _skip_info);
                });
            }
            return _promoted_index.get_block(_current_idx - 1, _trace_state).then([this, pos] (promoted_index_block* block) {
                sstlog.trace("mc_bsearch_clustered_cursor {}: [{}] = {}", fmt::ptr(this), block->index, *block);
                position_in_partition::less_compare less(_s);
                if (less(*block->end, pos) && _features.is_enabled(CorrectLastPiBlockWidth)) {
                    sstlog.trace("mc_bsearch_clustered_cursor {}: Move to next block", fmt::ptr(this));
                    if (_current_idx == _blocks_count) {
                        _current_pos = position_in_partition::after_all_clustered_rows();
                        _skip_info = skip_info{block->data_file_offset + block->width, tombstone(),
                                               position_in_partition::before_all_clustered_rows()};
                        sstlog.trace("mc_bsearch_clustered_cursor {}: {}", fmt::ptr(this), _skip_info);
                        return make_ready_future<>();
                    }
                    tombstone tomb;
                    auto tomb_pos = position_in_partition::before_all_clustered_rows();
                    if (block->end_open_marker) {
                        tomb = tombstone(*block->end_open_marker);
                        tomb_pos = *block->end;
                    }
                    _promoted_index.invalidate_prior(block, _trace_state);
                    ++_current_idx;
                    return _promoted_index.get_block(_current_idx - 1, _trace_state).then([this, tomb, tomb_pos = std::move(tomb_pos)] (promoted_index_block* block) mutable {
                        sstlog.trace("mc_bsearch_clustered_cursor {}: [{}] = {}", fmt::ptr(this), block->index, *block);
                        _skip_info = skip_info{block->data_file_offset, tomb, std::move(tomb_pos)};
                        sstlog.trace("mc_bsearch_clustered_cursor {}: {}", fmt::ptr(this), _skip_info);
                        if (_current_idx == _blocks_count) {
                            _current_pos = position_in_partition::after_all_clustered_rows();
                            return make_ready_future<>();
                        }
                        return _promoted_index.get_block_with_start(_current_idx, _trace_state).then([this] (promoted_index_block* block) {
                            _current_pos = *block->start;
                        });
                    });
                }
                offset_in_partition datafile_offset = block->data_file_offset;
                sstlog.trace("mc_bsearch_clustered_cursor {}: datafile_offset={}", fmt::ptr(this), datafile_offset);
                if (_current_idx < 2) {
                    _skip_info = skip_info{datafile_offset, tombstone(), position_in_partition::before_all_clustered_rows()};
                    return make_ready_future<>();
                }
                // _current_idx points to the block whose start is > than the cursor (upper bound).
                // The cursor is in _current_idx - 1. We will tell the data file reader to skip to
                // the beginning of _current_idx - 1. The index block contains tombstone which was
                // active at the end of the block, not at the beginning of the block, so we need
                // to read the active tombstone from the preceding block, _current_idx - 2.
                return _promoted_index.get_block(_current_idx - 2, _trace_state)
                        .then([this, datafile_offset] (promoted_index_block* block) {
                    sstlog.trace("mc_bsearch_clustered_cursor {}: [{}] = {}", fmt::ptr(this), _current_idx - 2, *block);
                    // XXX: Until we have automatic eviction, we need to invalidate cached index blocks
                    // as we walk so that memory footprint is not O(N) but O(log(N)).
                    _promoted_index.invalidate_prior(block, _trace_state);
                    tombstone tomb;
                    position_in_partition tomb_pos = position_in_partition::before_all_clustered_rows();
                    if (block->end_open_marker) {
                        tomb = tombstone(*block->end_open_marker);
                        tomb_pos = *block->end;
                    }
                    _skip_info = skip_info{datafile_offset, tomb, std::move(tomb_pos)};
                });
            });
        }).then([this] {
            sstlog.trace("mc_bsearch_clustered_cursor {}: {}", fmt::ptr(this), _skip_info);
            tracing::trace(_trace_state, "mc_bsearch_clustered_cursor {}: {}", fmt::ptr(this), _skip_info);
            return std::make_optional(_skip_info);
        });
    }

    future<std::optional<offset_in_partition>> probe_upper_bound(position_in_partition_view pos) override {
        return _promoted_index.upper_bound_cache_only(pos, _trace_state, _features);
    }

    future<std::optional<entry_info>> next_entry() override {
        if (_current_idx == _blocks_count) {
            return make_ready_future<std::optional<entry_info>>(std::nullopt);
        }
        return _promoted_index.get_block(_current_idx, _trace_state)
                .then([this] (promoted_index_block* block) -> std::optional<entry_info> {
            sstlog.trace("mc_bsearch_clustered_cursor {}: block {}: start={}, end={}, offset={}", fmt::ptr(this), _current_idx,
                *block->start, *block->end, block->data_file_offset);
            ++_current_idx;
            return entry_info{*block->start, *block->end, block->data_file_offset};
        });
    }

    // Advances the cursor to the first promoted index block whose start position is greater than `pos`
    // or to the end if there is no such block.
    //
    // If the block existed and advancing was successful (i.e. we weren't already at this block),
    // returns `skip_info` describing this block. Otherwise returns nullopt.
    future<std::optional<skip_info>> advance_past(position_in_partition_view pos) {
        return advance_to_upper_bound(pos).then([this] {
            if (_current_idx == _blocks_count) {
                return make_ready_future<std::optional<skip_info>>(std::nullopt);
            }
            return _promoted_index.get_block(_current_idx, _trace_state).then([this] (promoted_index_block* block) {
                offset_in_partition datafile_offset = block->data_file_offset;
                if (_current_idx == 0) {
                    return make_ready_future<std::optional<skip_info>>(skip_info{datafile_offset, tombstone(), position_in_partition::before_all_clustered_rows()});
                }
                return _promoted_index.get_block(_current_idx - 1, _trace_state)
                        .then([this, datafile_offset] (promoted_index_block* block) -> std::optional<skip_info> {
                    _promoted_index.invalidate_prior(block, _trace_state);
                    if (!block->end_open_marker) {
                        return skip_info{datafile_offset, tombstone(), position_in_partition::before_all_clustered_rows()};
                    }
                    auto tomb = tombstone(*block->end_open_marker);
                    return skip_info{datafile_offset, tomb, *block->end};
                });
            });
        });
    }

    // Returns the offset in the data file of the first row in the last promoted index block
    // (shortly: the offset of the last promoted index block in the data file), or nullopt
    // if there are no blocks.
    future<std::optional<uint64_t>> last_block_offset() {
        if (_blocks_count == 0) {
            return make_ready_future<std::optional<uint64_t>>();
        }
        return _promoted_index.get_block(_blocks_count - 1, _trace_state)
                .then([] (promoted_index_block* block) {
            return std::optional<uint64_t>(block->data_file_offset);
        });
    }

    future<> close() noexcept override {
        return make_ready_future<>();
    }
};

}

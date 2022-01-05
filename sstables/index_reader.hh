/*
 * Copyright (C) 2015-present ScyllaDB
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

#pragma once
#include "sstables.hh"
#include "consumer.hh"
#include "downsampling.hh"
#include "sstables/partition_index_cache.hh"
#include <seastar/util/bool_class.hh>
#include "utils/buffer_input_stream.hh"
#include "sstables/prepended_input_stream.hh"
#include "tracing/traced_file.hh"
#include "sstables/scanning_clustered_index_cursor.hh"
#include "sstables/mx/bsearch_clustered_cursor.hh"
#include "sstables/sstables_manager.hh"

namespace sstables {

extern seastar::logger sstlog;
extern thread_local cached_file::metrics index_page_cache_metrics;
extern thread_local mc::cached_promoted_index::metrics promoted_index_cache_metrics;

// Promoted index information produced by the parser.
struct parsed_promoted_index_entry {
    deletion_time del_time;
    uint64_t promoted_index_start;
    uint32_t promoted_index_size;
    uint32_t num_blocks;
};

// Partition index entry information produced by the parser.
struct parsed_partition_index_entry {
    temporary_buffer<char> key;
    uint64_t data_file_offset;
    uint64_t index_offset;
    std::optional<parsed_promoted_index_entry> promoted_index;
};

template <typename C>
concept PartitionIndexConsumer = requires(C c, parsed_partition_index_entry e) {
    // Called in the standard allocator context, outside allocating section.
    { c.consume_entry(std::move(e)) } -> std::same_as<void>;
};

// Partition index page builder.
// Implements PartitionIndexConsumer.
class index_consumer {
    schema_ptr _s;
    logalloc::allocating_section _alloc_section;
    logalloc::region& _region;
    size_t _size;
public:
    index_list indexes;

    index_consumer(logalloc::region& r, schema_ptr s, uint64_t size)
        : _s(std::move(s))
        , _region(r)
        , _size(size)
    {
        _alloc_section(_region, [&] {
            with_allocator(_region.allocator(), [&] {
                indexes._entries.reserve(_size);
            });
        });
    }

    ~index_consumer() {
        with_allocator(_region.allocator(), [&] {
            indexes._entries.clear_and_release();
        });
    }

    void consume_entry(parsed_partition_index_entry&& e) {
        _alloc_section(_region, [&] {
            with_allocator(_region.allocator(), [&] {
                managed_ref<promoted_index> pi;
                if (e.promoted_index) {
                    pi = make_managed<promoted_index>(*_s,
                            e.promoted_index->del_time,
                            e.promoted_index->promoted_index_start,
                            e.promoted_index->promoted_index_size,
                            e.promoted_index->num_blocks);
                }
                auto key = managed_bytes(reinterpret_cast<const blob_storage::char_type*>(e.key.get()), e.key.size());
                indexes._entries.emplace_back(make_managed<index_entry>(std::move(key), e.data_file_offset, std::move(pi)));
            });
        });
    }
};

// See #2993
class trust_promoted_index_tag;
using trust_promoted_index = bool_class<trust_promoted_index_tag>;

// TODO: make it templated on SSTables version since the exact format can be passed in at compile time
template <class IndexConsumer>
requires PartitionIndexConsumer<IndexConsumer>
class index_consume_entry_context : public data_consumer::continuous_data_consumer<index_consume_entry_context<IndexConsumer>> {
    using proceed = data_consumer::proceed;
    using processing_result = data_consumer::processing_result;
    using continuous_data_consumer = data_consumer::continuous_data_consumer<index_consume_entry_context<IndexConsumer>>;
    using read_status = typename continuous_data_consumer::read_status;
private:
    const sstable& _sst;
    IndexConsumer& _consumer;
    file _index_file;
    uint64_t _entry_offset;

    enum class state {
        START,
        KEY_SIZE,
        KEY_BYTES,
        POSITION,
        PROMOTED_SIZE,
        PARTITION_HEADER_LENGTH_1,
        PARTITION_HEADER_LENGTH_2,
        LOCAL_DELETION_TIME,
        MARKED_FOR_DELETE_AT,
        NUM_PROMOTED_INDEX_BLOCKS,
        CONSUME_ENTRY,
    } _state = state::START;

    friend std::ostream& operator<<(std::ostream& out, const state& s) {
        switch (s) {
        case state::START: return out <<  "START";
        case state::KEY_SIZE: return out <<  "KEY_SIZE";
        case state::KEY_BYTES: return out <<  "KEY_BYTES";
        case state::POSITION: return out <<  "POSITION";
        case state::PROMOTED_SIZE: return out <<  "PROMOTED_SIZE";
        case state::PARTITION_HEADER_LENGTH_1: return out <<  "PARTITION_HEADER_LENGTH_1";
        case state::PARTITION_HEADER_LENGTH_2: return out <<  "PARTITION_HEADER_LENGTH_2";
        case state::LOCAL_DELETION_TIME: return out <<  "LOCAL_DELETION_TIME";
        case state::MARKED_FOR_DELETE_AT: return out <<  "MARKED_FOR_DELETE_AT";
        case state::NUM_PROMOTED_INDEX_BLOCKS: return out <<  "NUM_PROMOTED_INDEX_BLOCKS";
        case state::CONSUME_ENTRY: return out <<  "CONSUME_ENTRY";
        }
        abort();
    }

    temporary_buffer<char> _key;
    uint64_t _promoted_index_end;
    uint64_t _position;
    uint64_t _partition_header_length = 0;
    std::optional<deletion_time> _deletion_time;

    trust_promoted_index _trust_pi;
    std::optional<column_values_fixed_lengths> _ck_values_fixed_lengths;
    tracing::trace_state_ptr _trace_state;

    inline bool is_mc_format() const { return static_cast<bool>(_ck_values_fixed_lengths); }

public:
    void verify_end_state() const {
        if (this->_remain > 0) {
            throw malformed_sstable_exception(fmt::format("index_consume_entry_context (state={}): parsing ended but there is unconsumed data", _state), _sst.filename(component_type::Index));
        }
        if (_state != state::KEY_SIZE && _state != state::START) {
            throw malformed_sstable_exception(fmt::format("index_consume_entry_context (state={}): cannot finish parsing current entry, no more data", _state), _sst.filename(component_type::Index));
        }
    }

    bool non_consuming() const {
        return ((_state == state::CONSUME_ENTRY) || (_state == state::START));
    }

    processing_result process_state(temporary_buffer<char>& data) {
        auto current_pos = [&] { return this->position() - data.size(); };
        auto read_vint_or_uint64 = [this] (temporary_buffer<char>& data) {
            return is_mc_format() ? this->read_unsigned_vint(data) : this->read_64(data);
        };
        auto read_vint_or_uint32 = [this] (temporary_buffer<char>& data) {
            return is_mc_format() ? this->read_unsigned_vint(data) : this->read_32(data);
        };
        auto get_uint32 = [this] {
            return is_mc_format() ? static_cast<uint32_t>(this->_u64) : this->_u32;
        };

        switch (_state) {
        // START comes first, to make the handling of the 0-quantity case simpler
        case state::START:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::START);
            _state = state::KEY_SIZE;
            break;
        case state::KEY_SIZE:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::KEY_SIZE);
            _entry_offset = current_pos();
            if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                _state = state::KEY_BYTES;
                break;
            }
        case state::KEY_BYTES:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::KEY_BYTES);
            if (this->read_bytes_contiguous(data, this->_u16, _key) != continuous_data_consumer::read_status::ready) {
                _state = state::POSITION;
                break;
            }
        case state::POSITION:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::POSITION);
            if (read_vint_or_uint64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::PROMOTED_SIZE;
                break;
            }
        case state::PROMOTED_SIZE:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::PROMOTED_SIZE);
            _position = this->_u64;
            if (read_vint_or_uint32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::PARTITION_HEADER_LENGTH_1;
                break;
            }
        case state::PARTITION_HEADER_LENGTH_1: {
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::PARTITION_HEADER_LENGTH_1);
            auto promoted_index_size_with_header = get_uint32();
            _promoted_index_end = current_pos() + promoted_index_size_with_header;
            if (promoted_index_size_with_header == 0) {
                _state = state::CONSUME_ENTRY;
                goto state_CONSUME_ENTRY;
            }
            if (!is_mc_format()) {
                // SSTables ka/la don't have a partition_header_length field
                _state = state::LOCAL_DELETION_TIME;
                goto state_LOCAL_DELETION_TIME;
            }
            if (this->read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                _state = state::PARTITION_HEADER_LENGTH_2;
                break;
            }
        }
        case state::PARTITION_HEADER_LENGTH_2:
            sstlog.trace("{}: pos {} state {} {}", fmt::ptr(this), current_pos(), state::PARTITION_HEADER_LENGTH_2, this->_u64);
            _partition_header_length = this->_u64;
        state_LOCAL_DELETION_TIME:
        case state::LOCAL_DELETION_TIME:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::LOCAL_DELETION_TIME);
            _deletion_time.emplace();
            if (this->read_32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::MARKED_FOR_DELETE_AT;
                break;
            }
        case state::MARKED_FOR_DELETE_AT:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::MARKED_FOR_DELETE_AT);
            _deletion_time->local_deletion_time = this->_u32;
            if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::NUM_PROMOTED_INDEX_BLOCKS;
                break;
            }
        case state::NUM_PROMOTED_INDEX_BLOCKS:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::NUM_PROMOTED_INDEX_BLOCKS);
            _deletion_time->marked_for_delete_at = this->_u64;
            if (read_vint_or_uint32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::CONSUME_ENTRY;
                break;
            }
        state_CONSUME_ENTRY:
        case state::CONSUME_ENTRY: {
            auto promoted_index_start = current_pos();
            auto promoted_index_size = _promoted_index_end - promoted_index_start;
            sstlog.trace("{}: pos {} state {} size {}", fmt::ptr(this), current_pos(), state::CONSUME_ENTRY, promoted_index_size);
            std::optional<parsed_promoted_index_entry> pi;
            if (_deletion_time && (_trust_pi == trust_promoted_index::yes) && (promoted_index_size > 0)) {
                pi.emplace();
                pi->num_blocks = get_uint32();
                pi->promoted_index_start = promoted_index_start;
                pi->promoted_index_size = promoted_index_size;
                pi->del_time = *_deletion_time;
            }
            _consumer.consume_entry(parsed_partition_index_entry{
                .key = std::move(_key),
                .data_file_offset = _position,
                .index_offset = _entry_offset,
                .promoted_index = std::move(pi)
            });
            auto data_size = data.size();
            _deletion_time = std::nullopt;
            _state = state::START;
            if (promoted_index_size <= data_size) {
                data.trim_front(promoted_index_size);
            } else {
                data.trim(0);
                sstlog.trace("{}: skip {} pos {} state {}", fmt::ptr(this), promoted_index_size - data_size, current_pos(), _state);
                return skip_bytes{promoted_index_size - data_size};
            }
        }
            break;
        }
        sstlog.trace("{}: exit pos {} state {}", fmt::ptr(this), current_pos(), _state);
        return proceed::yes;
    }

    index_consume_entry_context(const sstable& sst, reader_permit permit, IndexConsumer& consumer, trust_promoted_index trust_pi,
            file index_file, file_input_stream_options options, uint64_t start,
            uint64_t maxlen, std::optional<column_values_fixed_lengths> ck_values_fixed_lengths, tracing::trace_state_ptr trace_state = {})
        : continuous_data_consumer(std::move(permit), make_file_input_stream(index_file, start, maxlen, options), start, maxlen)
        , _sst(sst), _consumer(consumer), _index_file(index_file)
        , _entry_offset(start), _trust_pi(trust_pi), _ck_values_fixed_lengths(std::move(ck_values_fixed_lengths))
        , _trace_state(std::move(trace_state))
    {}
};

inline file make_tracked_index_file(sstable& sst, reader_permit permit, tracing::trace_state_ptr trace_state,
                                    use_caching caching) {
    auto f = caching ? sst.index_file() : sst.uncached_index_file();
    f = make_tracked_file(std::move(f), std::move(permit));
    if (!trace_state) {
        return f;
    }
    return tracing::make_traced_file(std::move(f), std::move(trace_state), format("{}:", sst.filename(component_type::Index)));
}

inline
std::unique_ptr<clustered_index_cursor> promoted_index::make_cursor(shared_sstable sst,
    reader_permit permit,
    tracing::trace_state_ptr trace_state,
    file_input_stream_options options,
    use_caching caching)
{
    std::optional<column_values_fixed_lengths> ck_values_fixed_lengths;
    if (sst->get_version() >= sstable_version_types::mc) {
        ck_values_fixed_lengths = std::make_optional(
            get_clustering_values_fixed_lengths(sst->get_serialization_header()));
    }

    if (sst->get_version() >= sstable_version_types::mc) {
        seastar::shared_ptr<cached_file> cached_file_ptr = caching
                ? sst->_cached_index_file
                : seastar::make_shared<cached_file>(make_tracked_index_file(*sst, permit, trace_state, caching),
                                                    index_page_cache_metrics,
                                                    sst->manager().get_cache_tracker().get_lru(),
                                                    sst->manager().get_cache_tracker().region(),
                                                    sst->_index_file_size);
        return std::make_unique<mc::bsearch_clustered_cursor>(*sst->get_schema(),
            _promoted_index_start, _promoted_index_size,
            promoted_index_cache_metrics, permit,
            *ck_values_fixed_lengths, cached_file_ptr, options.io_priority_class, _num_blocks, trace_state);
    }

    auto file = make_tracked_index_file(*sst, permit, std::move(trace_state), caching);
    auto promoted_index_stream = make_file_input_stream(std::move(file), _promoted_index_start, _promoted_index_size,options);
    return std::make_unique<scanning_clustered_index_cursor>(*sst->get_schema(), permit,
        std::move(promoted_index_stream), _promoted_index_size, _num_blocks, ck_values_fixed_lengths);
}

// Less-comparator for lookups in the partition index.
class index_comparator {
    dht::ring_position_comparator_for_sstables _tri_cmp;
public:
    index_comparator(const schema& s) : _tri_cmp(s) {}

    bool operator()(const summary_entry& e, dht::ring_position_view rp) const {
        return _tri_cmp(e.get_decorated_key(), rp) < 0;
    }

    bool operator()(const index_entry& e, dht::ring_position_view rp) const {
        return _tri_cmp(e.get_decorated_key(_tri_cmp.s), rp) < 0;
    }

    bool operator()(const managed_ref<index_entry>& e, dht::ring_position_view rp) const {
        return operator()(*e, rp);
    }

    bool operator()(dht::ring_position_view rp, const managed_ref<index_entry>& e) const {
        return operator()(rp, *e);
    }

    bool operator()(dht::ring_position_view rp, const summary_entry& e) const {
        return _tri_cmp(e.get_decorated_key(), rp) > 0;
    }

    bool operator()(dht::ring_position_view rp, const index_entry& e) const {
        return _tri_cmp(e.get_decorated_key(_tri_cmp.s), rp) > 0;
    }
};

// Stores information about open end RT marker
// of the lower index bound
struct open_rt_marker {
    position_in_partition pos;
    tombstone tomb;
};

// Contains information about index_reader position in the index file
struct index_bound {
    index_bound() = default;
    partition_index_cache::entry_ptr current_list;
    uint64_t previous_summary_idx = 0;
    uint64_t current_summary_idx = 0;
    uint64_t current_index_idx = 0;
    uint64_t current_pi_idx = 0; // Points to upper bound of the cursor.
    uint64_t data_file_position = 0;
    indexable_element element = indexable_element::partition;
    std::optional<open_rt_marker> end_open_marker;

    // Holds the cursor for the current partition. Lazily initialized.
    std::unique_ptr<clustered_index_cursor> clustered_cursor;

    // Cannot use default implementation because clustered_cursor is non-copyable.
    index_bound(const index_bound& other)
            : current_list(other.current_list)
            , previous_summary_idx(other.previous_summary_idx)
            , current_summary_idx(other.current_summary_idx)
            , current_index_idx(other.current_index_idx)
            , current_pi_idx(other.current_pi_idx)
            , data_file_position(other.data_file_position)
            , element(other.element)
            , end_open_marker(other.end_open_marker)
    { }

    index_bound(index_bound&&) noexcept = default;
    index_bound& operator=(index_bound&&) noexcept = default;
};

// Provides access to sstable indexes.
//
// Maintains logical cursors to sstable elements (partitions, cells).
// Holds two cursors pointing to the range within sstable (upper cursor may be not set).
// Initially the lower cursor is positioned on the first partition in the sstable.
// Lower cursor can be accessed and advanced from outside.
// Upper cursor can only be advanced along with the lower cursor and not accessed from outside.
//
// If eof() then the lower bound cursor is positioned past all partitions in the sstable.
class index_reader {
    shared_sstable _sstable;
    reader_permit _permit;
    const io_priority_class& _pc;
    tracing::trace_state_ptr _trace_state;
    std::unique_ptr<partition_index_cache> _local_index_cache; // Used when caching is disabled
    partition_index_cache& _index_cache;
    logalloc::allocating_section _alloc_section;
    logalloc::region& _region;
    use_caching _use_caching;

    struct reader {
        index_consumer _consumer;
        index_consume_entry_context<index_consumer> _context;

        inline static file_input_stream_options get_file_input_stream_options(shared_sstable sst, const io_priority_class& pc) {
            file_input_stream_options options;
            options.buffer_size = sst->sstable_buffer_size;
            options.read_ahead = 2;
            options.io_priority_class = pc;
            options.dynamic_adjustments = sst->_index_history;
            return options;
        }

        reader(shared_sstable sst, reader_permit permit, const io_priority_class& pc, tracing::trace_state_ptr trace_state, uint64_t begin, uint64_t end, uint64_t quantity,
               use_caching caching)
            : _consumer(sst->manager().get_cache_tracker().region(), sst->get_schema(), quantity)
            , _context(*sst, permit, _consumer,
                       trust_promoted_index(sst->has_correct_promoted_index_entries()),
                       make_tracked_index_file(*sst, permit, trace_state, caching),
                       get_file_input_stream_options(sst, pc), begin, end - begin,
                       (sst->get_version() >= sstable_version_types::mc
                           ? std::make_optional(get_clustering_values_fixed_lengths(sst->get_serialization_header()))
                           : std::optional<column_values_fixed_lengths>{}),
                       trace_state)
        { }
    };

private:
    index_bound _lower_bound;
    // Upper bound may remain uninitialized
    std::optional<index_bound> _upper_bound;

private:
    static future<> reset_clustered_cursor(index_bound& bound) noexcept {
        if (bound.clustered_cursor) {
            return bound.clustered_cursor->close().then([&bound] {
                bound.clustered_cursor.reset();
            });
        }
        return make_ready_future<>();
    }

    future<> advance_to_end(index_bound& bound) {
        sstlog.trace("index {}: advance_to_end() bound {}", fmt::ptr(this), fmt::ptr(&bound));
        bound.data_file_position = data_file_end();
        bound.element = indexable_element::partition;
        bound.current_list = {};
        bound.end_open_marker.reset();
        return reset_clustered_cursor(bound);
    }

    // Must be called for non-decreasing summary_idx.
    future<> advance_to_page(index_bound& bound, uint64_t summary_idx) {
        sstlog.trace("index {}: advance_to_page({}), bound {}", fmt::ptr(this), summary_idx, fmt::ptr(&bound));
        assert(!bound.current_list || bound.current_summary_idx <= summary_idx);
        if (bound.current_list && bound.current_summary_idx == summary_idx) {
            sstlog.trace("index {}: same page", fmt::ptr(this));
            return make_ready_future<>();
        }

        auto& summary = _sstable->get_summary();
        if (summary_idx >= summary.header.size) {
            sstlog.trace("index {}: eof", fmt::ptr(this));
            return advance_to_end(bound);
        }
        auto loader = [this] (uint64_t summary_idx) -> future<index_list> {
            auto& summary = _sstable->get_summary();
            uint64_t position = summary.entries[summary_idx].position;
            uint64_t quantity = downsampling::get_effective_index_interval_after_index(summary_idx, summary.header.sampling_level,
                summary.header.min_index_interval);

            uint64_t end;
            if (summary_idx + 1 >= summary.header.size) {
                end = _sstable->index_size();
            } else {
                end = summary.entries[summary_idx + 1].position;
            }

            return do_with(std::make_unique<reader>(_sstable, _permit, _pc, _trace_state, position, end, quantity, _use_caching), [this, summary_idx] (auto& entries_reader) {
                return entries_reader->_context.consume_input().then_wrapped([this, summary_idx, &entries_reader] (future<> f) {
                    std::exception_ptr ex;
                    if (f.failed()) {
                        ex = f.get_exception();
                        sstlog.error("failed reading index for {}: {}", _sstable->get_filename(), ex);
                    }
                    return entries_reader->_context.close().then([&entries_reader, ex = std::move(ex)] () mutable {
                        if (ex) {
                            return make_exception_future<index_list>(std::move(ex));
                        }
                        return make_ready_future<index_list>(std::move(entries_reader->_consumer.indexes));
                    });

                });
            });
        };

        return _index_cache.get_or_load(summary_idx, loader).then([this, &bound, summary_idx] (partition_index_cache::entry_ptr ref) {
            bound.current_list = std::move(ref);
            bound.current_summary_idx = summary_idx;
            bound.current_index_idx = 0;
            bound.current_pi_idx = 0;
            if (bound.current_list->empty()) {
                throw malformed_sstable_exception(format("missing index entry for summary index {} (bound {})", summary_idx, fmt::ptr(&bound)), _sstable->filename(component_type::Index));
            }
            bound.data_file_position = bound.current_list->_entries[0]->position();
            bound.element = indexable_element::partition;
            bound.end_open_marker.reset();

            if (sstlog.is_enabled(seastar::log_level::trace)) {
                sstlog.trace("index {} bound {}: page:", fmt::ptr(this), fmt::ptr(&bound));
                logalloc::reclaim_lock rl(_region);
                for (auto&& e : bound.current_list->_entries) {
                    auto dk = dht::decorate_key(*_sstable->_schema,
                        e->get_key().to_partition_key(*_sstable->_schema));
                    sstlog.trace("  {} -> {}", dk, e->position());
                }
            }

            return reset_clustered_cursor(bound);
        });
    }

    future<> advance_lower_to_start(const dht::partition_range &range) {
        if (range.start()) {
            return advance_to(_lower_bound,
                dht::ring_position_view(range.start()->value(),
                    dht::ring_position_view::after_key(!range.start()->is_inclusive())));
        }
        return make_ready_future<>();
    }

    future<> advance_upper_to_end(const dht::partition_range &range) {
        if (!_upper_bound) {
            _upper_bound.emplace();
        }
        if (range.end()) {
            return advance_to(*_upper_bound,
                dht::ring_position_view(range.end()->value(),
                    dht::ring_position_view::after_key(range.end()->is_inclusive())));
        }
        return advance_to_end(*_upper_bound);
    }

    // Tells whether details about current partition can be accessed.
    // If this returns false, you have to call read_partition_data().
    //
    // Calling read_partition_data() may involve doing I/O. The reason
    // why control over this is exposed and not done under the hood is that
    // in some cases it only makes sense to access partition details from index
    // if it is readily available, and if it is not, we're better off obtaining
    // them by continuing reading from sstable.
    bool partition_data_ready(const index_bound& bound) const {
        return static_cast<bool>(bound.current_list);
    }

    // Valid if partition_data_ready(bound)
    index_entry& current_partition_entry(index_bound& bound) {
        assert(bound.current_list);
        return *bound.current_list->_entries[bound.current_index_idx];
    }

    future<> advance_to_next_partition(index_bound& bound) {
        sstlog.trace("index {} bound {}: advance_to_next_partition()", fmt::ptr(&bound), fmt::ptr(this));
        if (!partition_data_ready(bound)) {
            return advance_to_page(bound, 0).then([this, &bound] {
                return advance_to_next_partition(bound);
            });
        }
        if (bound.current_index_idx + 1 < bound.current_list->size()) {
            ++bound.current_index_idx;
            bound.current_pi_idx = 0;
            bound.data_file_position = bound.current_list->_entries[bound.current_index_idx]->position();
            bound.element = indexable_element::partition;
            bound.end_open_marker.reset();
            return reset_clustered_cursor(bound);
        }
        auto& summary = _sstable->get_summary();
        if (bound.current_summary_idx + 1 < summary.header.size) {
            return advance_to_page(bound, bound.current_summary_idx + 1);
        }
        return advance_to_end(bound);
    }

    future<> advance_to(index_bound& bound, dht::ring_position_view pos) {
        sstlog.trace("index {} bound {}: advance_to({}), _previous_summary_idx={}, _current_summary_idx={}",
            fmt::ptr(this), fmt::ptr(&bound), pos, bound.previous_summary_idx, bound.current_summary_idx);

        if (pos.is_min()) {
            sstlog.trace("index {}: first entry", fmt::ptr(this));
            return make_ready_future<>();
        } else if (pos.is_max()) {
            return advance_to_end(bound);
        }

        auto& summary = _sstable->get_summary();
        bound.previous_summary_idx = std::distance(std::begin(summary.entries),
            std::lower_bound(summary.entries.begin() + bound.previous_summary_idx, summary.entries.end(), pos, index_comparator(*_sstable->_schema)));

        if (bound.previous_summary_idx == 0) {
            sstlog.trace("index {}: first entry", fmt::ptr(this));
            return make_ready_future<>();
        }

        auto summary_idx = bound.previous_summary_idx - 1;

        sstlog.trace("index {}: summary_idx={}", fmt::ptr(this), summary_idx);

        // Despite the requirement that the values of 'pos' in subsequent calls
        // are increasing we still may encounter a situation when we try to read
        // the previous bucket.
        // For example, let's say we have index like this:
        // summary:  A       K       ...
        // index:    A C D F K M N O ...
        // Now, we want to get positions for range [G, J]. We start with [G,
        // summary look up will tel us to check the first bucket. However, there
        // is no G in that bucket so we read the following one to get the
        // position (see the advance_to_page() call below). After we've got it, it's time to
        // get J] position. Again, summary points us to the first bucket and we
        // hit an assert since the reader is already at the second bucket and we
        // cannot go backward.
        // The solution is this condition above. If our lookup requires reading
        // the previous bucket we assume that the entry doesn't exist and return
        // the position of the first one in the current index bucket.
        if (summary_idx + 1 == bound.current_summary_idx) {
            return make_ready_future<>();
        }

        return advance_to_page(bound, summary_idx).then([this, &bound, pos, summary_idx] {
            sstlog.trace("index {}: old page index = {}", fmt::ptr(this), bound.current_index_idx);
            auto i = _alloc_section(_region, [&] {
                auto& entries = bound.current_list->_entries;
                return std::lower_bound(std::begin(entries) + bound.current_index_idx, std::end(entries), pos,
                    index_comparator(*_sstable->_schema));
            });
            // i is valid until next allocation point
            auto& entries = bound.current_list->_entries;
            if (i == std::end(entries)) {
                sstlog.trace("index {}: not found", fmt::ptr(this));
                return advance_to_page(bound, summary_idx + 1);
            }
            bound.current_index_idx = std::distance(std::begin(entries), i);
            bound.current_pi_idx = 0;
            bound.data_file_position = (*i)->position();
            bound.element = indexable_element::partition;
            bound.end_open_marker.reset();
            sstlog.trace("index {}: new page index = {}, pos={}", fmt::ptr(this), bound.current_index_idx, bound.data_file_position);
            return reset_clustered_cursor(bound);
        });
    }

    // Forwards the upper bound cursor to a position which is greater than given position in current partition.
    //
    // Note that the index within partition, unlike the partition index, doesn't cover all keys.
    // So this may not forward to the smallest position which is greater than pos.
    //
    // May advance to the next partition if it's not possible to find a suitable position inside
    // current partition.
    //
    // Must be called only when !eof().
    future<> advance_upper_past(position_in_partition_view pos) {
        sstlog.trace("index {}: advance_upper_past({})", fmt::ptr(this), pos);

        // We advance cursor within the current lower bound partition
        // So need to make sure first that it is read
        if (!partition_data_ready(_lower_bound)) {
            return read_partition_data().then([this, pos] {
                assert(partition_data_ready());
                return advance_upper_past(pos);
            });
        }

        if (!_upper_bound) {
            _upper_bound = _lower_bound;
        }

        index_entry& e = current_partition_entry(*_upper_bound);
        auto e_pos = e.position();
        clustered_index_cursor* cur = current_clustered_cursor(*_upper_bound);

        if (!cur) {
            sstlog.trace("index {}: no promoted index", fmt::ptr(this));
            return advance_to_next_partition(*_upper_bound);
        }

        return cur->probe_upper_bound(pos).then([this, e_pos] (std::optional<clustered_index_cursor::offset_in_partition> off) {
            if (!off) {
                return advance_to_next_partition(*_upper_bound);
            }
            _upper_bound->data_file_position = e_pos + *off;
            _upper_bound->element = indexable_element::cell;
            sstlog.trace("index {} upper bound: skipped to cell, _data_file_position={}", fmt::ptr(this), _upper_bound->data_file_position);
            return make_ready_future<>();
        });
    }

    // Returns position right after all partitions in the sstable
    uint64_t data_file_end() const {
        return _sstable->data_size();
    }

    static future<> close(index_bound& b) noexcept {
        return reset_clustered_cursor(b);
    }
public:
    index_reader(shared_sstable sst, reader_permit permit, const io_priority_class& pc, tracing::trace_state_ptr trace_state,
                 use_caching caching)
        : _sstable(std::move(sst))
        , _permit(std::move(permit))
        , _pc(pc)
        , _trace_state(std::move(trace_state))
        , _local_index_cache(caching ? nullptr
            : std::make_unique<partition_index_cache>(_sstable->manager().get_cache_tracker().get_lru(),
                                                      _sstable->manager().get_cache_tracker().region()))
        , _index_cache(caching ? *_sstable->_index_cache : *_local_index_cache)
        , _region(_sstable->manager().get_cache_tracker().region())
        , _use_caching(caching)
    {
        sstlog.trace("index {}: index_reader for {}", fmt::ptr(this), _sstable->get_filename());
    }

    // Ensures that partition_data_ready() returns true.
    // Can be called only when !eof()
    future<> read_partition_data() {
        assert(!eof());
        if (partition_data_ready(_lower_bound)) {
            return make_ready_future<>();
        }
        // The only case when _current_list may be missing is when the cursor is at the beginning
        assert(_lower_bound.current_summary_idx == 0);
        return advance_to_page(_lower_bound, 0);
    }

    // Advance index_reader bounds to the bounds of the supplied range
    future<> advance_to(const dht::partition_range& range) {
        return seastar::when_all_succeed(
            advance_lower_to_start(range),
            advance_upper_to_end(range)).discard_result();
    }

    // Get current index entry
    // The returned reference is LSA-managed so call with the region locked.
    index_entry& current_partition_entry() {
        return current_partition_entry(_lower_bound);
    }

    file_input_stream_options get_file_input_stream_options(const io_priority_class& pc) {
        file_input_stream_options options;
        options.buffer_size = _sstable->sstable_buffer_size;
        options.read_ahead = 2;
        options.io_priority_class = pc;
        options.dynamic_adjustments = _sstable->_index_history;
        return options;
    }

    // Returns a pointer to the clustered index cursor for the current partition
    // or nullptr if there is no clustered index in the current partition.
    // Returns the same instance until we move to a different partition.
    //
    // Precondition: partition_data_ready(bound).
    //
    // For sstable versions >= mc the returned cursor (if not nullptr) will be of type `bsearch_clustered_cursor`.
    clustered_index_cursor* current_clustered_cursor(index_bound& bound) {
        if (!bound.clustered_cursor) {
            _alloc_section(_region, [&] {
                index_entry& e = current_partition_entry(bound);
                promoted_index* pi = e.get_promoted_index().get();
                if (pi) {
                    bound.clustered_cursor = pi->make_cursor(_sstable, _permit, _trace_state,
                        get_file_input_stream_options(_pc), _use_caching);
                }
            });
            if (!bound.clustered_cursor) {
                return nullptr;
            }
        }
        return &*bound.clustered_cursor;
    }

    clustered_index_cursor* current_clustered_cursor() {
        return current_clustered_cursor(_lower_bound);
    }

    // Returns tombstone for the current partition if it was recorded in the sstable.
    // It may be unavailable for old sstables for which this information was not generated.
    // Can be called only when partition_data_ready().
    std::optional<sstables::deletion_time> partition_tombstone() {
        return current_partition_entry(_lower_bound).get_deletion_time();
    }

    // Returns the key for current partition.
    // Can be called only when partition_data_ready().
    partition_key get_partition_key() {
        return _alloc_section(_region, [this] {
            index_entry& e = current_partition_entry(_lower_bound);
            return e.get_key().to_partition_key(*_sstable->_schema);
        });
    }

    // Returns the data file position for the current partition.
    // Can be called only when partition_data_ready().
    uint64_t get_data_file_position() {
        index_entry& e = current_partition_entry(_lower_bound);
        return e.position();
    }

    // Returns the number of promoted index entries for the current partition.
    // Can be called only when partition_data_ready().
    uint64_t get_promoted_index_size() {
        index_entry& e = current_partition_entry(_lower_bound);
        return e.get_promoted_index_size();
    }

    bool partition_data_ready() const {
        return partition_data_ready(_lower_bound);
    }

    // Forwards the cursor to the given position in the current partition.
    //
    // Note that the index within partition, unlike the partition index, doesn't cover all keys.
    // So this may forward the cursor to some position pos' which precedes pos, even though
    // there exist rows with positions in the range [pos', pos].
    //
    // Must be called for non-decreasing positions.
    // Must be called only after advanced to some partition and !eof().
    future<> advance_to(position_in_partition_view pos) {
        sstlog.trace("index {}: advance_to({}), current data_file_pos={}",
                 fmt::ptr(this), pos, _lower_bound.data_file_position);

        const schema& s = *_sstable->_schema;
        if (pos.is_before_all_fragments(s)) {
            return make_ready_future<>();
        }

        if (!partition_data_ready()) {
            return read_partition_data().then([this, pos] {
                sstlog.trace("index {}: page done", fmt::ptr(this));
                assert(partition_data_ready(_lower_bound));
                return advance_to(pos);
            });
        }

        index_entry& e = current_partition_entry();
        auto e_pos = e.position();
        clustered_index_cursor* cur = current_clustered_cursor(_lower_bound);

        if (!cur) {
            sstlog.trace("index {}: no promoted index", fmt::ptr(this));
            return make_ready_future<>();
        }

        return cur->advance_to(pos).then([this, e_pos] (std::optional<clustered_index_cursor::skip_info> si) {
            if (!si) {
                sstlog.trace("index {}: position in the same block", fmt::ptr(this));
                return;
            }
            if (!si->active_tombstone) {
                // End open marker can be only engaged in SSTables 3.x ('mc' format) and never in ka/la
                _lower_bound.end_open_marker.reset();
            } else {
                _lower_bound.end_open_marker = open_rt_marker{std::move(si->active_tombstone_pos), si->active_tombstone};
            }
            _lower_bound.data_file_position = e_pos + si->offset;
            _lower_bound.element = indexable_element::cell;
            sstlog.trace("index {}: skipped to cell, _data_file_position={}", fmt::ptr(this), _lower_bound.data_file_position);
        });
    }

    // Like advance_to(dht::ring_position_view), but returns information whether the key was found
    // If upper_bound is provided, the upper bound within position is looked up
    future<bool> advance_lower_and_check_if_present(
            dht::ring_position_view key, std::optional<position_in_partition_view> pos = {}) {
        return advance_to(_lower_bound, key).then([this, key, pos] {
            if (eof()) {
                return make_ready_future<bool>(false);
            }
            return read_partition_data().then([this, key, pos] {
                index_comparator cmp(*_sstable->_schema);
                bool found = _alloc_section(_region, [&] {
                    return cmp(key, current_partition_entry(_lower_bound)) == 0;
                });
                if (!found || !pos) {
                    return make_ready_future<bool>(found);
                }

                return advance_upper_past(*pos).then([] {
                    return make_ready_future<bool>(true);
                });
            });
        });
    }

    // Advances the upper bound to the partition immediately following the partition of the lower bound.
    //
    // Precondition: the sstable version is >= mc.
    future<> advance_reverse_to_next_partition() {
        return advance_reverse(position_in_partition_view::after_all_clustered_rows());
    }

    // Advances the upper bound to the start of the first promoted index block after `pos`,
    // or to the next partition if there are no blocks after `pos`.
    //
    // Supports advancing backwards (i.e. `pos` can be smaller than the previous upper bound position).
    //
    // Precondition: the sstable version is >= mc.
    future<> advance_reverse(position_in_partition_view pos) {
        if (eof()) {
            return make_ready_future<>();
        }

        // The `clustered_cursor` of an index bound does not support moving backward;
        // we work around this by recreating the upper bound (if it already exists)
        // at the lower bound position, then moving forward.
        if (_upper_bound) {
            return close(*_upper_bound).then([this, pos] {
                _upper_bound.reset();
                return advance_reverse(pos);
            });
        }

        // We advance the clustered cursor within the current lower bound partition
        // so need to make sure first that the lower bound partition data is in memory.
        if (!partition_data_ready(_lower_bound)) {
            return read_partition_data().then([this, pos] {
                assert(partition_data_ready());
                return advance_reverse(pos);
            });
        }

        _upper_bound = _lower_bound;

        auto cur = current_clustered_cursor(*_upper_bound);
        if (!cur) {
            sstlog.trace("index {}: no promoted index", fmt::ptr(this));
            return advance_to_next_partition(*_upper_bound);
        }

        auto cur_bsearch = dynamic_cast<sstables::mc::bsearch_clustered_cursor*>(cur);
        // The dynamic cast must have succeeded by precondition (sstable version >= mc)
        // and `current_clustered_cursor` specification.
        if (!cur_bsearch) {
            on_internal_error(sstlog, format(
                "index {}: expected the cursor type to be bsearch_clustered_cursor, but it's not;"
                " sstable version (expected >= mc): {}", fmt::ptr(this), _sstable->get_version()));
        }

        index_entry& e = current_partition_entry(*_upper_bound);
        return cur_bsearch->advance_past(pos).then([this, partition_start_pos = get_data_file_position()]
                (std::optional<clustered_index_cursor::skip_info> si) {
            if (!si) {
                return advance_to_next_partition(*_upper_bound);
            }
            if (!si->active_tombstone) {
                // End open marker can be only engaged in SSTables 3.x ('mc' format) and never in ka/la
                _upper_bound->end_open_marker.reset();
            } else {
                _upper_bound->end_open_marker = open_rt_marker{std::move(si->active_tombstone_pos), si->active_tombstone};
            }
            _upper_bound->data_file_position = partition_start_pos + si->offset;
            _upper_bound->element = indexable_element::cell;
            sstlog.trace("index {}: advanced end after cell, _data_file_position={}", fmt::ptr(this), _upper_bound->data_file_position);
            return make_ready_future<>();
        });
    }

    // Returns the offset in the data file of the first row in the last promoted index block
    // in the current partition or nullopt if there are no blocks in the current partition.
    //
    // Preconditions: sstable version >= mc, partition_data_ready().
    future<std::optional<uint64_t>> last_block_offset() {
        assert(partition_data_ready());

        auto cur = current_clustered_cursor();
        if (!cur) {
            return make_ready_future<std::optional<uint64_t>>(std::nullopt);
        }

        auto cur_bsearch = dynamic_cast<sstables::mc::bsearch_clustered_cursor*>(cur);
        // The dynamic cast must have succeeded by precondition (sstable version >= mc)
        // and `current_clustered_cursor` specification.
        if (!cur_bsearch) {
            on_internal_error(sstlog, format(
                "index {}: expected the cursor type to be bsearch_clustered_cursor, but it's not;"
                " sstable version (expected >= mc): {}", fmt::ptr(this), _sstable->get_version()));
        }

        return cur_bsearch->last_block_offset();
    }

    // Moves the cursor to the beginning of next partition.
    // Can be called only when !eof().
    future<> advance_to_next_partition() {
        return advance_to_next_partition(_lower_bound);
    }

    // Positions the cursor on the first partition which is not smaller than pos (like std::lower_bound).
    // Must be called for non-decreasing positions.
    future<> advance_to(dht::ring_position_view pos) {
        return advance_to(_lower_bound, pos);
    }

    struct data_file_positions_range {
        uint64_t start;
        std::optional<uint64_t> end;
    };

    // Returns positions in the data file of the cursor.
    // End position may be unset
    data_file_positions_range data_file_positions() const {
        data_file_positions_range result;
        result.start = _lower_bound.data_file_position;
        if (_upper_bound) {
            result.end = _upper_bound->data_file_position;
        }
        return result;
    }

    // Returns the kind of sstable element the cursor is pointing at.
    indexable_element element_kind() const {
        return _lower_bound.element;
    }

    std::optional<open_rt_marker> end_open_marker() const {
        return _lower_bound.end_open_marker;
    }

    std::optional<open_rt_marker> reverse_end_open_marker() const {
        return _upper_bound->end_open_marker;
    }

    bool eof() const {
        return _lower_bound.data_file_position == data_file_end();
    }

    const shared_sstable& sstable() const { return _sstable; }

    future<> close() noexcept {
        // index_bound::close must not fail
        return close(_lower_bound).then([this] {
            if (_upper_bound) {
                return close(*_upper_bound);
            }
            if (_local_index_cache) {
                return _local_index_cache->evict_gently();
            }
            return make_ready_future<>();
        });
    }
};

}

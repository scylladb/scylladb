/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "sstables.hh"
#include "consumer.hh"
#include "exceptions.hh"
#include "sstables/partition_index_cache.hh"
#include <seastar/util/bool_class.hh>
#include "tracing/traced_file.hh"
#include "sstables/scanning_clustered_index_cursor.hh"
#include "sstables/mx/bsearch_clustered_cursor.hh"
#include "sstables/sstables_manager.hh"

namespace sstables {

extern seastar::logger sstlog;
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
public:
    index_list indexes;

    index_consumer(logalloc::region& r, schema_ptr s)
        : _s(std::move(s))
        , _region(r)
    { }

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
                auto key = managed_bytes(reinterpret_cast<const bytes::value_type*>(e.key.get()), e.key.size());
                indexes._entries.emplace_back(make_managed<index_entry>(std::move(key), e.data_file_offset, std::move(pi)));
            });
        });
    }

    void prepare(uint64_t size) {
        _alloc_section = logalloc::allocating_section();
        _alloc_section(_region, [&] {
            with_allocator(_region.allocator(), [&] {
                indexes._entries.reserve(size);
            });
        });
    }
};

// See #2993
class trust_promoted_index_tag;
using trust_promoted_index = bool_class<trust_promoted_index_tag>;

enum class index_consume_entry_context_state {
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
};

inline std::string_view format_as(index_consume_entry_context_state s) {
    using enum index_consume_entry_context_state;
    switch (s) {
    case START: return "START";
    case KEY_SIZE: return "KEY_SIZE";
    case KEY_BYTES: return "KEY_BYTES";
    case POSITION: return "POSITION";
    case PROMOTED_SIZE: return "PROMOTED_SIZE";
    case PARTITION_HEADER_LENGTH_1: return "PARTITION_HEADER_LENGTH_1";
    case PARTITION_HEADER_LENGTH_2: return "PARTITION_HEADER_LENGTH_2";
    case LOCAL_DELETION_TIME: return "LOCAL_DELETION_TIME";
    case MARKED_FOR_DELETE_AT: return "MARKED_FOR_DELETE_AT";
    case NUM_PROMOTED_INDEX_BLOCKS: return "NUM_PROMOTED_INDEX_BLOCKS";
    case CONSUME_ENTRY: return "CONSUME_ENTRY";
    }
    abort();
}

} // namespace sstables

#if FMT_VERSION < 10'00'00
template <>
struct fmt::formatter<sstables::index_consume_entry_context_state> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(sstables::index_consume_entry_context_state s, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", format_as(s));
    }
};
#endif

namespace sstables {

// TODO: make it templated on SSTables version since the exact format can be passed in at compile time
template <class IndexConsumer>
requires PartitionIndexConsumer<IndexConsumer>
class index_consume_entry_context : public data_consumer::continuous_data_consumer<index_consume_entry_context<IndexConsumer>> {
    using proceed = data_consumer::proceed;
    using processing_result = data_consumer::processing_result;
    using continuous_data_consumer = data_consumer::continuous_data_consumer<index_consume_entry_context<IndexConsumer>>;
    using read_status = typename continuous_data_consumer::read_status;
    using state = index_consume_entry_context_state;
private:
    const sstable& _sst;
    IndexConsumer& _consumer;
    uint64_t _entry_offset;

    state _state = state::START;

    temporary_buffer<char> _key;
    uint64_t _promoted_index_end;
    uint64_t _position;
    uint64_t _partition_header_length = 0;
    std::optional<deletion_time> _deletion_time;

    trust_promoted_index _trust_pi;
    std::optional<column_values_fixed_lengths> _ck_values_fixed_lengths;
    tracing::trace_state_ptr _trace_state;
    const abort_source& _abort;

    inline bool is_mc_format() const { return static_cast<bool>(_ck_values_fixed_lengths); }

public:
    void verify_end_state() const {
        if (this->_remain > 0) {
            throw malformed_sstable_exception(fmt::format("index_consume_entry_context (state={}): parsing ended but there is unconsumed data", _state), _sst.index_filename());
        }
        if (_state != state::KEY_SIZE && _state != state::START) {
            throw malformed_sstable_exception(fmt::format("index_consume_entry_context (state={}): cannot finish parsing current entry, no more data", _state), _sst.index_filename());
        }
    }

    bool non_consuming() const {
        return ((_state == state::CONSUME_ENTRY) || (_state == state::START));
    }

    processing_result process_state(temporary_buffer<char>& data) {
        _abort.check();

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
            sstlog.trace("{}: pos {} state {} - data.size()={}", fmt::ptr(this), current_pos(), state::START, data.size());
            _state = state::KEY_SIZE;
            break;
        case state::KEY_SIZE:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::KEY_SIZE);
            _entry_offset = current_pos();
            if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                _state = state::KEY_BYTES;
                break;
            }
            [[fallthrough]];
        case state::KEY_BYTES:
            sstlog.trace("{}: pos {} state {} - size={}", fmt::ptr(this), current_pos(), state::KEY_BYTES, this->_u16);
            if (this->read_bytes_contiguous(data, this->_u16, _key) != continuous_data_consumer::read_status::ready) {
                _state = state::POSITION;
                break;
            }
            [[fallthrough]];
        case state::POSITION:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::POSITION);
            if (read_vint_or_uint64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::PROMOTED_SIZE;
                break;
            }
            [[fallthrough]];
        case state::PROMOTED_SIZE:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::PROMOTED_SIZE);
            _position = this->_u64;
            if (read_vint_or_uint32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::PARTITION_HEADER_LENGTH_1;
                break;
            }
            [[fallthrough]];
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
            [[fallthrough]];
        case state::PARTITION_HEADER_LENGTH_2:
            sstlog.trace("{}: pos {} state {} {}", fmt::ptr(this), current_pos(), state::PARTITION_HEADER_LENGTH_2, this->_u64);
            _partition_header_length = this->_u64;
        state_LOCAL_DELETION_TIME:
            [[fallthrough]];
        case state::LOCAL_DELETION_TIME:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::LOCAL_DELETION_TIME);
            _deletion_time.emplace();
            if (this->read_32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::MARKED_FOR_DELETE_AT;
                break;
            }
            [[fallthrough]];
        case state::MARKED_FOR_DELETE_AT:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::MARKED_FOR_DELETE_AT);
            _deletion_time->local_deletion_time = this->_u32;
            if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::NUM_PROMOTED_INDEX_BLOCKS;
                break;
            }
            [[fallthrough]];
        case state::NUM_PROMOTED_INDEX_BLOCKS:
            sstlog.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::NUM_PROMOTED_INDEX_BLOCKS);
            _deletion_time->marked_for_delete_at = this->_u64;
            if (read_vint_or_uint32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::CONSUME_ENTRY;
                break;
            }
        state_CONSUME_ENTRY:
            [[fallthrough]];
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
            input_stream<char>&& input, uint64_t start, uint64_t maxlen,
            std::optional<column_values_fixed_lengths> ck_values_fixed_lengths,
            const abort_source& abort, tracing::trace_state_ptr trace_state = {})
        : continuous_data_consumer(std::move(permit), std::move(input), start, maxlen)
        , _sst(sst), _consumer(consumer), _entry_offset(start), _trust_pi(trust_pi)
        , _ck_values_fixed_lengths(std::move(ck_values_fixed_lengths))
        , _trace_state(std::move(trace_state))
        , _abort(abort)
    {}
};

inline file make_tracked_index_file(sstable& sst, reader_permit permit, tracing::trace_state_ptr trace_state,
                                    use_caching caching) {
    auto f = caching ? sst.index_file() : sst.uncached_index_file();
    f = make_tracked_file(std::move(f), std::move(permit));
    if (!trace_state) {
        return f;
    }
    return tracing::make_traced_file(std::move(f), std::move(trace_state), format("{}:", sst.index_filename()));
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
                                                    sst->manager().get_cache_tracker().get_index_cached_file_stats(),
                                                    sst->manager().get_cache_tracker().get_lru(),
                                                    sst->manager().get_cache_tracker().region(),
                                                    sst->_index_file_size);
        return std::make_unique<mc::bsearch_clustered_cursor>(*sst->get_schema(),
            _promoted_index_start, _promoted_index_size,
            promoted_index_cache_metrics, permit,
            *ck_values_fixed_lengths, cached_file_ptr, _num_blocks, trace_state);
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

    std::unique_ptr<index_consumer> consumer;
    std::unique_ptr<index_consume_entry_context<index_consumer>> context;
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

struct data_file_positions_range {
    uint64_t start;
    std::optional<uint64_t> end;
};

class index_reader {
public:
    virtual ~index_reader() {};
    virtual future<> close() noexcept = 0;
    virtual data_file_positions_range data_file_positions() const = 0;
    virtual future<std::optional<uint64_t>> last_block_offset() = 0;
    virtual future<bool> advance_lower_and_check_if_present(
            dht::ring_position_view key, std::optional<position_in_partition_view> pos = {}) = 0;
    virtual future<> advance_to_next_partition() = 0;
    virtual indexable_element element_kind() const = 0;
    virtual future<> advance_to(dht::ring_position_view pos) = 0;
    virtual future<> advance_after_existing(const dht::decorated_key& dk) = 0;
    virtual future<> advance_to(position_in_partition_view pos) = 0;
    virtual std::optional<sstables::deletion_time> partition_tombstone() = 0;
    virtual std::optional<partition_key> get_partition_key() = 0;
    virtual partition_key get_partition_key_prefix() = 0;
    virtual bool partition_data_ready() const = 0;
    virtual future<> read_partition_data() = 0;
    virtual future<> advance_reverse(position_in_partition_view pos) = 0;
    virtual future<> advance_to(const dht::partition_range& range) = 0;
    virtual future<> advance_reverse_to_next_partition() = 0;

    virtual std::optional<open_rt_marker> end_open_marker() const = 0;
    virtual std::optional<open_rt_marker> reverse_end_open_marker() const = 0;
    virtual clustered_index_cursor* current_clustered_cursor() = 0;
    virtual uint64_t get_data_file_position() = 0;

    virtual uint64_t get_promoted_index_size() = 0;
    virtual bool eof() const = 0;
};

std::unique_ptr<index_reader> make_index_reader(shared_sstable sst, reader_permit permit,
                 tracing::trace_state_ptr trace_state = {},
                 use_caching caching = use_caching::yes,
                 bool single_partition_read = false,
                 bool force_no_trie = false);

}

/*
 * Copyright (C) 2015 ScyllaDB
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
#include "mutation.hh"
#include "sstables.hh"
#include "types.hh"
#include "core/future-util.hh"
#include "key.hh"
#include "keys.hh"
#include "core/do_with.hh"
#include "unimplemented.hh"
#include "utils/move.hh"
#include "dht/i_partitioner.hh"
#include <seastar/core/byteorder.hh>
#include "index_reader.hh"
#include "counters.hh"
#include "utils/data_input.hh"
#include "clustering_ranges_walker.hh"

namespace sstables {

/**
 * @returns: >= 0, if key is found. That is the index where the key is found.
 *             -1, if key is not found, and is smaller than the first key in the list.
 *          <= -2, if key is not found, but is greater than one of the keys. By adding 2 and
 *                 negating, one can determine the index before which the key would have to
 *                 be inserted.
 *
 * Origin uses this slightly modified binary search for the Summary, that will
 * indicate in which bucket the element would be in case it is not a match.
 *
 * For the Index entries, it uses a "normal", java.lang binary search. Because
 * we have made the explicit decision to open code the comparator for
 * efficiency, using a separate binary search would be possible, but very
 * messy.
 *
 * It's easier to reuse the same code for both binary searches, and just ignore
 * the extra information when not needed.
 *
 * This code should work in all kinds of vectors in whose's elements is possible to aquire
 * a key view via get_key().
 */
template <typename T>
int sstable::binary_search(const T& entries, const key& sk, const dht::token& token) {
    int low = 0, mid = entries.size(), high = mid - 1, result = -1;

    auto& partitioner = dht::global_partitioner();

    while (low <= high) {
        // The token comparison should yield the right result most of the time.
        // So we avoid expensive copying operations that happens at key
        // creation by keeping only a key view, and then manually carrying out
        // both parts of the comparison ourselves.
        mid = low + ((high - low) >> 1);
        key_view mid_key = entries[mid].get_key();
        auto mid_token = partitioner.get_token(mid_key);

        if (token == mid_token) {
            result = sk.tri_compare(mid_key);
        } else {
            result = token < mid_token ? -1 : 1;
        }

        if (result > 0) {
            low = mid + 1;
        } else if (result < 0) {
            high = mid - 1;
        } else {
            return mid;
        }
    }

    return -mid - (result < 0 ? 1 : 2);
}

// Force generation, so we make it available outside this compilation unit without moving that
// much code to .hh
template int sstable::binary_search<>(const std::vector<summary_entry>& entries, const key& sk);
template int sstable::binary_search<>(const std::vector<index_entry>& entries, const key& sk);

static inline bytes pop_back(std::vector<bytes>& vec) {
    auto b = std::move(vec.back());
    vec.pop_back();
    return std::move(b);
}

class sstable_streamed_mutation;

class mp_row_consumer : public row_consumer {
public:
    struct new_mutation {
        partition_key key;
        tombstone tomb;
    };
private:
    schema_ptr _schema;
    key_view _key;
    const io_priority_class& _pc;
    const query::partition_slice& _slice;
    bool _out_of_range = false;
    stdx::optional<query::clustering_key_filter_ranges> _ck_ranges;
    stdx::optional<clustering_ranges_walker> _ck_ranges_walker;
    sstable_streamed_mutation* _sm;

    bool _skip_partition = false;
    // When set, the fragment pending in _in_progress should not be emitted.
    bool _skip_in_progress = false;

    // The value of _ck_ranges->lower_bound_counter() last time we tried to skip to _ck_ranges->lower_bound().
    size_t _last_lower_bound_counter = 0;

    // We don't have "end of clustering row" markers. So we know that the current
    // row has ended once we get something (e.g. a live cell) that belongs to another
    // one. If that happens sstable reader is interrupted (proceed::no) but we
    // already have the whole row that just ended and a part of the new row.
    // The finished row is moved to _ready so that upper layer can retrieve it and
    // the part of the new row goes to _in_progress and this is were we will continue
    // accumulating data once sstable reader is continued.
    //
    // _ready only holds fragments which are in the query range, but _in_progress
    // not necessarily.
    //
    // _in_progress may be disengaged only before reading first fragment of partition
    // or after all fragments of partition were consumed. Fast-forwarding within partition
    // should not clear it, we rely on it being set to detect repeated tombstones.
    mutation_fragment_opt _in_progress;
    mutation_fragment_opt _ready;

    stdx::optional<new_mutation> _mutation;
    bool _is_mutation_end = true;
    position_in_partition _fwd_end = position_in_partition::after_all_clustered_rows(); // Restricts the stream on top of _ck_ranges_walker.
    streamed_mutation::forwarding _fwd;

    // Because of #1203 we may encounter sstables with range tombstones
    // placed earlier than expected. We fix the ordering by loading range tombstones
    // initially into _range_tombstones, until first row is encountered,
    // and then merge the two streams in push_ready_fragments().
    //
    // _range_tombstones holds only tombstones which are relevant for current ranges.
    range_tombstone_stream _range_tombstones;
    bool _first_row_encountered = false;
public:
    void set_streamed_mutation(sstable_streamed_mutation* sm) {
        _sm = sm;
    }
    struct column {
        bool is_static;
        bytes_view col_name;
        std::vector<bytes> clustering;
        // see is_collection. collections have an extra element aside from the name.
        // This will be non-zero size if this is a collection, and zero size othersize.
        bytes collection_extra_data;
        bytes cell;
        const column_definition *cdef;
        bool is_present;

        static constexpr size_t static_size = 2;

        // For every normal column, we expect the clustering key, followed by the
        // extra element for the column name.
        //
        // For a collection, some auxiliary data will be embedded into the
        // column_name as seen by the row consumer. This means that if our
        // exploded clustering keys has more rows than expected, we are dealing
        // with a collection.
        bool is_collection(const schema& s) {
            auto expected_normal = s.clustering_key_size() + 1;
            // Note that we can have less than the expected. That is the case for
            // incomplete prefixes, for instance.
            if (clustering.size() <= expected_normal) {
                return false;
            } else if (clustering.size() == (expected_normal + 1)) {
                return true;
            }
            throw malformed_sstable_exception(sprint("Found %d clustering elements in column name. Was not expecting that!", clustering.size()));
        }

        static bool check_static(const schema& schema, bytes_view col) {
            return composite_view(col, schema.is_compound()).is_static();
        }

        static bytes_view fix_static_name(const schema& schema, bytes_view col) {
            return fix_static_name(col, check_static(schema, col));
        }

        static bytes_view fix_static_name(bytes_view col, bool is_static) {
            if(is_static) {
                col.remove_prefix(static_size);
            }
            return col;
        }

        std::vector<bytes> extract_clustering_key(const schema& schema) {
            return composite_view(col_name, schema.is_compound()).explode();
        }
        column(const schema& schema, bytes_view col, api::timestamp_type timestamp)
            : is_static(check_static(schema, col))
            , col_name(fix_static_name(col, is_static))
            , clustering(extract_clustering_key(schema))
            , collection_extra_data(is_collection(schema) ? pop_back(clustering) : bytes()) // collections are not supported with COMPACT STORAGE, so this is fine
            , cell(!schema.is_dense() ? pop_back(clustering) : (*(schema.regular_begin())).name()) // dense: cell name is not provided. It is the only regular column
            , cdef(schema.get_column_definition(cell))
            , is_present(cdef && timestamp > cdef->dropped_at())
        {

            if (is_static) {
                for (auto& e: clustering) {
                    if (e.size() != 0) {
                        throw malformed_sstable_exception("Static row has clustering key information. I didn't expect that!");
                    }
                }
            }

            if (is_present && is_static != cdef->is_static()) {
                throw malformed_sstable_exception(seastar::format("Mismatch between {} cell and {} column definition",
                        is_static ? "static" : "non-static", cdef->is_static() ? "static" : "non-static"));
            }
        }
    };

private:
    // Notes for collection mutation:
    //
    // While we could in theory generate the mutation for the elements as they
    // appear, that would be costly.  We would need to keep deserializing and
    // serializing them, either explicitly or through a merge.
    //
    // The best way forward is to accumulate the collection data into a data
    // structure, and later on serialize it fully when this (sstable) row ends.
    class collection_mutation {
        const column_definition *_cdef;
    public:
        collection_type_impl::mutation cm;

        // We need to get a copy of the prefix here, because the outer object may be short lived.
        collection_mutation(const column_definition *cdef)
            : _cdef(cdef) { }

        collection_mutation() : _cdef(nullptr) {}

        bool is_new_collection(const column_definition *c) {
            if (!_cdef || ((_cdef->id != c->id) || (_cdef->kind != c->kind))) {
                return true;
            }
            return false;
        };

        void flush(const schema& s, mutation_fragment& mf) {
            if (!_cdef) {
                return;
            }
            auto ctype = static_pointer_cast<const collection_type_impl>(_cdef->type);
            auto ac = atomic_cell_or_collection::from_collection_mutation(ctype->serialize_mutation_form(cm));
            if (_cdef->is_static()) {
                mf.as_mutable_static_row().set_cell(*_cdef, std::move(ac));
            } else {
                mf.as_mutable_clustering_row().set_cell(*_cdef, std::move(ac));
            }
        }
    };
    std::experimental::optional<collection_mutation> _pending_collection = {};

    collection_mutation& pending_collection(const column_definition *cdef) {
        if (!_pending_collection || _pending_collection->is_new_collection(cdef)) {
            flush_pending_collection(*_schema);

            if (!cdef->type->is_multi_cell()) {
                throw malformed_sstable_exception("frozen set should behave like a cell\n");
            }
            _pending_collection = collection_mutation(cdef);
        }
        return *_pending_collection;
    }

    proceed push_ready_fragments_out_of_range();
    proceed push_ready_fragments_with_ready_set();

    void update_pending_collection(const column_definition *cdef, bytes&& col, atomic_cell&& ac) {
        pending_collection(cdef).cm.cells.emplace_back(std::move(col), std::move(ac));
    }

    void update_pending_collection(const column_definition *cdef, tombstone&& t) {
        pending_collection(cdef).cm.tomb = std::move(t);
    }

    void flush_pending_collection(const schema& s) {
        if (_pending_collection) {
            _pending_collection->flush(s, *_in_progress);
            _pending_collection = {};
        }
    }

    // Returns true if and only if the position is inside requested ranges.
    // Assumes that this and the other advance_to() are called with monotonic positions.
    // We rely on the fact that the first 'S' in SSTables stands for 'sorted'
    // and the clustering row keys are always in an ascending order.
    void advance_to(position_in_partition_view pos) {
        position_in_partition::less_compare less(*_schema);

        if (!less(pos, _fwd_end)) {
            _out_of_range = true;
            _skip_in_progress = false;
        } else {
            _skip_in_progress = !_ck_ranges_walker->advance_to(pos);
            _out_of_range |= _ck_ranges_walker->out_of_range();
        }

        sstlog.trace("mp_row_consumer {}: advance_to({}) => out_of_range={}, skip_in_progress={}", this, pos, _out_of_range, _skip_in_progress);
    }

    // Assumes that this and other advance_to() overloads are called with monotonic positions.
    void advance_to(const range_tombstone& rt) {
        position_in_partition::less_compare less(*_schema);
        auto&& start = rt.position();
        auto&& end = rt.end_position();

        if (!less(start, _fwd_end)) {
            _out_of_range = true;
            _skip_in_progress = false; // It may become in range after next forwarding, so cannot drop it
        } else {
            _skip_in_progress = !_ck_ranges_walker->advance_to(start, end);
            _out_of_range |= _ck_ranges_walker->out_of_range();
        }

        sstlog.trace("mp_row_consumer {}: advance_to({}) => out_of_range={}, skip_in_progress={}", this, rt, _out_of_range, _skip_in_progress);
    }

    void advance_to(const mutation_fragment& mf) {
        if (mf.is_range_tombstone()) {
            advance_to(mf.as_range_tombstone());
        } else {
            advance_to(mf.position());
        }
    }

    void set_up_ck_ranges(const partition_key& pk) {
        sstlog.trace("mp_row_consumer {}: set_up_ck_ranges({})", this, pk);
        _ck_ranges = query::clustering_key_filter_ranges::get_ranges(*_schema, _slice, pk);
        _ck_ranges_walker = clustering_ranges_walker(*_schema, _ck_ranges->ranges(), _schema->has_static_columns());
        _last_lower_bound_counter = 0;
        _fwd_end = _fwd ? position_in_partition::before_all_clustered_rows() : position_in_partition::after_all_clustered_rows();
        _out_of_range = false;
        _range_tombstones.reset();
        _first_row_encountered = false;
    }
public:
    mutation_opt mut;

    mp_row_consumer(const key& key,
                    const schema_ptr schema,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    streamed_mutation::forwarding fwd)
            : _schema(schema)
            , _key(key_view(key))
            , _pc(pc)
            , _slice(slice)
            , _fwd(fwd)
            , _range_tombstones(*_schema)
    {
        set_up_ck_ranges(partition_key::from_exploded(*_schema, key.explode(*_schema)));
    }

    mp_row_consumer(const key& key,
                    const schema_ptr schema,
                    const io_priority_class& pc,
                    streamed_mutation::forwarding fwd)
            : mp_row_consumer(key, schema, query::full_slice, pc, fwd) { }

    mp_row_consumer(const schema_ptr schema,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    streamed_mutation::forwarding fwd)
            : _schema(schema)
            , _pc(pc)
            , _slice(slice)
            , _fwd(fwd)
            , _range_tombstones(*_schema)
    { }

    mp_row_consumer(const schema_ptr schema,
                    const io_priority_class& pc,
                    streamed_mutation::forwarding fwd)
            : mp_row_consumer(schema, query::full_slice, pc, fwd) { }

    virtual proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        if (_key.empty() || key == _key) {
            _mutation = new_mutation { partition_key::from_exploded(key.explode(*_schema)), tombstone(deltime) };
            _is_mutation_end = false;
            _skip_partition = false;
            _skip_in_progress = false;
            set_up_ck_ranges(_mutation->key);
            return proceed::no;
        } else {
            throw malformed_sstable_exception(sprint("Key mismatch. Got %s while processing %s", to_hex(bytes_view(key)).c_str(), to_hex(bytes_view(_key)).c_str()));
        }
    }

    proceed flush() {
        sstlog.trace("mp_row_consumer {}: flush(in_progress={}, ready={}, skip={})", this, _in_progress, _ready, _skip_in_progress);
        flush_pending_collection(*_schema);
        // If _ready is already set we have a bug: get_mutation_fragment()
        // was not called, and below we will lose one clustering row!
        assert(!_ready);
        if (!_skip_in_progress) {
            _ready = move_and_disengage(_in_progress);
            return push_ready_fragments_with_ready_set();
        } else {
            _in_progress = { };
            _ready = { };
            _skip_in_progress = false;
            return proceed::yes;
        }
    }

    proceed flush_if_needed(range_tombstone&& rt) {
        sstlog.trace("mp_row_consumer {}: flush_if_needed(in_progress={}, ready={}, skip={})", this, _in_progress, _ready, _skip_in_progress);
        proceed ret = proceed::yes;
        if (_in_progress) {
            ret = flush();
        }
        advance_to(rt);
        _in_progress = mutation_fragment(std::move(rt));
        if (_out_of_range) {
            ret = push_ready_fragments_out_of_range();
        }
        if (needs_skip()) {
            ret = proceed::no;
        }
        return ret;
    }

    proceed flush_if_needed(bool is_static, position_in_partition&& pos) {
        sstlog.trace("mp_row_consumer {}: flush_if_needed({})", this, pos);

        // Part of workaround for #1203
        _first_row_encountered = !is_static;

        position_in_partition::equal_compare eq(*_schema);
        proceed ret = proceed::yes;
        if (_in_progress && !eq(_in_progress->position(), pos)) {
            ret = flush();
        }
        if (!_in_progress) {
            advance_to(pos);
            if (is_static) {
                _in_progress = mutation_fragment(static_row());
            } else {
                _in_progress = mutation_fragment(clustering_row(std::move(pos.key())));
            }
            if (_out_of_range) {
                ret = push_ready_fragments_out_of_range();
            }
            if (needs_skip()) {
                ret = proceed::no;
            }
        }
        return ret;
    }

    proceed flush_if_needed(bool is_static, const exploded_clustering_prefix& ecp) {
        auto pos = [&] {
            if (is_static) {
                return position_in_partition(position_in_partition::static_row_tag_t());
            } else {
                auto ck = clustering_key_prefix::from_clustering_prefix(*_schema, ecp);
                return position_in_partition(position_in_partition::clustering_row_tag_t(), std::move(ck));
            }
        }();
        return flush_if_needed(is_static, std::move(pos));
    }

    proceed flush_if_needed(clustering_key_prefix&& ck) {
        return flush_if_needed(false, position_in_partition(position_in_partition::clustering_row_tag_t(), std::move(ck)));
    }

    atomic_cell make_counter_cell(int64_t timestamp, bytes_view value) {
        static constexpr size_t shard_size = 32;

        data_input in(value);

        auto header_size = in.read<int16_t>();
        for (auto i = 0; i < header_size; i++) {
            auto idx = in.read<int16_t>();
            if (idx >= 0) {
                throw marshal_exception("encountered a local shard in a counter cell");
            }
        }
        auto shard_count = value.size() / shard_size;
        if (shard_count != size_t(header_size)) {
            throw marshal_exception("encountered remote shards in a counter cell");
        }

        std::vector<counter_shard> shards;
        shards.reserve(shard_count);
        counter_cell_builder ccb(shard_count);
        for (auto i = 0u; i < shard_count; i++) {
            auto id_hi = in.read<int64_t>();
            auto id_lo = in.read<int64_t>();
            auto clock = in.read<int64_t>();
            auto value = in.read<int64_t>();
            ccb.add_shard(counter_shard(counter_id(utils::UUID(id_hi, id_lo)), value, clock));
        }
        return ccb.build(timestamp);
    }

    template<typename CreateCell>
    //requires requires(CreateCell create_cell, column col) {
    //    { create_cell(col) } -> void;
    //}
    proceed do_consume_cell(bytes_view col_name, int64_t timestamp, int32_t ttl, int32_t expiration, CreateCell&& create_cell) {
        if (_skip_partition) {
            return proceed::yes;
        }

        struct column col(*_schema, col_name, timestamp);

        auto clustering_prefix = exploded_clustering_prefix(std::move(col.clustering));
        auto ret = flush_if_needed(col.is_static, clustering_prefix);
        if (_skip_in_progress) {
            return ret;
        }

        if (col.cell.size() == 0) {
            row_marker rm(timestamp, gc_clock::duration(ttl), gc_clock::time_point(gc_clock::duration(expiration)));
            _in_progress->as_mutable_clustering_row().apply(std::move(rm));
            return ret;
        }

        if (!col.is_present) {
            return ret;
        }

        create_cell(std::move(col));
        return ret;
    }

    virtual proceed consume_counter_cell(bytes_view col_name, bytes_view value, int64_t timestamp) override {
        return do_consume_cell(col_name, timestamp, 0, 0, [&] (auto&& col) {
            auto ac = make_counter_cell(timestamp, value);

            if (col.is_static) {
                _in_progress->as_mutable_static_row().set_cell(*(col.cdef), std::move(ac));
            } else {
                _in_progress->as_mutable_clustering_row().set_cell(*(col.cdef), atomic_cell_or_collection(std::move(ac)));
            }
        });
    }

    atomic_cell make_atomic_cell(uint64_t timestamp, bytes_view value, uint32_t ttl, uint32_t expiration) {
        if (ttl) {
            return atomic_cell::make_live(timestamp, value,
                gc_clock::time_point(gc_clock::duration(expiration)), gc_clock::duration(ttl));
        } else {
            return atomic_cell::make_live(timestamp, value);
        }
    }

    virtual proceed consume_cell(bytes_view col_name, bytes_view value, int64_t timestamp, int32_t ttl, int32_t expiration) override {
        return do_consume_cell(col_name, timestamp, ttl, expiration, [&] (auto&& col) {
            auto ac = make_atomic_cell(timestamp, value, ttl, expiration);

            bool is_multi_cell = col.collection_extra_data.size();
            if (is_multi_cell != col.cdef->type->is_multi_cell()) {
                return;
            }
            if (is_multi_cell) {
                update_pending_collection(col.cdef, std::move(col.collection_extra_data), std::move(ac));
                return;
            }

            if (col.is_static) {
                _in_progress->as_mutable_static_row().set_cell(*(col.cdef), std::move(ac));
                return;
            }
            _in_progress->as_mutable_clustering_row().set_cell(*(col.cdef), atomic_cell_or_collection(std::move(ac)));
        });
    }

    virtual proceed consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        if (_skip_partition) {
            return proceed::yes;
        }

        auto timestamp = deltime.marked_for_delete_at;
        struct column col(*_schema, col_name, timestamp);
        gc_clock::duration secs(deltime.local_deletion_time);

        return consume_deleted_cell(col, timestamp, gc_clock::time_point(secs));
    }

    proceed consume_deleted_cell(column &col, int64_t timestamp, gc_clock::time_point ttl) {
        auto clustering_prefix = exploded_clustering_prefix(std::move(col.clustering));
        auto ret = flush_if_needed(col.is_static, clustering_prefix);
        if (_skip_in_progress) {
            return ret;
        }

        if (col.cell.size() == 0) {
            row_marker rm(tombstone(timestamp, ttl));
            _in_progress->as_mutable_clustering_row().apply(rm);
            return ret;
        }
        if (!col.is_present) {
            return ret;
        }

        auto ac = atomic_cell::make_dead(timestamp, ttl);

        bool is_multi_cell = col.collection_extra_data.size();
        if (is_multi_cell != col.cdef->type->is_multi_cell()) {
            return ret;
        }

        if (is_multi_cell) {
            update_pending_collection(col.cdef, std::move(col.collection_extra_data), std::move(ac));
        } else if (col.is_static) {
            _in_progress->as_mutable_static_row().set_cell(*col.cdef, atomic_cell_or_collection(std::move(ac)));
        } else {
            _in_progress->as_mutable_clustering_row().set_cell(*col.cdef, atomic_cell_or_collection(std::move(ac)));
        }
        return ret;
    }
    virtual proceed consume_row_end() override {
        if (_in_progress) {
            flush();
        }
        _is_mutation_end = true;
        _out_of_range = true;
        return proceed::no;
    }

    static bound_kind start_marker_to_bound_kind(bytes_view component) {
        auto found = composite::eoc(component.back());
        switch (found) {
        // start_col may have composite_marker::none in sstables
        // from older versions of Cassandra (see CASSANDRA-7593).
        case composite::eoc::none:
            return bound_kind::incl_start;
        case composite::eoc::start:
            return bound_kind::incl_start;
        case composite::eoc::end:
            return bound_kind::excl_start;
        default:
            throw malformed_sstable_exception(sprint("Unexpected start composite marker %d\n", uint16_t(uint8_t(found))));
        }
    }

    static bound_kind end_marker_to_bound_kind(bytes_view component) {
        auto found = composite::eoc(component.back());
        switch (found) {
        // start_col may have composite_marker::none in sstables
        // from older versions of Cassandra (see CASSANDRA-7593).
        case composite::eoc::none:
            return bound_kind::incl_end;
        case composite::eoc::start:
            return bound_kind::excl_end;
        case composite::eoc::end:
            return bound_kind::incl_end;
        default:
            throw malformed_sstable_exception(sprint("Unexpected start composite marker %d\n", uint16_t(uint8_t(found))));
        }
    }

    virtual proceed consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {

        if (_skip_partition) {
            return proceed::yes;
        }

        auto start = composite_view(column::fix_static_name(*_schema, start_col)).explode();

        // Note how this is slightly different from the check in is_collection. Collection tombstones
        // do not have extra data.
        //
        // Still, it is enough to check if we're dealing with a collection, since any other tombstone
        // won't have a full clustering prefix (otherwise it isn't a range)
        if (start.size() <= _schema->clustering_key_size()) {
            auto start_ck = clustering_key_prefix::from_exploded(std::move(start));
            auto start_kind = start_marker_to_bound_kind(start_col);
            auto end = clustering_key_prefix::from_exploded(composite_view(column::fix_static_name(*_schema, end_col)).explode());
            auto end_kind = end_marker_to_bound_kind(end_col);
            if (range_tombstone::is_single_clustering_row_tombstone(*_schema, start_ck, start_kind, end, end_kind)) {
                auto ret = flush_if_needed(std::move(start_ck));
                if (!_skip_in_progress) {
                    _in_progress->as_mutable_clustering_row().apply(tombstone(deltime));
                }
                return ret;
            } else {
                auto rt = range_tombstone(std::move(start_ck), start_kind, std::move(end), end_kind, tombstone(deltime));
                position_in_partition::less_compare less(*_schema);
                auto rt_pos = rt.position();
                if (_in_progress && !less(_in_progress->position(), rt_pos)) {
                    return proceed::yes; // repeated tombstone, ignore
                }
                // Workaround for #1203
                if (!_first_row_encountered) {
                    if (_ck_ranges_walker->contains_tombstone(rt_pos, rt.end_position())) {
                        _range_tombstones.apply(std::move(rt));
                    }
                    return proceed::yes;
                }
                return flush_if_needed(std::move(rt));
            }
        } else {
            auto&& column = pop_back(start);
            auto cdef = _schema->get_column_definition(column);
            if (cdef && cdef->type->is_multi_cell() && deltime.marked_for_delete_at > cdef->dropped_at()) {
                auto ret = flush_if_needed(cdef->is_static(), exploded_clustering_prefix(std::move(start)));
                if (!_skip_in_progress) {
                    update_pending_collection(cdef, tombstone(deltime));
                }
                return ret;
            }
        }
        return proceed::yes;
    }
    virtual const io_priority_class& io_priority() override {
        return _pc;
    }

    // Returns true if the consumer is positioned at partition boundary,
    // meaning that after next read either get_mutation() will
    // return engaged mutation or end of stream was reached.
    bool is_mutation_end() const {
        return _is_mutation_end;
    }

    bool is_out_of_range() const {
        return _out_of_range;
    }

    stdx::optional<new_mutation> get_mutation() {
        return move_and_disengage(_mutation);
    }

    // Pushes ready fragments into the streamed_mutation's buffer.
    // Tries to push as much as possible, but respects buffer limits.
    // Sets streamed_mutation::_end_of_range when there are no more fragments for the query range.
    // Returns information whether the parser should continue to parse more
    // input and produce more fragments or we have collected enough and should yield.
    proceed push_ready_fragments();

    void skip_partition() {
        _pending_collection = { };
        _in_progress = { };
        _ready = { };

        _skip_partition = true;
    }

    virtual void reset(indexable_element el) override {
        sstlog.trace("mp_row_consumer {}: reset({})", this, static_cast<int>(el));
        _ready = {};
        if (el == indexable_element::partition) {
            _pending_collection = {};
            _in_progress = {};
            _is_mutation_end = true;
            _out_of_range = true;
        } else {
            // Do not reset _in_progress so that out-of-order tombstone detection works.
            _is_mutation_end = false;
        }
    }

    // Changes current fragment range.
    //
    // When there are no more fragments for current range,
    // is_out_of_range() will return true.
    //
    // The new range must not overlap with the previous range and
    // must be after it.
    //
    future<> fast_forward_to(position_range);

    bool needs_skip() const {
        return (_skip_in_progress || !_in_progress)
               && _last_lower_bound_counter != _ck_ranges_walker->lower_bound_change_counter();
    }

    // Tries to fast forward the consuming context to the next position.
    // Must be called outside consuming context.
    future<> maybe_skip();
};

struct sstable_data_source {
    shared_sstable _sst;
    mp_row_consumer _consumer;
    data_consume_context _context;
    std::unique_ptr<index_reader> _lh_index; // For lower bound
    std::unique_ptr<index_reader> _rh_index; // For upper bound

    sstable_data_source(shared_sstable sst, mp_row_consumer&& consumer)
        : _sst(std::move(sst))
        , _consumer(std::move(consumer))
        , _context(_sst->data_consume_rows(_consumer))
    { }

    sstable_data_source(shared_sstable sst, mp_row_consumer&& consumer, sstable::disk_read_range toread,
            std::unique_ptr<index_reader> lh_index = {}, std::unique_ptr<index_reader> rh_index = {})
        : _sst(std::move(sst))
        , _consumer(std::move(consumer))
        , _context(_sst->data_consume_rows(_consumer, std::move(toread)))
        , _lh_index(std::move(lh_index))
        , _rh_index(std::move(rh_index))
    { }

    sstable_data_source(schema_ptr s, shared_sstable sst, const sstables::key& k, const io_priority_class& pc,
            const query::partition_slice& slice, sstable::disk_read_range toread, streamed_mutation::forwarding fwd)
        : _sst(std::move(sst))
        , _consumer(k, s, slice, pc, fwd)
        , _context(_sst->data_consume_single_partition(_consumer, std::move(toread)))
    { }

    ~sstable_data_source() {
        auto close = [] (std::unique_ptr<index_reader>& ptr) {
            if (ptr) {
                auto f = ptr->close();
                f.handle_exception([index = std::move(ptr)] (auto&&) { });
            }
        };
        close(_lh_index);
        close(_rh_index);
    }

    index_reader& lh_index() {
        if (!_lh_index) {
            _lh_index = _sst->get_index_reader(_consumer.io_priority());
        }
        return *_lh_index;
    }
};

class sstable_streamed_mutation : public streamed_mutation::impl {
    friend class mp_row_consumer;
    lw_shared_ptr<sstable_data_source> _ds;
    tombstone _t;
    position_in_partition::less_compare _cmp;
    position_in_partition::equal_compare _eq;
    bool _index_in_current = false; // Whether _ds->_lh_index is in current partition;
public:
    sstable_streamed_mutation(schema_ptr s, dht::decorated_key dk, tombstone t, lw_shared_ptr<sstable_data_source> ds)
        : streamed_mutation::impl(s, std::move(dk), t)
        , _ds(std::move(ds))
        , _t(t)
        , _cmp(*s)
        , _eq(*s)
    {
        _ds->_consumer.set_streamed_mutation(this);
    }

    sstable_streamed_mutation(sstable_streamed_mutation&&) = delete;

    virtual future<> fill_buffer() final override {
        _ds->_consumer.push_ready_fragments();
        if (is_buffer_full() || is_end_of_stream()) {
            return make_ready_future<>();
        }
        return _ds->_consumer.maybe_skip().then([this] {
            return _ds->_context.read();
        });
    }

    future<> fast_forward_to(position_range range) override {
        _end_of_stream = false;
        forward_buffer_to(range.start());
        return _ds->_consumer.fast_forward_to(std::move(range));
    }

    future<> advance_context(position_in_partition_view pos) {
        if (pos.is_static_row()) {
            return make_ready_future<>();
        }
        return [this] {
            if (!_index_in_current) {
                _index_in_current = true;
                return _ds->lh_index().advance_to(_key);
            }
            return make_ready_future();
        }().then([this, pos] {
            return _ds->lh_index().advance_to(pos).then([this] {
                index_reader& idx = *_ds->_lh_index;
                return _ds->_context.skip_to(idx.element_kind(), idx.data_file_position());
            });
        });
    }

    static future<streamed_mutation> create(schema_ptr s, shared_sstable sst, const sstables::key& k,
                                            const query::partition_slice& slice,
                                            const io_priority_class& pc,
                                            sstable::disk_read_range toread,
                                            streamed_mutation::forwarding fwd)
    {
        auto ds = make_lw_shared<sstable_data_source>(s, sst, k, pc, slice, std::move(toread), fwd);
        return ds->_context.read().then([s, ds] {
            auto mut = ds->_consumer.get_mutation();
            assert(mut);
            auto dk = dht::global_partitioner().decorate_key(*s, std::move(mut->key));
            return make_streamed_mutation<sstable_streamed_mutation>(s, std::move(dk), mut->tomb, ds);
        });
    }
};

row_consumer::proceed
mp_row_consumer::push_ready_fragments_with_ready_set() {
    // We're merging two streams here, one is _range_tombstones
    // and the other is the main fragment stream represented by
    // _ready and _out_of_range (which means end of stream).

    while (!_sm->is_buffer_full()) {
        auto mfo = _range_tombstones.get_next(*_ready);
        if (mfo) {
            _sm->push_mutation_fragment(std::move(*mfo));
        } else {
            _sm->push_mutation_fragment(std::move(*_ready));
            _ready = {};
            return proceed(!_sm->is_buffer_full());
        }
    }
    return proceed::no;
}

row_consumer::proceed
mp_row_consumer::push_ready_fragments_out_of_range() {
    // Emit all range tombstones relevant to the current forwarding range first.
    while (!_sm->is_buffer_full()) {
        auto mfo = _range_tombstones.get_next(_fwd_end);
        if (!mfo) {
            _sm->_end_of_stream = true;
            break;
        }
        _sm->push_mutation_fragment(std::move(*mfo));
    }
    return proceed::no;
}

row_consumer::proceed
mp_row_consumer::push_ready_fragments() {
    if (_ready) {
       return push_ready_fragments_with_ready_set();
    }

    if (_out_of_range) {
        return push_ready_fragments_out_of_range();
    }

    return proceed::yes;
}

future<> mp_row_consumer::fast_forward_to(position_range r) {
    sstlog.trace("mp_row_consumer {}: fast_forward_to({})", this, r);
    _out_of_range = _is_mutation_end;
    _fwd_end = std::move(r).end();

    _range_tombstones.forward_to(r.start());

    _ck_ranges_walker->trim_front(std::move(r).start());
    if (_ck_ranges_walker->out_of_range()) {
        _out_of_range = true;
        _ready = {};
        sstlog.trace("mp_row_consumer {}: no more ranges", this);
        return make_ready_future<>();
    }

    auto start = _ck_ranges_walker->lower_bound();

    if (_ready && !_ready->relevant_for_range(*_schema, start)) {
        _ready = {};
    }

    if (_in_progress) {
        advance_to(*_in_progress);
        if (!_skip_in_progress) {
            sstlog.trace("mp_row_consumer {}: _in_progress in range", this);
            return make_ready_future<>();
        }
    }

    if (_out_of_range) {
        sstlog.trace("mp_row_consumer {}: _out_of_range=true", this);
        return make_ready_future<>();
    }

    position_in_partition::less_compare less(*_schema);
    if (!less(start, _fwd_end)) {
        _out_of_range = true;
        sstlog.trace("mp_row_consumer {}: no overlap with restrictions", this);
        return make_ready_future<>();
    }

    sstlog.trace("mp_row_consumer {}: advance_context({})", this, start);
    _last_lower_bound_counter = _ck_ranges_walker->lower_bound_change_counter();
    return _sm->advance_context(start);
}

future<> mp_row_consumer::maybe_skip() {
    if (!needs_skip()) {
        return make_ready_future<>();
    }
    _last_lower_bound_counter = _ck_ranges_walker->lower_bound_change_counter();
    auto pos = _ck_ranges_walker->lower_bound();
    sstlog.trace("mp_row_consumer {}: advance_context({})", this, pos);
    return _sm->advance_context(pos);
}

static int adjust_binary_search_index(int idx) {
    if (idx < 0) {
        // binary search gives us the first index _greater_ than the key searched for,
        // i.e., its insertion position
        auto gt = (idx + 1) * -1;
        idx = gt - 1;
    }
    return idx;
}

future<uint64_t> sstables::sstable::data_end_position(uint64_t summary_idx, uint64_t index_idx, const index_list& il,
                                                      const io_priority_class& pc) {
    if (uint64_t(index_idx + 1) < il.size()) {
        return make_ready_future<uint64_t>(il[index_idx + 1].position());
    }

    return data_end_position(summary_idx, pc);
}

future<uint64_t> sstables::sstable::data_end_position(uint64_t summary_idx, const io_priority_class& pc) {
    // We should only go to the end of the file if we are in the last summary group.
    // Otherwise, we will determine the end position of the current data read by looking
    // at the first index in the next summary group.
    if (size_t(summary_idx + 1) >= _components->summary.entries.size()) {
        return make_ready_future<uint64_t>(data_size());
    }

    return read_indexes(summary_idx + 1, pc).then([] (auto next_il) {
        return next_il.front().position();
    });
}

future<streamed_mutation_opt>
sstables::sstable::read_row(schema_ptr schema,
                            const sstables::key& key,
                            const query::partition_slice& slice,
                            const io_priority_class& pc,
                            streamed_mutation::forwarding fwd) {

    assert(schema);

    return find_disk_ranges(schema, key, slice, pc).then([this, &key, &slice, &pc, schema, fwd] (disk_read_range toread) {
        if (!toread.found_row()) {
            _filter_tracker.add_false_positive();
        }
        if (!toread) {
            return make_ready_future<streamed_mutation_opt>();
        }
        _filter_tracker.add_true_positive();
        return sstable_streamed_mutation::create(schema, this->shared_from_this(), key, slice, pc, std::move(toread), fwd).then([] (auto sm) {
            return streamed_mutation_opt(std::move(sm));
        });
    });
}

static inline void ensure_len(bytes_view v, size_t len) {
    if (v.size() < len) {
        throw malformed_sstable_exception(sprint("Expected {} bytes, but remaining is {}", len, v.size()));
    }
}

template <typename T>
static inline T read_be(const signed char* p) {
    return ::read_be<T>(reinterpret_cast<const char*>(p));
}

template<typename T>
static inline T consume_be(bytes_view& p) {
    ensure_len(p, sizeof(T));
    T i = read_be<T>(p.data());
    p.remove_prefix(sizeof(T));
    return i;
}

static inline bytes_view consume_bytes(bytes_view& p, size_t len) {
    ensure_len(p, len);
    auto ret = bytes_view(p.data(), len);
    p.remove_prefix(len);
    return ret;
}

static inline clustering_key_prefix get_clustering_key(
        const schema& schema, bytes_view col_name) {
    mp_row_consumer::column col(schema, std::move(col_name), api::max_timestamp);
    return std::move(col.clustering);
}

static bool has_static_columns(const schema& schema, index_entry &ie) {
    // We can easily check if there are any static columns in this partition,
    // because the static columns always come first, so the first promoted
    // index block will start with one, if there are any. The name of a static
    // column is a  composite beginning with a special marker (0xffff).
    // But we can only assume the column name is composite if the schema is
    // compound - if it isn't, we cannot have any static columns anyway.
    //
    // The first 18 bytes are deletion times (4+8), num blocks (4), and
    // length of start column (2). Then come the actual column name bytes.
    // See also composite::is_static().
    auto data = ie.get_promoted_index_bytes();
    return schema.is_compound() && data.size() >= 20 && data[18] == -1 && data[19] == -1;
}

future<sstable::disk_read_range>
sstables::sstable::find_disk_ranges(
        schema_ptr schema, const sstables::key& key,
        const query::partition_slice& slice,
        const io_priority_class& pc) {
    auto& partitioner = dht::global_partitioner();
    auto token = partitioner.get_token(key_view(key));

    if (token < partitioner.get_token(key_view(_components->summary.first_key.value))
            || token > partitioner.get_token(key_view(_components->summary.last_key.value))) {
        return make_ready_future<disk_read_range>();
    }
    auto summary_idx = adjust_binary_search_index(binary_search(_components->summary.entries, key, token));
    if (summary_idx < 0) {
        return make_ready_future<disk_read_range>();
    }

    return read_indexes(summary_idx, pc).then([this, schema, &slice, &key, token, summary_idx, &pc] (auto index_list) {
        auto index_idx = this->binary_search(index_list, key, token);
        if (index_idx < 0) {
            return make_ready_future<disk_read_range>();
        }
        index_entry& ie = index_list[index_idx];
        if (ie.get_promoted_index_bytes().size() >= 16) {
          try {
            auto&& pkey = partition_key::from_exploded(*schema, key.explode(*schema));
            auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*schema, slice, pkey);
            if (ck_ranges.size() == 1 && ck_ranges.begin()->is_full()) {
                // When no clustering filter is given to sstable::read_row(),
                // we get here one range unbounded on both sides. This is fine
                // (the code below will work with an unbounded range), but
                // let's drop this range to revert to the classic behavior of
                // reading entire sstable row without using the promoted index
            } else if (has_static_columns(*schema, ie)) {
                // FIXME: If we need to read the static columns and also a
                // non-full clustering key range, we need to return two byte
                // ranges in the returned disk_read_range. We don't support
                // this yet so for now let's fall back to reading the entire
                // partition which is wasteful but at least correct.
                // This case should be replaced by correctly adding the static
                // column's blocks to the return.
            } else if (ck_ranges.size() == 1) {
                auto data = ie.get_promoted_index_bytes();
                // note we already verified above that data.size >= 16
                sstables::deletion_time deltime;
                deltime.local_deletion_time = consume_be<uint32_t>(data);
                deltime.marked_for_delete_at = consume_be<uint64_t>(data);
                uint32_t num_blocks = consume_be<uint32_t>(data);
                // We do a linear search on the promoted index. If we were to
                // look in the same promoted index several times it might have
                // made sense to build an array of key starts so we can do a
                // binary search. We could do this once we have a key cache.
                auto& range_start = ck_ranges.begin()->start();
                bool found_range_start = false;
                uint64_t range_start_pos;
                auto& range_end = ck_ranges.begin()->end();

                auto cmp = clustering_key_prefix::tri_compare(*schema);
                while (num_blocks--) {
                    uint16_t len = consume_be<uint16_t>(data);
                    // The promoted index contains ranges of full column
                    // names, which may include a clustering key and column.
                    // But we only need to match the clustering key, because
                    // we got a clustering key range to search for.
                    auto start_ck = get_clustering_key(*schema,
                            consume_bytes(data, len));
                    len = consume_be<uint16_t>(data);
                    auto end_ck = get_clustering_key(*schema,
                            consume_bytes(data, len));
                    uint64_t offset = consume_be<uint64_t>(data);
                    uint64_t width = consume_be<uint64_t>(data);
                    if (!found_range_start) {
                        if (!range_start || cmp(range_start->value(), end_ck) <= 0) {
                            range_start_pos = ie.position() + offset;
                            found_range_start = true;
                        }
                    }
                    bool found_range_end = false;
                    uint64_t range_end_pos;
                    if (range_end) {
                        if (cmp(range_end->value(), start_ck) < 0) {
                            // this block is already past the range_end
                            found_range_end = true;
                            range_end_pos = ie.position() + offset;
                        } else if (cmp(range_end->value(), end_ck) < 0 || num_blocks == 0) {
                            // range_end is in the middle of this block.
                            // Note the strict inequality above is important:
                            // if range_end==end_ck the next block may contain
                            // still more items matching range_end.
                            found_range_end = true;
                            range_end_pos = ie.position() + offset + width;
                        }
                    } else if (num_blocks == 0) {
                        // When !range_end, read until the last block.
                        // In this case we could have also found the end of
                        // the partition using the index.
                        found_range_end = true;
                        range_end_pos = ie.position() + offset + width;
                    }
                    if (found_range_end) {
                        if (!found_range_start) {
                            // return empty range
                            range_start_pos = range_end_pos = 0;
                        }
                        return make_ready_future<disk_read_range>(
                                disk_read_range(range_start_pos, range_end_pos,
                                        key, deltime));
                    }
                }
            }
            // Else, if more than one clustering-key range needs to be read,
            // fall back to reading the entire partition.
            // FIXME: support multiple ranges, and do not fall back to reading
            // the entire partition.
          } catch (...) {
              // Fall back to reading whole partition
              sstlog.error("Failed to parse promoted index for sstable {}, page {}, index {}: {}", this->get_filename(),
                  summary_idx, index_idx, std::current_exception());
          }
        }
        // If we're still here there is no promoted index, or we had problems
        // using it, so just just find the entire partition's range.
        auto start = ie.position();
        return this->data_end_position(summary_idx, index_idx, index_list, pc).then([start] (uint64_t end) {
            return disk_read_range(start, end);
        });
    });
}

void index_entry::parse_promoted_index(const schema& s) {
    bytes_view data = get_promoted_index_bytes();
    if (data.empty()) {
        return;
    }

    sstables::deletion_time del_time;
    del_time.local_deletion_time = consume_be<uint32_t>(data);
    del_time.marked_for_delete_at = consume_be<uint64_t>(data);

    auto num_blocks = consume_be<uint32_t>(data);
    std::deque<promoted_index::entry> entries;
    while (num_blocks--) {
        uint16_t len = consume_be<uint16_t>(data);
        auto start_ck = composite_view(consume_bytes(data, len), s.is_compound());
        len = consume_be<uint16_t>(data);
        auto end_ck = composite_view(consume_bytes(data, len), s.is_compound());
        uint64_t offset = consume_be<uint64_t>(data);
        uint64_t width = consume_be<uint64_t>(data);
        entries.emplace_back(promoted_index::entry{start_ck, end_ck, offset, width});
    }

    _promoted_index = promoted_index{del_time, std::move(entries)};
}

class mutation_reader::impl {
private:
    bool _read_enabled = true;
    const io_priority_class& _pc;
    schema_ptr _schema;
    lw_shared_ptr<sstable_data_source> _ds;
    std::function<future<lw_shared_ptr<sstable_data_source>> ()> _get_data_source;
    stdx::optional<dht::decorated_key> _key;
public:
    impl(shared_sstable sst, schema_ptr schema, sstable::disk_read_range toread,
         const io_priority_class &pc,
         streamed_mutation::forwarding fwd)
        : _pc(pc)
        , _schema(schema)
        , _get_data_source([this, sst = std::move(sst), toread, &pc, fwd] {
            auto consumer = mp_row_consumer(_schema, query::full_slice, pc, fwd);
            auto ds = make_lw_shared<sstable_data_source>(std::move(sst), std::move(consumer), std::move(toread));
            return make_ready_future<lw_shared_ptr<sstable_data_source>>(std::move(ds));
        }) { }
    impl(shared_sstable sst, schema_ptr schema,
         const io_priority_class &pc,
         streamed_mutation::forwarding fwd)
        : _pc(pc)
        , _schema(schema)
        , _get_data_source([this, sst = std::move(sst), &pc, fwd] {
            auto consumer = mp_row_consumer(_schema, query::full_slice, pc, fwd);
            auto ds = make_lw_shared<sstable_data_source>(std::move(sst), std::move(consumer));
            return make_ready_future<lw_shared_ptr<sstable_data_source>>(std::move(ds));
        }) { }
    impl(shared_sstable sst,
         schema_ptr schema,
         const dht::partition_range& pr,
         const query::partition_slice& slice,
         const io_priority_class& pc,
         streamed_mutation::forwarding fwd)
        : _pc(pc)
        , _schema(schema)
        , _get_data_source([this, &pr, sst = std::move(sst), &pc, &slice, fwd] () mutable {
            auto lh_index = sst->get_index_reader(_pc); // lh = left hand
            auto rh_index = sst->get_index_reader(_pc);
            auto f = seastar::when_all_succeed(lh_index->advance_to_start(pr), rh_index->advance_to_end(pr));
            return f.then([this, lh_index = std::move(lh_index), rh_index = std::move(rh_index), sst = std::move(sst), &pc, &slice, fwd] () mutable {
                sstable::disk_read_range drr{lh_index->data_file_position(),
                                             rh_index->data_file_position()};
                if (!drr.found_row()) {
                    _read_enabled = false;
                }
                auto consumer = mp_row_consumer(_schema, slice, pc, fwd);
                return make_lw_shared<sstable_data_source>(std::move(sst), std::move(consumer), std::move(drr),
                    std::move(lh_index), std::move(rh_index));
            });
        }) { }

    // Reference to _consumer is passed to data_consume_rows() in the constructor so we must not allow move/copy
    impl(impl&&) = delete;
    impl(const impl&) = delete;

    future<streamed_mutation_opt> read() {
        if (!_read_enabled) {
            // empty mutation reader returns EOF immediately
            return make_ready_future<streamed_mutation_opt>();
        }

        if (_ds) {
            return do_read();
        }
        return (_get_data_source)().then([this] (lw_shared_ptr<sstable_data_source> ds) {
            // We must get the sstable_data_source and backup it in case we enable read
            // again in the future.
            _ds = std::move(ds);
            if (!_read_enabled) {
                return make_ready_future<streamed_mutation_opt>();
            }
            return do_read();
        });
    }
    future<> fast_forward_to(const dht::partition_range& pr) {
        assert(_ds->_lh_index);
        assert(_ds->_rh_index);
        auto f1 = _ds->_lh_index->advance_to_start(pr);
        auto f2 = _ds->_rh_index->advance_to_end(pr);
        return seastar::when_all_succeed(std::move(f1), std::move(f2)).then([this] {
            auto start = _ds->_lh_index->data_file_position();
            auto end = _ds->_rh_index->data_file_position();
            if (start != end) {
                _read_enabled = true;
                return _ds->_context.fast_forward_to(start, end);
            }
            _read_enabled = false;
            return make_ready_future<>();
        });
    }
private:
    future<streamed_mutation_opt> do_read() {
        auto& consumer = _ds->_consumer;
        if (!consumer.is_mutation_end()) {
            {
                return _ds->lh_index().advance_to(dht::ring_position_view::for_after_key(*_key)).then([this] {
                    return _ds->_context.skip_to(_ds->_lh_index->element_kind(), _ds->_lh_index->data_file_position()).then([this] {
                        assert(_ds->_consumer.is_mutation_end());
                        return do_read();
                    });
                });
            }
        }
        return _ds->_context.read().then([this] {
            auto& consumer = _ds->_consumer;
            auto mut = consumer.get_mutation();
            if (!mut) {
                return make_ready_future<streamed_mutation_opt>();
            }
            _key = dht::global_partitioner().decorate_key(*_schema, std::move(mut->key));
            auto sm = make_streamed_mutation<sstable_streamed_mutation>(_schema, *_key, mut->tomb, _ds);
            return make_ready_future<streamed_mutation_opt>(std::move(sm));
        });
    }
};

mutation_reader::~mutation_reader() = default;
mutation_reader::mutation_reader(mutation_reader&&) = default;
mutation_reader& mutation_reader::operator=(mutation_reader&&) = default;
mutation_reader::mutation_reader(std::unique_ptr<impl> p)
    : _pimpl(std::move(p)) { }
future<streamed_mutation_opt> mutation_reader::read() {
    return _pimpl->read();
}
future<> mutation_reader::fast_forward_to(const dht::partition_range& pr) {
    return _pimpl->fast_forward_to(pr);
}

mutation_reader sstable::read_rows(schema_ptr schema, const io_priority_class& pc, streamed_mutation::forwarding fwd) {
    return std::make_unique<mutation_reader::impl>(shared_from_this(), schema, pc, fwd);
}

mutation_reader
sstable::read_range_rows(schema_ptr schema,
                         const dht::partition_range& range,
                         const query::partition_slice& slice,
                         const io_priority_class& pc,
                         streamed_mutation::forwarding fwd) {
    return std::make_unique<mutation_reader::impl>(
        shared_from_this(), std::move(schema), range, slice, pc, fwd);
}

}

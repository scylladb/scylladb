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
#include "binary_search.hh"
#include "../dht/i_partitioner.hh"

namespace sstables {

static inline bytes_view pop_back(std::vector<bytes_view>& vec) {
    auto b = std::move(vec.back());
    vec.pop_back();
    return b;
}

class sstable_mutation_reader;

class mp_row_consumer : public row_consumer {
public:
    struct new_mutation {
        partition_key key;
        tombstone tomb;
    };
private:
    sstable_mutation_reader* _reader;
    schema_ptr _schema;
    const query::partition_slice& _slice;
    bool _out_of_range = false;
    stdx::optional<query::clustering_key_filter_ranges> _ck_ranges;
    stdx::optional<clustering_ranges_walker> _ck_ranges_walker;

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
    struct column {
        bool is_static;
        bytes_view col_name;
        std::vector<bytes_view> clustering;
        // see is_collection. collections have an extra element aside from the name.
        // This will be non-zero size if this is a collection, and zero size othersize.
        bytes_view collection_extra_data;
        bytes_view cell;
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

        std::vector<bytes_view> extract_clustering_key(const schema& schema) {
            return composite_view(col_name, schema.is_compound()).explode();
        }
        column(const schema& schema, bytes_view col, api::timestamp_type timestamp)
            : is_static(check_static(schema, col))
            , col_name(fix_static_name(col, is_static))
            , clustering(extract_clustering_key(schema))
            , collection_extra_data(is_collection(schema) ? pop_back(clustering) : bytes()) // collections are not supported with COMPACT STORAGE, so this is fine
            , cell(!schema.is_dense() ? pop_back(clustering) : (*(schema.regular_begin())).name()) // dense: cell name is not provided. It is the only regular column
            , cdef(schema.get_column_definition(to_bytes(cell)))
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

            if (!cdef->is_multi_cell()) {
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

    mp_row_consumer(sstable_mutation_reader* reader,
                    const schema_ptr schema,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    reader_resource_tracker resource_tracker,
                    streamed_mutation::forwarding fwd)
            : row_consumer(std::move(resource_tracker), pc)
            , _reader(reader)
            , _schema(schema)
            , _slice(slice)
            , _fwd(fwd)
            , _range_tombstones(*_schema)
    { }

    mp_row_consumer(sstable_mutation_reader* reader,
                    const schema_ptr schema,
                    const io_priority_class& pc,
                    reader_resource_tracker resource_tracker,
                    streamed_mutation::forwarding fwd)
            : mp_row_consumer(reader, schema, schema->full_slice(), pc, std::move(resource_tracker), fwd) { }

    virtual proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        if (!_is_mutation_end) {
            return proceed::yes;
        }
        _mutation = new_mutation{partition_key::from_exploded(key.explode(*_schema)), tombstone(deltime)};
        setup_for_partition(_mutation->key);
        return proceed::no;
    }

    void setup_for_partition(const partition_key& pk) {
        _is_mutation_end = false;
        _skip_partition = false;
        _skip_in_progress = false;
        set_up_ck_ranges(pk);
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

    proceed flush_if_needed(bool is_static, const std::vector<bytes_view>& ecp) {
        auto pos = [&] {
            if (is_static) {
                return position_in_partition(position_in_partition::static_row_tag_t());
            } else {
                auto ck = clustering_key_prefix::from_exploded_view(ecp);
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
            ccb.add_maybe_unsorted_shard(counter_shard(counter_id(utils::UUID(id_hi, id_lo)), value, clock));
        }
        ccb.sort_and_remove_duplicates();
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

        auto ret = flush_if_needed(col.is_static, col.clustering);
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
            if (is_multi_cell != col.cdef->is_multi_cell()) {
                return;
            }
            if (is_multi_cell) {
                update_pending_collection(col.cdef, to_bytes(col.collection_extra_data), std::move(ac));
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
        auto ret = flush_if_needed(col.is_static, col.clustering);
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
        if (is_multi_cell != col.cdef->is_multi_cell()) {
            return ret;
        }

        if (is_multi_cell) {
            update_pending_collection(col.cdef, to_bytes(col.collection_extra_data), std::move(ac));
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

    virtual proceed consume_shadowable_row_tombstone(bytes_view col_name, sstables::deletion_time deltime) override {
        if (_skip_partition) {
            return proceed::yes;
        }
        auto key = composite_view(column::fix_static_name(*_schema, col_name)).explode();
        auto ck = clustering_key_prefix::from_exploded_view(key);
        auto ret = flush_if_needed(std::move(ck));
        if (!_skip_in_progress) {
            _in_progress->as_mutable_clustering_row().apply(shadowable_tombstone(tombstone(deltime)));
        }
        return ret;
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
            auto start_ck = clustering_key_prefix::from_exploded_view(start);
            auto start_kind = start_marker_to_bound_kind(start_col);
            auto end = clustering_key_prefix::from_exploded_view(composite_view(column::fix_static_name(*_schema, end_col)).explode());
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
            auto cdef = _schema->get_column_definition(to_bytes(column));
            if (cdef && cdef->is_multi_cell() && deltime.marked_for_delete_at > cdef->dropped_at()) {
                auto ret = flush_if_needed(cdef->is_static(), start);
                if (!_skip_in_progress) {
                    update_pending_collection(cdef, tombstone(deltime));
                }
                return ret;
            }
        }
        return proceed::yes;
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
    stdx::optional<position_in_partition_view> fast_forward_to(position_range);

    bool needs_skip() const {
        return (_skip_in_progress || !_in_progress)
               && _last_lower_bound_counter != _ck_ranges_walker->lower_bound_change_counter();
    }

    // Tries to fast forward the consuming context to the next position.
    // Must be called outside consuming context.
    stdx::optional<position_in_partition_view> maybe_skip();
};

future<streamed_mutation_opt>
sstables::sstable::read_row(schema_ptr schema,
                            const sstables::key& key,
                            const query::partition_slice& slice,
                            const io_priority_class& pc,
                            reader_resource_tracker resource_tracker,
                            streamed_mutation::forwarding fwd)
{
    return do_with(dht::global_partitioner().decorate_key(*schema,
                key.to_partition_key(*schema)),
                [this, schema, &slice, &pc, resource_tracker = std::move(resource_tracker), fwd] (auto& dk) {
                    return this->read_row(schema, dk, slice, pc, std::move(resource_tracker), fwd);
                });
}

flat_mutation_reader
sstables::sstable::read_row_flat(schema_ptr schema,
                                 const sstables::key& key,
                                 const query::partition_slice& slice,
                                 const io_priority_class& pc,
                                 reader_resource_tracker resource_tracker,
                                 streamed_mutation::forwarding fwd)
{
    auto dk = dht::global_partitioner().decorate_key(*schema, key.to_partition_key(*schema));
    return this->read_row_flat(schema, std::move(dk), slice, pc, std::move(resource_tracker), fwd);
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

promoted_index promoted_index_view::parse(const schema& s) const {
    bytes_view data = _bytes;

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

    return promoted_index{del_time, std::move(entries)};
}

sstables::deletion_time promoted_index_view::get_deletion_time() const {
    bytes_view data = _bytes;
    sstables::deletion_time del_time;
    del_time.local_deletion_time = consume_be<uint32_t>(data);
    del_time.marked_for_delete_at = consume_be<uint64_t>(data);
    return del_time;
}

static
future<> advance_to_upper_bound(index_reader& ix, const schema& s, const query::partition_slice& slice, dht::ring_position_view key) {
    auto& ranges = slice.row_ranges(s, *key.key());
    if (ranges.empty()) {
        return ix.advance_past(position_in_partition_view::for_static_row());
    } else {
        return ix.advance_past(position_in_partition_view::for_range_end(ranges[ranges.size() - 1]));
    }
}

class sstable_mutation_reader : public flat_mutation_reader::impl {
    friend class mp_row_consumer;
    shared_sstable _sst;
    mp_row_consumer _consumer;
    bool _index_in_current_partition = false; // Whether _lh_index is in current partition
    bool _will_likely_slice = false;
    bool _read_enabled = true;
    data_consume_context_opt _context;
    std::unique_ptr<index_reader> _lh_index; // For lower bound
    std::unique_ptr<index_reader> _rh_index; // For upper bound
    // We avoid unnecessary lookup for single partition reads thanks to this flag
    bool _single_partition_read = false;
    std::function<future<> ()> _initialize;
    streamed_mutation::forwarding _fwd;
    stdx::optional<dht::decorated_key> _current_partition_key;
    bool _partition_finished = true;
public:
    sstable_mutation_reader(shared_sstable sst, schema_ptr schema,
         const io_priority_class &pc,
         reader_resource_tracker resource_tracker,
         streamed_mutation::forwarding fwd)
        : impl(std::move(schema))
        , _sst(std::move(sst))
        , _consumer(this, _schema, _schema->full_slice(), pc, std::move(resource_tracker), fwd)
        , _initialize([this] {
            _context = _sst->data_consume_rows(_consumer);
            return make_ready_future<>();
        })
        , _fwd(fwd) { }
    sstable_mutation_reader(shared_sstable sst,
         schema_ptr schema,
         const dht::partition_range& pr,
         const query::partition_slice& slice,
         const io_priority_class& pc,
         reader_resource_tracker resource_tracker,
         streamed_mutation::forwarding fwd,
         mutation_reader::forwarding fwd_mr)
        : impl(std::move(schema))
        , _sst(std::move(sst))
        , _consumer(this, _schema, slice, pc, std::move(resource_tracker), fwd)
        , _initialize([this, pr, &pc, &slice, resource_tracker = std::move(resource_tracker), fwd_mr] () mutable {
            _lh_index = _sst->get_index_reader(pc); // lh = left hand
            _rh_index = _sst->get_index_reader(pc);
            auto f = seastar::when_all_succeed(_lh_index->advance_to_start(pr), _rh_index->advance_to_end(pr));
            return f.then([this, &pc, &slice, fwd_mr] () mutable {
                sstable::disk_read_range drr{_lh_index->data_file_position(),
                                             _rh_index->data_file_position()};
                auto last_end = fwd_mr ? _sst->data_size() : drr.end;
                _context = _sst->data_consume_rows(_consumer, std::move(drr), last_end);
                _index_in_current_partition = true;
                _will_likely_slice = will_likely_slice(slice);
            });
        })
        , _fwd(fwd) { }
    sstable_mutation_reader(shared_sstable sst,
                            schema_ptr schema,
                            dht::ring_position_view key,
                            const query::partition_slice& slice,
                            const io_priority_class& pc,
                            reader_resource_tracker resource_tracker,
                            streamed_mutation::forwarding fwd,
                            mutation_reader::forwarding fwd_mr)
        : impl(std::move(schema))
        , _sst(std::move(sst))
        , _consumer(this, _schema, slice, pc, std::move(resource_tracker), fwd)
        , _initialize([this, key = std::move(key), &pc, &slice, fwd_mr] () mutable {
            _lh_index = _sst->get_index_reader(pc);
            auto f = _lh_index->advance_and_check_if_present(key);
            return f.then([this, &slice, &pc, key] (bool present) mutable {
                if (!present) {
                    _sst->get_filter_tracker().add_false_positive();
                    return make_ready_future<>();
                }

                _sst->get_filter_tracker().add_true_positive();

                _rh_index = std::make_unique<index_reader>(*_lh_index);
                auto f = advance_to_upper_bound(*_rh_index, *_schema, slice, key);
                return f.then([this, &slice, &pc] () mutable {
                    _context = _sst->data_consume_single_partition(_consumer,
                            { _lh_index->data_file_position(), _rh_index->data_file_position() });
                    _will_likely_slice = will_likely_slice(slice);
                    _index_in_current_partition = true;
                });
            });
        })
        , _fwd(fwd) { }

    // Reference to _consumer is passed to data_consume_rows() in the constructor so we must not allow move/copy
    sstable_mutation_reader(sstable_mutation_reader&&) = delete;
    sstable_mutation_reader(const sstable_mutation_reader&) = delete;
    ~sstable_mutation_reader() {
        auto close = [] (std::unique_ptr<index_reader>& ptr) {
            if (ptr) {
                auto f = ptr->close();
                f.handle_exception([index = std::move(ptr)] (auto&&) { });
            }
        };
        close(_lh_index);
        close(_rh_index);
    }
private:
    static bool will_likely_slice(const query::partition_slice& slice) {
        return (!slice.default_row_ranges().empty() && !slice.default_row_ranges()[0].is_full())
               || slice.get_specific_ranges();
    }
    index_reader& lh_index() {
        if (!_lh_index) {
            _lh_index = _sst->get_index_reader(_consumer.io_priority());
        }
        return *_lh_index;
    }
    future<> advance_to_next_partition() {
        sstlog.trace("reader {}: advance_to_next_partition()", this);
        auto& consumer = _consumer;
        if (consumer.is_mutation_end()) {
            sstlog.trace("reader {}: already at partition boundary", this);
            _index_in_current_partition = false;
            return make_ready_future<>();
        }
        if (_single_partition_read) {
            return make_ready_future<>();
        }
        return (_index_in_current_partition
                ? _lh_index->advance_to_next_partition()
                : lh_index().advance_to(dht::ring_position_view::for_after_key(*_current_partition_key))).then([this] {
            _index_in_current_partition = true;
            if (bool(_rh_index) && _lh_index->data_file_position() > _rh_index->data_file_position()) {
                _read_enabled = false;
                return make_ready_future<>();
            }
            return _context->skip_to(_lh_index->element_kind(), _lh_index->data_file_position());
        });
    }
    future<> read_from_index() {
        sstlog.trace("reader {}: read from index", this);
        auto tomb = _lh_index->partition_tombstone();
        if (!tomb) {
            sstlog.trace("reader {}: no tombstone", this);
            return read_from_datafile();
        }
        auto pk = _lh_index->partition_key().to_partition_key(*_schema);
        auto key = dht::global_partitioner().decorate_key(*_schema, std::move(pk));
        _consumer.setup_for_partition(key.key());
        on_next_partition(std::move(key), tombstone(*tomb));
        return make_ready_future<>();
    }
    future<> read_from_datafile() {
        sstlog.trace("reader {}: read from data file", this);
        return _context->read().then([this] {
            auto& consumer = _consumer;
            auto mut = consumer.get_mutation();
            if (!mut) {
                sstlog.trace("reader {}: eof", this);
                return make_ready_future<>();
            }
            on_next_partition(dht::global_partitioner().decorate_key(*_schema, std::move(mut->key)), mut->tomb);
            return make_ready_future<>();
        });
    }
    // Assumes that we're currently positioned at partition boundary.
    future<> read_partition() {
        sstlog.trace("reader {}: reading partition", this);

        if (!_read_enabled) {
            return make_ready_future<>();
        }

        if (!_consumer.is_mutation_end()) {
            if (_single_partition_read) {
                _read_enabled = false;
                return make_ready_future<>();
            }
            // FIXME: give more details from _context
            throw malformed_sstable_exception("consumer not at partition boundary", _sst->get_filename());
        }

        // It's better to obtain partition information from the index if we already have it.
        // We can save on IO if the user will skip past the front of partition immediately.
        //
        // It is also better to pay the cost of reading the index if we know that we will
        // need to use the index anyway soon.
        //
        if (_index_in_current_partition) {
            if (_context->eof()) {
                sstlog.trace("reader {}: eof", this);
                return make_ready_future<>();
            }
            if (_lh_index->partition_data_ready()) {
                return read_from_index();
            }
            if (_will_likely_slice) {
                return _lh_index->read_partition_data().then([this] {
                    return read_from_index();
                });
            }
        }

        // FIXME: advance index to current partition if _will_likely_slice
        return read_from_datafile();
    }
    // Can be called from any position.
    future<> read_next_partition() {
        sstlog.trace("reader {}: read next partition", this);
        // If next partition exists then on_next_partition will be called
        // and _end_of_stream will be set to false again.
        _end_of_stream = true;
        if (!_read_enabled) {
            sstlog.trace("reader {}: eof", this);
            return make_ready_future<>();
        }
        return advance_to_next_partition().then([this] {
            return read_partition();
        });
    }
    future<> advance_context(stdx::optional<position_in_partition_view> pos) {
        if (!pos) {
            return make_ready_future<>();
        }
        if (pos->is_before_all_fragments(*_schema)) {
            return make_ready_future<>();
        }
        assert (_current_partition_key);
        return [this] {
            if (!_index_in_current_partition) {
                _index_in_current_partition = true;
                return lh_index().advance_to(*_current_partition_key);
            }
            return make_ready_future();
        }().then([this, pos] {
            return lh_index().advance_to(*pos).then([this] {
                index_reader& idx = *_lh_index;
                return _context->skip_to(idx.element_kind(), idx.data_file_position());
            });
        });
    }
    void on_next_partition(dht::decorated_key key, tombstone tomb) {
        _partition_finished = false;
        _end_of_stream = false;
        _current_partition_key = std::move(key);
        push_mutation_fragment(
            mutation_fragment(partition_start(*_current_partition_key, tomb)));
    }
    bool is_initialized() const {
        return bool(_context);
    }
    future<> ensure_initialized() {
        if (is_initialized()) {
            return make_ready_future<>();
        }
        return _initialize();
    }
public:
    void on_end_of_stream() {
        if (_fwd == streamed_mutation::forwarding::yes) {
            _end_of_stream = true;
        } else {
            this->push_mutation_fragment(mutation_fragment(partition_end()));
            _partition_finished = true;
        }
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        return ensure_initialized().then([this, &pr] {
            if (!is_initialized()) {
                _end_of_stream = true;
                return make_ready_future<>();
            } else {
                clear_buffer();
                _partition_finished = true;
                _end_of_stream = false;
                assert(_lh_index);
                assert(_rh_index);
                auto f1 = _lh_index->advance_to_start(pr);
                auto f2 = _rh_index->advance_to_end(pr);
                return seastar::when_all_succeed(std::move(f1), std::move(f2)).then([this] {
                    auto start = _lh_index->data_file_position();
                    auto end = _rh_index->data_file_position();
                    if (start != end) {
                        _read_enabled = true;
                        _index_in_current_partition = true;
                        return _context->fast_forward_to(start, end);
                    }
                    _index_in_current_partition = false;
                    _read_enabled = false;
                    return make_ready_future<>();
                });
            }
        });
    }
    virtual future<> fill_buffer() override {
        if (_end_of_stream) {
            return make_ready_future<>();
        }
        if (!is_initialized()) {
            return _initialize().then([this] {
                if (!is_initialized()) {
                    _end_of_stream = true;
                    return make_ready_future<>();
                } else {
                    return fill_buffer();
                }
            });
        }
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            if (_partition_finished) {
                return read_next_partition();
            } else {
                return do_until([this] { return is_buffer_full() || _partition_finished || _end_of_stream; }, [this] {
                    _consumer.push_ready_fragments();
                    if (is_buffer_full() || _partition_finished || _end_of_stream) {
                        return make_ready_future<>();
                    }
                    return advance_context(_consumer.maybe_skip()).then([this] {
                        return _context->read();
                    });
                });
            }
        });
    }
    virtual void next_partition() override {
        if (is_initialized()) {
            if (_fwd == streamed_mutation::forwarding::yes) {
                clear_buffer();
                _partition_finished = true;
                _end_of_stream = false;
            } else {
                clear_buffer_to_next_partition();
                if (!_partition_finished && is_buffer_empty()) {
                    _partition_finished = true;
                }
            }
        }
        // If _ds is not created then next_partition() has no effect because there was no partition_start emitted yet.
    }
    virtual future<> fast_forward_to(position_range cr) override {
        forward_buffer_to(cr.start());
        if (!_partition_finished) {
            _end_of_stream = false;
            return advance_context(_consumer.fast_forward_to(std::move(cr)));
        } else {
            _end_of_stream = true;
            return make_ready_future<>();
        }
    }
};

flat_mutation_reader sstable::read_rows_flat(schema_ptr schema, const io_priority_class& pc, streamed_mutation::forwarding fwd) {
    return make_flat_mutation_reader<sstable_mutation_reader>(shared_from_this(), std::move(schema), pc, no_resource_tracking(), fwd);
}

future<streamed_mutation_opt>
sstables::sstable::read_row(schema_ptr schema,
    dht::ring_position_view key,
    const query::partition_slice& slice,
    const io_priority_class& pc,
    reader_resource_tracker resource_tracker,
    streamed_mutation::forwarding fwd)
{
    return streamed_mutation_from_flat_mutation_reader(
        read_row_flat(std::move(schema), std::move(key), slice, pc, std::move(resource_tracker), fwd));
}

flat_mutation_reader
sstables::sstable::read_row_flat(schema_ptr schema,
                                 dht::ring_position_view key,
                                 const query::partition_slice& slice,
                                 const io_priority_class& pc,
                                 reader_resource_tracker resource_tracker,
                                 streamed_mutation::forwarding fwd)
{
    return make_flat_mutation_reader<sstable_mutation_reader>(shared_from_this(), std::move(schema), std::move(key), slice, pc, std::move(resource_tracker), fwd, mutation_reader::forwarding::no);
}

mutation_reader
sstable::read_range_rows(schema_ptr schema,
                         const dht::partition_range& range,
                         const query::partition_slice& slice,
                         const io_priority_class& pc,
                         reader_resource_tracker resource_tracker,
                         streamed_mutation::forwarding fwd,
                         mutation_reader::forwarding fwd_mr) {
    return mutation_reader_from_flat_mutation_reader(
        read_range_rows_flat(std::move(schema), range, slice, pc, std::move(resource_tracker), fwd, fwd_mr));
}

flat_mutation_reader
sstable::read_range_rows_flat(schema_ptr schema,
                         const dht::partition_range& range,
                         const query::partition_slice& slice,
                         const io_priority_class& pc,
                         reader_resource_tracker resource_tracker,
                         streamed_mutation::forwarding fwd,
                         mutation_reader::forwarding fwd_mr) {
    return make_flat_mutation_reader<sstable_mutation_reader>(
        shared_from_this(), std::move(schema), range, slice, pc, std::move(resource_tracker), fwd, fwd_mr);
}

row_consumer::proceed
mp_row_consumer::push_ready_fragments_with_ready_set() {
    // We're merging two streams here, one is _range_tombstones
    // and the other is the main fragment stream represented by
    // _ready and _out_of_range (which means end of stream).

    while (!_reader->is_buffer_full()) {
        auto mfo = _range_tombstones.get_next(*_ready);
        if (mfo) {
            _reader->push_mutation_fragment(std::move(*mfo));
        } else {
            _reader->push_mutation_fragment(std::move(*_ready));
            _ready = {};
            return proceed(!_reader->is_buffer_full());
        }
    }
    return proceed::no;
}

row_consumer::proceed
mp_row_consumer::push_ready_fragments_out_of_range() {
    // Emit all range tombstones relevant to the current forwarding range first.
    while (!_reader->is_buffer_full()) {
        auto mfo = _range_tombstones.get_next(_fwd_end);
        if (!mfo) {
            _reader->on_end_of_stream();
            break;
        }
        _reader->push_mutation_fragment(std::move(*mfo));
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

stdx::optional<position_in_partition_view> mp_row_consumer::fast_forward_to(position_range r) {
    sstlog.trace("mp_row_consumer {}: fast_forward_to({})", this, r);
    _out_of_range = _is_mutation_end;
    _fwd_end = std::move(r).end();

    _range_tombstones.forward_to(r.start());

    _ck_ranges_walker->trim_front(std::move(r).start());
    if (_ck_ranges_walker->out_of_range()) {
        _out_of_range = true;
        _ready = {};
        sstlog.trace("mp_row_consumer {}: no more ranges", this);
        return { };
    }

    auto start = _ck_ranges_walker->lower_bound();

    if (_ready && !_ready->relevant_for_range(*_schema, start)) {
        _ready = {};
    }

    if (_in_progress) {
        advance_to(*_in_progress);
        if (!_skip_in_progress) {
            sstlog.trace("mp_row_consumer {}: _in_progress in range", this);
            return { };
        }
    }

    if (_out_of_range) {
        sstlog.trace("mp_row_consumer {}: _out_of_range=true", this);
        return { };
    }

    position_in_partition::less_compare less(*_schema);
    if (!less(start, _fwd_end)) {
        _out_of_range = true;
        sstlog.trace("mp_row_consumer {}: no overlap with restrictions", this);
        return { };
    }

    sstlog.trace("mp_row_consumer {}: advance_context({})", this, start);
    _last_lower_bound_counter = _ck_ranges_walker->lower_bound_change_counter();
    return start;
}

stdx::optional<position_in_partition_view> mp_row_consumer::maybe_skip() {
    if (!needs_skip()) {
        return { };
    }
    _last_lower_bound_counter = _ck_ranges_walker->lower_bound_change_counter();
    sstlog.trace("mp_row_consumer {}: advance_context({})", this, _ck_ranges_walker->lower_bound());
    return _ck_ranges_walker->lower_bound();
}

}

/*
 * Copyright (C) 2018 ScyllaDB
 *
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


#include <variant>
#include "flat_mutation_reader.hh"
#include "timestamp.hh"
#include "gc_clock.hh"
#include "mutation_fragment.hh"
#include "schema.hh"
#include "row.hh"
#include "clustering_ranges_walker.hh"
#include "utils/data_input.hh"
#include "utils/overloaded_functor.hh"
#include "liveness_info.hh"
#include "mutation_fragment_filter.hh"
#include "types.hh"
#include "keys.hh"
#include "clustering_bounds_comparator.hh"
#include "range_tombstone.hh"

namespace sstables {

static inline bytes_view pop_back(std::vector<bytes_view>& vec) {
    auto b = std::move(vec.back());
    vec.pop_back();
    return b;
}

class mp_row_consumer_reader : public flat_mutation_reader::impl {
    friend class mp_row_consumer_k_l;
    friend class mp_row_consumer_m;
public:
    mp_row_consumer_reader(schema_ptr s) : impl(std::move(s)) {}
    virtual void on_end_of_stream() = 0;
};

struct new_mutation {
    partition_key key;
    tombstone tomb;
};

inline atomic_cell make_atomic_cell(const abstract_type& type,
                                    api::timestamp_type timestamp,
                                    bytes_view value,
                                    gc_clock::duration ttl,
                                    gc_clock::time_point expiration,
                                    atomic_cell::collection_member cm) {
    if (ttl != gc_clock::duration::zero()) {
        return atomic_cell::make_live(type, timestamp, value, expiration, ttl, cm);
    } else {
        return atomic_cell::make_live(type, timestamp, value, cm);
    }
}

atomic_cell make_counter_cell(api::timestamp_type timestamp, bytes_view value);

class mp_row_consumer_k_l : public row_consumer {
private:
    mp_row_consumer_reader* _reader;
    schema_ptr _schema;
    const query::partition_slice& _slice;
    bool _out_of_range = false;
    std::optional<query::clustering_key_filter_ranges> _ck_ranges;
    std::optional<clustering_ranges_walker> _ck_ranges_walker;

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

    std::optional<new_mutation> _mutation;
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

    // See #2986
    bool _treat_non_compound_rt_as_compound;
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

    proceed push_ready_fragments_out_of_range() {
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
    proceed push_ready_fragments_with_ready_set() {
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

        sstlog.trace("mp_row_consumer_k_l {}: advance_to({}) => out_of_range={}, skip_in_progress={}", this, pos, _out_of_range, _skip_in_progress);
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

        sstlog.trace("mp_row_consumer_k_l {}: advance_to({}) => out_of_range={}, skip_in_progress={}", this, rt, _out_of_range, _skip_in_progress);
    }

    void advance_to(const mutation_fragment& mf) {
        if (mf.is_range_tombstone()) {
            advance_to(mf.as_range_tombstone());
        } else {
            advance_to(mf.position());
        }
    }

    void set_up_ck_ranges(const partition_key& pk) {
        sstlog.trace("mp_row_consumer_k_l {}: set_up_ck_ranges({})", this, pk);
        _ck_ranges = query::clustering_key_filter_ranges::get_ranges(*_schema, _slice, pk);
        _ck_ranges_walker.emplace(*_schema, _ck_ranges->ranges(), _schema->has_static_columns());
        _last_lower_bound_counter = 0;
        _fwd_end = _fwd ? position_in_partition::before_all_clustered_rows() : position_in_partition::after_all_clustered_rows();
        _out_of_range = false;
        _range_tombstones.reset();
        _first_row_encountered = false;
    }
public:
    mutation_opt mut;

    mp_row_consumer_k_l(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        const query::partition_slice& slice,
                        const io_priority_class& pc,
                        reader_resource_tracker resource_tracker,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : row_consumer(std::move(resource_tracker), pc)
        , _reader(reader)
        , _schema(schema)
        , _slice(slice)
        , _fwd(fwd)
        , _range_tombstones(*_schema)
        , _treat_non_compound_rt_as_compound(!sst->has_correct_non_compound_range_tombstones())
    { }

    mp_row_consumer_k_l(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        const io_priority_class& pc,
                        reader_resource_tracker resource_tracker,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : mp_row_consumer_k_l(reader, schema, schema->full_slice(), pc, std::move(resource_tracker), fwd, sst) { }

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
        _skip_in_progress = false;
        set_up_ck_ranges(pk);
    }

    proceed flush() {
        sstlog.trace("mp_row_consumer_k_l {}: flush(in_progress={}, ready={}, skip={})", this, _in_progress, _ready, _skip_in_progress);
        flush_pending_collection(*_schema);
        // If _ready is already set we have a bug: get_mutation_fragment()
        // was not called, and below we will lose one clustering row!
        assert(!_ready);
        if (!_skip_in_progress) {
            _ready = std::exchange(_in_progress, { });
            return push_ready_fragments_with_ready_set();
        } else {
            _in_progress = { };
            _ready = { };
            _skip_in_progress = false;
            return proceed::yes;
        }
    }

    proceed flush_if_needed(range_tombstone&& rt) {
        sstlog.trace("mp_row_consumer_k_l {}: flush_if_needed(in_progress={}, ready={}, skip={})", this, _in_progress, _ready, _skip_in_progress);
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
        sstlog.trace("mp_row_consumer_k_l {}: flush_if_needed({})", this, pos);

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

    template<typename CreateCell>
    //requires requires(CreateCell create_cell, column col) {
    //    { create_cell(col) } -> void;
    //}
    proceed do_consume_cell(bytes_view col_name, int64_t timestamp, int32_t ttl, int32_t expiration, CreateCell&& create_cell) {
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

    virtual proceed consume_cell(bytes_view col_name, bytes_view value, int64_t timestamp, int32_t ttl, int32_t expiration) override {
        return do_consume_cell(col_name, timestamp, ttl, expiration, [&] (auto&& col) {
            bool is_multi_cell = col.collection_extra_data.size();
            if (is_multi_cell != col.cdef->is_multi_cell()) {
                return;
            }
            if (is_multi_cell) {
                auto ctype = static_pointer_cast<const collection_type_impl>(col.cdef->type);
                auto ac = make_atomic_cell(*ctype->value_comparator(),
                                           api::timestamp_type(timestamp),
                                           value,
                                           gc_clock::duration(ttl),
                                           gc_clock::time_point(gc_clock::duration(expiration)),
                                           atomic_cell::collection_member::yes);
                update_pending_collection(col.cdef, to_bytes(col.collection_extra_data), std::move(ac));
                return;
            }

            auto ac = make_atomic_cell(*col.cdef->type,
                                       api::timestamp_type(timestamp),
                                       value,
                                       gc_clock::duration(ttl),
                                       gc_clock::time_point(gc_clock::duration(expiration)),
                                       atomic_cell::collection_member::no);
            if (col.is_static) {
                _in_progress->as_mutable_static_row().set_cell(*(col.cdef), std::move(ac));
                return;
            }
            _in_progress->as_mutable_clustering_row().set_cell(*(col.cdef), atomic_cell_or_collection(std::move(ac)));
        });
    }

    virtual proceed consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
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
        auto compound = _schema->is_compound() || _treat_non_compound_rt_as_compound;
        auto start = composite_view(column::fix_static_name(*_schema, start_col), compound).explode();

        // Note how this is slightly different from the check in is_collection. Collection tombstones
        // do not have extra data.
        //
        // Still, it is enough to check if we're dealing with a collection, since any other tombstone
        // won't have a full clustering prefix (otherwise it isn't a range)
        if (start.size() <= _schema->clustering_key_size()) {
            auto start_ck = clustering_key_prefix::from_exploded_view(start);
            auto start_kind = compound ? start_marker_to_bound_kind(start_col) : bound_kind::incl_start;
            auto end = clustering_key_prefix::from_exploded_view(composite_view(column::fix_static_name(*_schema, end_col), compound).explode());
            auto end_kind = compound ? end_marker_to_bound_kind(end_col) : bound_kind::incl_end;
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

    std::optional<new_mutation> get_mutation() {
        return std::exchange(_mutation, { });
    }

    // Pushes ready fragments into the streamed_mutation's buffer.
    // Tries to push as much as possible, but respects buffer limits.
    // Sets streamed_mutation::_end_of_range when there are no more fragments for the query range.
    // Returns information whether the parser should continue to parse more
    // input and produce more fragments or we have collected enough and should yield.
    proceed push_ready_fragments() {
        if (_ready) {
            return push_ready_fragments_with_ready_set();
        }

        if (_out_of_range) {
            return push_ready_fragments_out_of_range();
        }

        return proceed::yes;
    }

    virtual void reset(indexable_element el) override {
        sstlog.trace("mp_row_consumer_k_l {}: reset({})", this, static_cast<int>(el));
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
    std::optional<position_in_partition_view> fast_forward_to(position_range r, db::timeout_clock::time_point timeout) {
        sstlog.trace("mp_row_consumer_k_l {}: fast_forward_to({})", this, r);
        _out_of_range = _is_mutation_end;
        _fwd_end = std::move(r).end();

        _range_tombstones.forward_to(r.start());

        _ck_ranges_walker->trim_front(std::move(r).start());
        if (_ck_ranges_walker->out_of_range()) {
            _out_of_range = true;
            _ready = {};
            sstlog.trace("mp_row_consumer_k_l {}: no more ranges", this);
            return { };
        }

        auto start = _ck_ranges_walker->lower_bound();

        if (_ready && !_ready->relevant_for_range(*_schema, start)) {
            _ready = {};
        }

        if (_in_progress) {
            advance_to(*_in_progress);
            if (!_skip_in_progress) {
                sstlog.trace("mp_row_consumer_k_l {}: _in_progress in range", this);
                return { };
            }
        }

        if (_out_of_range) {
            sstlog.trace("mp_row_consumer_k_l {}: _out_of_range=true", this);
            return { };
        }

        position_in_partition::less_compare less(*_schema);
        if (!less(start, _fwd_end)) {
            _out_of_range = true;
            sstlog.trace("mp_row_consumer_k_l {}: no overlap with restrictions", this);
            return { };
        }

        sstlog.trace("mp_row_consumer_k_l {}: advance_context({})", this, start);
        _last_lower_bound_counter = _ck_ranges_walker->lower_bound_change_counter();
        return start;
    }

    bool needs_skip() const {
        return (_skip_in_progress || !_in_progress)
               && _last_lower_bound_counter != _ck_ranges_walker->lower_bound_change_counter();
    }

    // Tries to fast forward the consuming context to the next position.
    // Must be called outside consuming context.
    std::optional<position_in_partition_view> maybe_skip() {
        if (!needs_skip()) {
            return { };
        }
        _last_lower_bound_counter = _ck_ranges_walker->lower_bound_change_counter();
        sstlog.trace("mp_row_consumer_k_l {}: advance_context({})", this, _ck_ranges_walker->lower_bound());
        return _ck_ranges_walker->lower_bound();
    }
};

class mp_row_consumer_m : public consumer_m {
    mp_row_consumer_reader* _reader;
    schema_ptr _schema;
    const query::partition_slice& _slice;
    std::optional<mutation_fragment_filter> _mf_filter;

    std::optional<new_mutation> _mutation;
    bool _is_mutation_end = true;
    streamed_mutation::forwarding _fwd;

    std::optional<clustering_row> _in_progress_row;
    std::optional<range_tombstone> _stored_tombstone;
    static_row _in_progress_static_row;
    bool _inside_static_row = false;

    struct cell {
        column_id id;
        atomic_cell_or_collection val;
    };
    std::vector<cell> _cells;
    collection_type_impl::mutation _cm;

    struct range_tombstone_start {
        clustering_key_prefix ck;
        bound_kind kind;
        tombstone tomb;

        position_in_partition_view position() {
            return position_in_partition_view(position_in_partition_view::range_tag_t{}, bound_view(ck, kind));
        }
    };

    inline friend std::ostream& operator<<(std::ostream& o, const sstables::mp_row_consumer_m::range_tombstone_start& rt_start) {
        o << "{ clustering: " << rt_start.ck
          << ", kind: " << rt_start.kind
          << ", tombstone: " << rt_start.tomb << " }";
        return o;
    }

    std::optional<range_tombstone_start> _opened_range_tombstone;

    void consume_range_tombstone_start(clustering_key_prefix ck, bound_kind k, tombstone t) {
        if (_opened_range_tombstone) {
            throw sstables::malformed_sstable_exception(
                    format("Range tombstones have to be disjoint: current opened range tombstone {}, new tombstone {}",
                           *_opened_range_tombstone, t));
        }
        _opened_range_tombstone = {std::move(ck), k, std::move(t)};
    }

    proceed consume_range_tombstone_end(clustering_key_prefix ck, bound_kind k, tombstone t) {
        if (!_opened_range_tombstone) {
            throw sstables::malformed_sstable_exception(
                    format("Closing range tombstone that wasn't opened: clustering {}, kind {}, tombstone {}",
                           ck, k, t));
        }
        if (_opened_range_tombstone->tomb.compare(t) != 0) {
            throw sstables::malformed_sstable_exception(
                    format("Range tombstone with ck {} and two different tombstones at ends: {}, {}",
                           ck, _opened_range_tombstone->tomb, t));
        }


        auto rt = range_tombstone {std::move(_opened_range_tombstone->ck),
                            _opened_range_tombstone->kind,
                            std::move(ck),
                            k,
                            std::move(t)};
        _opened_range_tombstone.reset();
        return maybe_push_range_tombstone(std::move(rt));
    }

    const column_definition& get_column_definition(std::optional<column_id> column_id) {
        auto column_type = _inside_static_row ? column_kind::static_column : column_kind::regular_column;
        return _schema->column_at(column_type, *column_id);
    }

    inline proceed maybe_push_range_tombstone(range_tombstone&& rt) {
        const auto action = _mf_filter->apply(rt);
        switch (action) {
        case mutation_fragment_filter::result::emit:
            _reader->push_mutation_fragment(std::move(rt));
            break;
        case mutation_fragment_filter::result::ignore:
            if (_mf_filter->is_current_range_changed()) {
                return proceed::no;
            }
            break;
        case mutation_fragment_filter::result::store_and_finish:
            _stored_tombstone = std::move(rt);
            _reader->on_end_of_stream();
            return proceed::no;
        }

        return proceed(!_reader->is_buffer_full());
    }

    inline void reset_for_new_partition() {
        _is_mutation_end = true;
        _in_progress_row.reset();
        _stored_tombstone.reset();
        _mf_filter.reset();
        _opened_range_tombstone.reset();
    }

    void check_schema_mismatch(const column_translation::column_info& column_info, const column_definition& column_def) {
        if (column_info.schema_mismatch) {
            throw malformed_sstable_exception(
                    format("{} definition in serialization header does not match schema. Expected {} but got {}.",
                        column_def.name(),
                        column_def.type->name(),
                        column_info.type->name()));
        }
    }

    void check_column_missing_in_current_schema(const column_translation::column_info& column_info,
                                                api::timestamp_type timestamp) {
        if (!column_info.id) {
            sstring name = sstring(to_sstring_view(*column_info.name));
            auto it = _schema->dropped_columns().find(name);
            if (it == _schema->dropped_columns().end() || timestamp > it->second.timestamp) {
                throw malformed_sstable_exception(format("Column {} missing in current schema.", name));
            }
        }
    }

public:

    /*
     * In m format, RTs are represented as separate start and end bounds,
     * so setting/resetting RT start is needed so that we could skip using index.
     * For this, the following methods need to be defined:
     *
     * void set_range_tombstone_start(clustering_key_prefix, bound_kind, tombstone);
     * void reset_range_tombstone_start();
     */
    constexpr static bool is_setting_range_tombstone_start_supported = true;

    mp_row_consumer_m(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        const query::partition_slice& slice,
                        const io_priority_class& pc,
                            reader_resource_tracker resource_tracker,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : consumer_m(resource_tracker, pc)
        , _reader(reader)
        , _schema(schema)
        , _slice(slice)
        , _fwd(fwd)
    {
        _cells.reserve(std::max(_schema->static_columns_count(), _schema->regular_columns_count()));
    }

    mp_row_consumer_m(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        const io_priority_class& pc,
                            reader_resource_tracker resource_tracker,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
    : mp_row_consumer_m(reader, schema, schema->full_slice(), pc, std::move(resource_tracker), fwd, sst)
    { }

    virtual ~mp_row_consumer_m() {}

    proceed push_ready_fragments() {
        if (!_mf_filter || _mf_filter->out_of_range()) {
            _reader->on_end_of_stream();
            return proceed::no;
        }

        auto maybe_push = [this] (auto&& mfopt) {
            if (mfopt) {
                switch (_mf_filter->apply(*mfopt)) {
                case mutation_fragment_filter::result::emit:
                    _reader->push_mutation_fragment(*std::exchange(mfopt, {}));
                    break;
                case mutation_fragment_filter::result::ignore:
                    mfopt.reset();
                    if (_mf_filter->is_current_range_changed()) {
                       return true;
                    }
                    break;
                case mutation_fragment_filter::result::store_and_finish:
                    _reader->on_end_of_stream();
                    return true;
                }
            }
            return false;
        };

        return maybe_push(_stored_tombstone) ? proceed::no : proceed::yes;
    }

    std::optional<position_in_partition_view> maybe_skip() {
        if (!_mf_filter) {
            return {};
        }
        return _mf_filter->maybe_skip();
    }

    bool is_mutation_end() const {
        return _is_mutation_end;
    }

    void setup_for_partition(const partition_key& pk) {
        sstlog.trace("mp_row_consumer_m {}: setup_for_partition({})", this, pk);
        _is_mutation_end = false;
        _mf_filter.emplace(*_schema, _slice, pk, _fwd);
    }

    std::optional<new_mutation> get_mutation() {
        return std::exchange(_mutation, { });
    }

    std::optional<position_in_partition_view> fast_forward_to(position_range r, db::timeout_clock::time_point) {
        if (!_mf_filter) {
            return {};
        }
        auto skip = _mf_filter->fast_forward_to(std::move(r));
        if (skip) {
            position_in_partition::less_compare less(*_schema);
            // No need to skip using index if stored fragments are after the start of the range
            if (_in_progress_row && !less(_in_progress_row->position(), *skip)) {
                return {};
            }
            if (_stored_tombstone && !less(_stored_tombstone->position(), *skip)) {
                return {};
            }
        }
        return skip;
    }

    /*
     * Sets the range tombstone start. Overwrites the currently set RT start if any.
     * Used for skipping through wide partitions using index when the data block
     * skipped to starts in the middle of an opened range tombstone.
     */
    void set_range_tombstone_start(clustering_key_prefix ck, bound_kind k, tombstone t) {
        _opened_range_tombstone = {std::move(ck), k, std::move(t)};
    }

    /*
     * Resets the previously set range tombstone start if any.
     */
    void reset_range_tombstone_start() {
        _opened_range_tombstone.reset();
    }

    virtual proceed consume_partition_start(sstables::key_view key, sstables::deletion_time deltime) override {
        sstlog.trace("mp_row_consumer_m {}: consume_partition_start(deltime=({}, {})), _is_mutation_end={}", this,
            deltime.local_deletion_time, deltime.marked_for_delete_at, _is_mutation_end);
        if (!_is_mutation_end) {
            return proceed::yes;
        }
        _mutation = new_mutation{partition_key::from_exploded(key.explode(*_schema)), tombstone(deltime)};
        setup_for_partition(_mutation->key);
        return proceed::no;
    }

    virtual consumer_m::row_processing_result consume_row_start(const std::vector<temporary_buffer<char>>& ecp) override {
        auto key = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const temporary_buffer<char>& b) { return to_bytes_view(b); }));

        sstlog.trace("mp_row_consumer_m {}: consume_row_start({})", this, key);

        // enagaged _in_progress_row means we have already split around this key.
        if (_opened_range_tombstone && !_in_progress_row) {
            // We have an opened range tombstone which means that the current row is spanned by that RT.
            auto ck = key;
            bool was_non_full_key = clustering_key::make_full(*_schema, ck);
            auto end_kind = was_non_full_key ? bound_kind::excl_end : bound_kind::incl_end;
            assert(!_stored_tombstone);
            auto rt = range_tombstone(std::move(_opened_range_tombstone->ck),
                _opened_range_tombstone->kind,
                ck,
                end_kind,
                _opened_range_tombstone->tomb);
            sstlog.trace("mp_row_consumer_m {}: push({})", this, rt);
            _opened_range_tombstone->ck = std::move(ck);
            _opened_range_tombstone->kind = was_non_full_key ? bound_kind::incl_start : bound_kind::excl_start;

            if (maybe_push_range_tombstone(std::move(rt)) == proceed::no) {
                _in_progress_row.emplace(std::move(key));
                return consumer_m::row_processing_result::retry_later;
            }
        }

        _in_progress_row.emplace(std::move(key));

        switch (_mf_filter->apply(_in_progress_row->position())) {
        case mutation_fragment_filter::result::emit:
            sstlog.trace("mp_row_consumer_m {}: emit", this);
            return consumer_m::row_processing_result::do_proceed;
        case mutation_fragment_filter::result::ignore:
            sstlog.trace("mp_row_consumer_m {}: ignore", this);
            if (_mf_filter->is_current_range_changed()) {
                return consumer_m::row_processing_result::retry_later;
            } else {
                _in_progress_row.reset();
                return consumer_m::row_processing_result::skip_row;
            }
        case mutation_fragment_filter::result::store_and_finish:
            sstlog.trace("mp_row_consumer_m {}: store_and_finish", this);
            _reader->on_end_of_stream();
            return consumer_m::row_processing_result::retry_later;
        }
        abort();
    }

    virtual proceed consume_row_marker_and_tombstone(
            const liveness_info& info, tombstone tomb, tombstone shadowable_tomb) override {
        sstlog.trace("mp_row_consumer_m {}: consume_row_marker_and_tombstone({}, {}, {}), key={}",
            this, info.to_row_marker(), tomb, shadowable_tomb, _in_progress_row->position());
        _in_progress_row->apply(info.to_row_marker());
        _in_progress_row->apply(tomb);
        if (shadowable_tomb) {
            _in_progress_row->apply(shadowable_tombstone{shadowable_tomb});
        }
        return proceed::yes;
    }

    virtual consumer_m::row_processing_result consume_static_row_start() override {
        sstlog.trace("mp_row_consumer_m {}: consume_static_row_start()", this);
        _inside_static_row = true;
        _in_progress_static_row = static_row();
        return consumer_m::row_processing_result::do_proceed;
    }

    virtual proceed consume_column(const column_translation::column_info& column_info,
                                   bytes_view cell_path,
                                   bytes_view value,
                                   api::timestamp_type timestamp,
                                   gc_clock::duration ttl,
                                   gc_clock::time_point local_deletion_time,
                                   bool is_deleted) override {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_column(id={}, path={}, value={}, ts={}, ttl={}, del_time={}, deleted={})", this,
            column_id, cell_path, value, timestamp, ttl.count(), local_deletion_time.time_since_epoch().count(), is_deleted);
        check_column_missing_in_current_schema(column_info, timestamp);
        if (!column_id) {
            return proceed::yes;
        }
        const column_definition& column_def = get_column_definition(column_id);
        if (timestamp <= column_def.dropped_at()) {
            return proceed::yes;
        }
        check_schema_mismatch(column_info, column_def);
        if (column_def.is_multi_cell()) {
            auto ctype = static_pointer_cast<const collection_type_impl>(column_def.type);
            auto ac = is_deleted ? atomic_cell::make_dead(timestamp, local_deletion_time)
                                 : make_atomic_cell(*ctype->value_comparator(),
                                                    timestamp,
                                                    value,
                                                    ttl,
                                                    local_deletion_time,
                                                    atomic_cell::collection_member::yes);
            _cm.cells.emplace_back(to_bytes(cell_path), std::move(ac));
        } else {
            auto ac = is_deleted ? atomic_cell::make_dead(timestamp, local_deletion_time)
                                 : make_atomic_cell(*column_def.type, timestamp, value, ttl, local_deletion_time,
                                       atomic_cell::collection_member::no);
            _cells.push_back({*column_id, atomic_cell_or_collection(std::move(ac))});
        }
        return proceed::yes;
    }

    virtual proceed consume_complex_column_start(const sstables::column_translation::column_info& column_info,
                                                 tombstone tomb) override {
        sstlog.trace("mp_row_consumer_m {}: consume_complex_column_start({}, {})", this, column_info.id, tomb);
        _cm.tomb = tomb;
        _cm.cells.clear();
        return proceed::yes;
    }

    virtual proceed consume_complex_column_end(const sstables::column_translation::column_info& column_info) override {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_complex_column_end({})", this, column_id);
        if (_cm.tomb) {
            check_column_missing_in_current_schema(column_info, _cm.tomb.timestamp);
        }
        if (column_id) {
            const column_definition& column_def = get_column_definition(column_id);
            if (!_cm.cells.empty() || (_cm.tomb && _cm.tomb.timestamp > column_def.dropped_at())) {
                check_schema_mismatch(column_info, column_def);
                auto ctype = static_pointer_cast<const collection_type_impl>(column_def.type);
                auto ac = atomic_cell_or_collection::from_collection_mutation(ctype->serialize_mutation_form(_cm));
                _cells.push_back({column_def.id, atomic_cell_or_collection(std::move(ac))});
            }
        }
        _cm.tomb = {};
        _cm.cells.clear();
        return proceed::yes;
    }

    virtual proceed consume_counter_column(const column_translation::column_info& column_info,
                                           bytes_view value,
                                           api::timestamp_type timestamp) override {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_counter_column({}, {}, {})", this, column_id, value, timestamp);
        check_column_missing_in_current_schema(column_info, timestamp);
        if (!column_id) {
            return proceed::yes;
        }
        const column_definition& column_def = get_column_definition(column_id);
        if (timestamp <= column_def.dropped_at()) {
            return proceed::yes;
        }
        check_schema_mismatch(column_info, column_def);
        auto ac = make_counter_cell(timestamp, value);
        _cells.push_back({*column_id, atomic_cell_or_collection(std::move(ac))});
        return proceed::yes;
    }

    virtual proceed consume_range_tombstone(const std::vector<temporary_buffer<char>>& ecp,
                                            bound_kind kind,
                                            tombstone tomb) override {
        auto ck = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const temporary_buffer<char>& b) { return to_bytes_view(b); }));
        if (kind == bound_kind::incl_start || kind == bound_kind::excl_start) {
            consume_range_tombstone_start(std::move(ck), kind, std::move(tomb));
            return proceed(!_reader->is_buffer_full());
        } else { // *_end kind
            return consume_range_tombstone_end(std::move(ck), kind, std::move(tomb));
        }
    }

    virtual proceed consume_range_tombstone(const std::vector<temporary_buffer<char>>& ecp,
                                            sstables::bound_kind_m kind,
                                            tombstone end_tombstone,
                                            tombstone start_tombstone) override {
        auto result = proceed::yes;
        auto ck = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const temporary_buffer<char>& b) { return to_bytes_view(b); }));
        switch (kind) {
        case bound_kind_m::incl_end_excl_start:
            result = consume_range_tombstone_end(ck, bound_kind::incl_end, std::move(end_tombstone));
            consume_range_tombstone_start(std::move(ck), bound_kind::excl_start, std::move(start_tombstone));
            break;
        case bound_kind_m::excl_end_incl_start:
            result = consume_range_tombstone_end(ck, bound_kind::excl_end, std::move(end_tombstone));
            consume_range_tombstone_start(std::move(ck), bound_kind::incl_start, std::move(start_tombstone));
            break;
        default:
            throw sstables::malformed_sstable_exception(
                    format("Corrupted range tombstone: invalid boundary type {}", kind));
        }

        return result;
    }

    virtual proceed consume_row_end() override {
        auto fill_cells = [this] (column_kind kind, row& cells) {
            auto max_id = boost::max_element(_cells, [](auto &&a, auto &&b) {
                return a.id < b.id;
            });
            cells.reserve(max_id->id);
            for (auto &&c : _cells) {
                cells.apply(_schema->column_at(kind, c.id), std::move(c.val));
            }
            _cells.clear();
        };

        if (_inside_static_row) {
            fill_cells(column_kind::static_column, _in_progress_static_row.cells());
            sstlog.trace("mp_row_consumer_m {}: consume_row_end(_in_progress_static_row={})", this, _in_progress_static_row);
            _inside_static_row = false;
            if (!_in_progress_static_row.empty()) {
                auto action = _mf_filter->apply(_in_progress_static_row);
                switch (action) {
                case mutation_fragment_filter::result::emit:
                    _reader->push_mutation_fragment(std::move(_in_progress_static_row));
                    break;
                case mutation_fragment_filter::result::ignore:
                    break;
                case mutation_fragment_filter::result::store_and_finish:
                    // static row is always either emited or ignored.
                    throw runtime_exception("We should never need to store static row");
                }
            }
        } else {
            if (!_cells.empty()) {
                fill_cells(column_kind::regular_column, _in_progress_row->cells());
            }
            _reader->push_mutation_fragment(*std::exchange(_in_progress_row, {}));
        }

        return proceed(!_reader->is_buffer_full());
    }

    virtual void on_end_of_stream() override {
        if (_opened_range_tombstone) {
            if (!_mf_filter || _mf_filter->out_of_range()) {
                throw sstables::malformed_sstable_exception("Unclosed range tombstone.");
            }
            auto range_end = _mf_filter->uppermost_bound();
            position_in_partition::less_compare less(*_schema);
            auto start_pos = position_in_partition_view(position_in_partition_view::range_tag_t{},
                                                        bound_view(_opened_range_tombstone->ck, _opened_range_tombstone->kind));
            if (!less(range_end, start_pos)) {
                auto end_bound = range_end.as_end_bound_view();
                auto rt = range_tombstone {std::move(_opened_range_tombstone->ck),
                                           _opened_range_tombstone->kind,
                                           end_bound.prefix(),
                                           end_bound.kind(),
                                           _opened_range_tombstone->tomb};
                _opened_range_tombstone.reset();
                _reader->push_mutation_fragment(std::move(rt));
            }
        }
        consume_partition_end();
    }

    virtual proceed consume_partition_end() override {
        sstlog.trace("mp_row_consumer_m {}: consume_partition_end()", this);
        reset_for_new_partition();
        return proceed::no;
    }

    virtual void reset(sstables::indexable_element el) override {
        sstlog.trace("mp_row_consumer_m {}: reset({})", this, static_cast<int>(el));
        if (el == indexable_element::partition) {
            reset_for_new_partition();
        } else {
            _in_progress_row.reset();
            _is_mutation_end = false;
        }
    }
};

}

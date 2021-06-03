/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "sstables/kl/reader.hh"
#include "sstables/kl/reader_impl.hh"
#include "sstables/sstable_mutation_reader.hh"
#include "concrete_types.hh"

namespace sstables {
namespace kl {

static inline bytes_view pop_back(std::vector<bytes_view>& vec) {
    auto b = std::move(vec.back());
    vec.pop_back();
    return b;
}

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
        bool is_collection(const schema& s) const {
            auto expected_normal = s.clustering_key_size() + 1;
            // Note that we can have less than the expected. That is the case for
            // incomplete prefixes, for instance.
            if (clustering.size() <= expected_normal) {
                return false;
            } else if (clustering.size() == (expected_normal + 1)) {
                return true;
            }
            throw malformed_sstable_exception(format("Found {:d} clustering elements in column name. Was not expecting that!", clustering.size()));
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
        collection_mutation_description cm;

        // We need to get a copy of the prefix here, because the outer object may be short lived.
        collection_mutation(const column_definition *cdef)
            : _cdef(cdef) { }

        collection_mutation() : _cdef(nullptr) {}

        bool is_new_collection(const column_definition *c) const {
            if (!_cdef || ((_cdef->id != c->id) || (_cdef->kind != c->kind))) {
                return true;
            }
            return false;
        };

        void flush(const schema& s, mutation_fragment& mf) {
            if (!_cdef) {
                return;
            }
            auto ac = atomic_cell_or_collection::from_collection_mutation(cm.serialize(*_cdef->type));
            if (_cdef->is_static()) {
                mf.mutate_as_static_row(s, [&] (static_row& sr) mutable {
                    sr.set_cell(*_cdef, std::move(ac));
                });
            } else {
                mf.mutate_as_clustering_row(s, [&] (clustering_row& cr) {
                    cr.set_cell(*_cdef, std::move(ac));
                });
            }
        }
    };
    std::optional<collection_mutation> _pending_collection = {};

    collection_mutation& pending_collection(const column_definition *cdef) {
        assert(cdef->is_multi_cell() && "frozen set should behave like a cell\n");
        if (!_pending_collection || _pending_collection->is_new_collection(cdef)) {
            flush_pending_collection(*_schema);
            _pending_collection = collection_mutation(cdef);
        }
        return *_pending_collection;
    }

    proceed push_ready_fragments_out_of_range() {
        // Emit all range tombstones relevant to the current forwarding range first.
        while (!_reader->is_buffer_full()) {
            auto mfo = _range_tombstones.get_next(_fwd_end);
            if (!mfo) {
                if (!_reader->_partition_finished) {
                    _reader->on_out_of_clustering_range();
                }
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

        sstlog.trace("mp_row_consumer_k_l {}: advance_to({}) => out_of_range={}, skip_in_progress={}", fmt::ptr(this), pos, _out_of_range, _skip_in_progress);
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

        sstlog.trace("mp_row_consumer_k_l {}: advance_to({}) => out_of_range={}, skip_in_progress={}", fmt::ptr(this), rt, _out_of_range, _skip_in_progress);
    }

    void advance_to(const mutation_fragment& mf) {
        if (mf.is_range_tombstone()) {
            advance_to(mf.as_range_tombstone());
        } else {
            advance_to(mf.position());
        }
    }

    void set_up_ck_ranges(const partition_key& pk) {
        sstlog.trace("mp_row_consumer_k_l {}: set_up_ck_ranges({})", fmt::ptr(this), pk);
        _ck_ranges = query::clustering_key_filter_ranges::get_ranges(*_schema, _slice, pk);
        _ck_ranges_walker.emplace(*_schema, _ck_ranges->ranges(), _schema->has_static_columns());
        _last_lower_bound_counter = 0;
        _fwd_end = _fwd ? position_in_partition::before_all_clustered_rows() : position_in_partition::after_all_clustered_rows();
        _out_of_range = false;
        _range_tombstones.reset();
        _ready = {};
        _first_row_encountered = false;
    }
public:
    mutation_opt mut;

    mp_row_consumer_k_l(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        const query::partition_slice& slice,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : row_consumer(std::move(permit), std::move(trace_state), pc)
        , _reader(reader)
        , _schema(schema)
        , _slice(slice)
        , _fwd(fwd)
        , _range_tombstones(*_schema, this->permit())
        , _treat_non_compound_rt_as_compound(!sst->has_correct_non_compound_range_tombstones())
    { }

    mp_row_consumer_k_l(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : mp_row_consumer_k_l(reader, schema, std::move(permit), schema->full_slice(), pc, std::move(trace_state), fwd, sst) { }

    virtual proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        if (!_is_mutation_end) {
            return proceed::yes;
        }
        auto pk = partition_key::from_exploded(key.explode(*_schema));
        setup_for_partition(pk);
        auto dk = dht::decorate_key(*_schema, pk);
        _reader->on_next_partition(std::move(dk), tombstone(deltime));
        return proceed::yes;
    }

    void setup_for_partition(const partition_key& pk) {
        _is_mutation_end = false;
        _skip_in_progress = false;
        set_up_ck_ranges(pk);
    }

    proceed flush() {
        sstlog.trace("mp_row_consumer_k_l {}: flush(in_progress={}, ready={}, skip={})", fmt::ptr(this),
            _in_progress ? std::optional<mutation_fragment::printer>(std::in_place, *_schema, *_in_progress) : std::optional<mutation_fragment::printer>(),
            _ready ? std::optional<mutation_fragment::printer>(std::in_place, *_schema, *_ready) : std::optional<mutation_fragment::printer>(),
            _skip_in_progress);
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
        sstlog.trace("mp_row_consumer_k_l {}: flush_if_needed(in_progress={}, ready={}, skip={})", fmt::ptr(this),
            _in_progress ? std::optional<mutation_fragment::printer>(std::in_place, *_schema, *_in_progress) : std::optional<mutation_fragment::printer>(),
            _ready ? std::optional<mutation_fragment::printer>(std::in_place, *_schema, *_ready) : std::optional<mutation_fragment::printer>(),
            _skip_in_progress);
        proceed ret = proceed::yes;
        if (_in_progress) {
            ret = flush();
        }
        advance_to(rt);
        _in_progress = mutation_fragment(*_schema, permit(), std::move(rt));
        if (_out_of_range) {
            ret = push_ready_fragments_out_of_range();
        }
        if (needs_skip()) {
            ret = proceed::no;
        }
        return ret;
    }

    proceed flush_if_needed(bool is_static, position_in_partition&& pos) {
        sstlog.trace("mp_row_consumer_k_l {}: flush_if_needed({})", fmt::ptr(this), pos);

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
                _in_progress = mutation_fragment(*_schema, permit(), static_row());
            } else {
                _in_progress = mutation_fragment(*_schema, permit(), clustering_row(std::move(pos.key())));
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
    proceed do_consume_cell(bytes_view col_name, int64_t timestamp, int64_t ttl, int64_t expiration, CreateCell&& create_cell) {
        struct column col(*_schema, col_name, timestamp);

        auto ret = flush_if_needed(col.is_static, col.clustering);
        if (_skip_in_progress) {
            return ret;
        }

        if (col.cell.size() == 0) {
            row_marker rm(timestamp, gc_clock::duration(ttl), gc_clock::time_point(gc_clock::duration(expiration)));
            _in_progress->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) {
                cr.apply(std::move(rm));
            });
            return ret;
        }

        if (!col.is_present) {
            return ret;
        }

        create_cell(std::move(col));
        return ret;
    }

    virtual proceed consume_counter_cell(bytes_view col_name, fragmented_temporary_buffer::view value, int64_t timestamp) override {
        return do_consume_cell(col_name, timestamp, 0, 0, [&] (auto&& col) {
            auto ac = make_counter_cell(timestamp, value);

            if (col.is_static) {
                _in_progress->mutate_as_static_row(*_schema, [&] (static_row& sr) mutable {
                    sr.set_cell(*(col.cdef), std::move(ac));
                });
            } else {
                _in_progress->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                    cr.set_cell(*(col.cdef), atomic_cell_or_collection(std::move(ac)));
                });
            }
        });
    }

    virtual proceed consume_cell(bytes_view col_name, fragmented_temporary_buffer::view value, int64_t timestamp, int64_t ttl, int64_t expiration) override {
        return do_consume_cell(col_name, timestamp, ttl, expiration, [&] (auto&& col) {
            bool is_multi_cell = col.collection_extra_data.size();
            if (is_multi_cell != col.cdef->is_multi_cell()) {
                return;
            }
            if (is_multi_cell) {
                auto& value_type = visit(*col.cdef->type, make_visitor(
                    [] (const collection_type_impl& ctype) -> const abstract_type& { return *ctype.value_comparator(); },
                    [&] (const user_type_impl& utype) -> const abstract_type& {
                        if (col.collection_extra_data.size() != sizeof(int16_t)) {
                            throw malformed_sstable_exception(format("wrong size of field index while reading UDT column: expected {}, got {}",
                                        sizeof(int16_t), col.collection_extra_data.size()));
                        }

                        auto field_idx = deserialize_field_index(col.collection_extra_data);
                        if (field_idx >= utype.size()) {
                            throw malformed_sstable_exception(format("field index too big while reading UDT column: type has {} fields, got {}",
                                        utype.size(), field_idx));
                        }

                        return *utype.type(field_idx);
                    },
                    [] (const abstract_type& o) -> const abstract_type& {
                        throw malformed_sstable_exception(format("attempted to read multi-cell column, but expected type was {}", o.name()));
                    }
                ));
                auto ac = make_atomic_cell(value_type,
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
                _in_progress->mutate_as_static_row(*_schema, [&] (static_row& sr) mutable {
                    sr.set_cell(*(col.cdef), std::move(ac));
                });
                return;
            }
            _in_progress->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                cr.set_cell(*(col.cdef), atomic_cell_or_collection(std::move(ac)));
            });
        });
    }

    virtual proceed consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        auto timestamp = deltime.marked_for_delete_at;
        struct column col(*_schema, col_name, timestamp);
        gc_clock::duration secs(deltime.local_deletion_time);

        return consume_deleted_cell(col, timestamp, gc_clock::time_point(secs));
    }

    proceed consume_deleted_cell(column &col, int64_t timestamp, gc_clock::time_point local_deletion_time) {
        auto ret = flush_if_needed(col.is_static, col.clustering);
        if (_skip_in_progress) {
            return ret;
        }

        if (col.cell.size() == 0) {
            row_marker rm(tombstone(timestamp, local_deletion_time));
            _in_progress->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                cr.apply(rm);
            });
            return ret;
        }
        if (!col.is_present) {
            return ret;
        }

        auto ac = atomic_cell::make_dead(timestamp, local_deletion_time);

        bool is_multi_cell = col.collection_extra_data.size();
        if (is_multi_cell != col.cdef->is_multi_cell()) {
            return ret;
        }

        if (is_multi_cell) {
            update_pending_collection(col.cdef, to_bytes(col.collection_extra_data), std::move(ac));
        } else if (col.is_static) {
            _in_progress->mutate_as_static_row(*_schema, [&] (static_row& sr) {
                sr.set_cell(*col.cdef, atomic_cell_or_collection(std::move(ac)));
            });
        } else {
            _in_progress->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                cr.set_cell(*col.cdef, atomic_cell_or_collection(std::move(ac)));
            });
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
            _in_progress->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                cr.apply(shadowable_tombstone(tombstone(deltime)));
            });
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
        }
        throw malformed_sstable_exception(format("Unexpected start composite marker {:d}", uint16_t(uint8_t(found))));
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
        }
        throw malformed_sstable_exception(format("Unexpected end composite marker {:d}", uint16_t(uint8_t(found))));
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
                    _in_progress->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                        cr.apply(tombstone(deltime));
                    });
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
    // meaning that after next read partition_start will be emitted
    // or end of stream was reached.
    bool is_mutation_end() const {
        return _is_mutation_end;
    }

    bool is_out_of_range() const {
        return _out_of_range;
    }

    // See the RowConsumer concept
    void push_ready_fragments() {
        if (_ready) {
            if (push_ready_fragments_with_ready_set() == proceed::no) {
                return;
            }
        }

        if (_out_of_range) {
            push_ready_fragments_out_of_range();
        }
    }

    virtual void reset(indexable_element el) override {
        sstlog.trace("mp_row_consumer_k_l {}: reset({})", fmt::ptr(this), static_cast<int>(el));
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

    virtual position_in_partition_view position() override {
        if (_in_progress) {
            return _in_progress->position();
        }
        if (_ready) {
            return _ready->position();
        }
        if (_is_mutation_end) {
            return position_in_partition_view(position_in_partition_view::end_of_partition_tag_t{});
        }
        return position_in_partition_view(position_in_partition_view::partition_start_tag_t{});
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
        sstlog.trace("mp_row_consumer_k_l {}: fast_forward_to({})", fmt::ptr(this), r);
        _out_of_range = _is_mutation_end;
        _fwd_end = std::move(r).end();

        _range_tombstones.forward_to(r.start());

        _ck_ranges_walker->trim_front(std::move(r).start());
        if (_ck_ranges_walker->out_of_range()) {
            _out_of_range = true;
            _ready = {};
            sstlog.trace("mp_row_consumer_k_l {}: no more ranges", fmt::ptr(this));
            return { };
        }

        auto start = _ck_ranges_walker->lower_bound();

        if (_ready && !_ready->relevant_for_range(*_schema, start)) {
            _ready = {};
        }

        if (_in_progress) {
            advance_to(*_in_progress);
            if (!_skip_in_progress) {
                sstlog.trace("mp_row_consumer_k_l {}: _in_progress in range", fmt::ptr(this));
                return { };
            }
        }

        if (_out_of_range) {
            sstlog.trace("mp_row_consumer_k_l {}: _out_of_range=true", fmt::ptr(this));
            return { };
        }

        position_in_partition::less_compare less(*_schema);
        if (!less(start, _fwd_end)) {
            _out_of_range = true;
            sstlog.trace("mp_row_consumer_k_l {}: no overlap with restrictions", fmt::ptr(this));
            return { };
        }

        sstlog.trace("mp_row_consumer_k_l {}: advance_context({})", fmt::ptr(this), start);
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
        sstlog.trace("mp_row_consumer_k_l {}: advance_context({})", fmt::ptr(this), _ck_ranges_walker->lower_bound());
        return _ck_ranges_walker->lower_bound();
    }
};

flat_mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor) {
    return make_flat_mutation_reader<sstable_mutation_reader<data_consume_rows_context, mp_row_consumer_k_l>>(
        std::move(sstable), std::move(schema), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr, monitor);
}

} // namespace kl
} // namespace sstables

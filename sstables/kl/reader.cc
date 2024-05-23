/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "sstables/consumer.hh"
#include "sstables/kl/reader.hh"
#include "sstables/processing_result_generator.hh"
#include "sstables/sstable_mutation_reader.hh"
#include "sstables/sstables.hh"
#include "sstables/types.hh"
#include "clustering_key_filter.hh"
#include "clustering_ranges_walker.hh"
#include "concrete_types.hh"
#include "utils/assert.hh"
#include "utils/to_string.hh"
#include "utils/value_or_reference.hh"

namespace sstables {
namespace kl {

static inline bytes_view pop_back(std::vector<bytes_view>& vec) {
    auto b = std::move(vec.back());
    vec.pop_back();
    return b;
}

class mp_row_consumer_reader_k_l : public mp_row_consumer_reader_base, public mutation_reader::impl {
    friend class sstables::kl::mp_row_consumer_k_l;
private:
    range_tombstone_change_generator _rtc_gen;
public:
    mp_row_consumer_reader_k_l(schema_ptr s, reader_permit permit, shared_sstable sst)
        : mp_row_consumer_reader_base(std::move(sst))
        , impl(std::move(s), std::move(permit))
        , _rtc_gen(*_schema)
    {
        _permit.on_start_sstable_read();
    }
    virtual ~mp_row_consumer_reader_k_l() {
        _permit.on_finish_sstable_read();
    }

    void on_next_partition(dht::decorated_key key, tombstone tomb) {
        _partition_finished = false;
        _before_partition = false;
        _end_of_stream = false;
        _current_partition_key = std::move(key);
        _rtc_gen.reset();
        impl::push_mutation_fragment(*_schema, _permit, partition_start(*_current_partition_key, tomb));
        _sst->get_stats().on_partition_read();
    }
    void flush_tombstones(position_in_partition_view pos) {
        _rtc_gen.flush(pos, [this] (range_tombstone_change&& rtc) {
            impl::push_mutation_fragment(*_schema, _permit, std::move(rtc));
        });
    }
    void push_mutation_fragment(mutation_fragment&& mf) {
        flush_tombstones(mf.position());
        switch (mf.mutation_fragment_kind()) {
            case mutation_fragment::kind::partition_start:
                impl::push_mutation_fragment(*_schema, _permit, std::move(mf).as_partition_start());
                break;
            case mutation_fragment::kind::static_row:
                impl::push_mutation_fragment(*_schema, _permit, std::move(mf).as_static_row());
                break;
            case mutation_fragment::kind::clustering_row:
                impl::push_mutation_fragment(*_schema, _permit, std::move(mf).as_clustering_row());
                break;
            case mutation_fragment::kind::range_tombstone:
                _rtc_gen.consume(std::move(mf).as_range_tombstone());
                break;
            case mutation_fragment::kind::partition_end:
                impl::push_mutation_fragment(*_schema, _permit, std::move(mf).as_end_of_partition());
                break;
        }
    }
};

// Important note: the row key, column name and column value, passed to the
// consume_* functions, are passed as a "bytes_view" object, which points to
// internal data held by the feeder. This internal data is only valid for the
// duration of the single consume function it was passed to. If the object
// wants to hold these strings longer, it must make a copy of the bytes_view's
// contents. [Note, in reality, because our implementation reads the whole
// row into one buffer, the byte_views remain valid until consume_row_end()
// is called.]
class mp_row_consumer_k_l {
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;

public:
    using proceed = data_consumer::proceed;

    /*
     * In k/l formats, RTs are represented as cohesive entries so
     * setting/resetting RT start is not supported.
     */
    constexpr static bool is_setting_range_tombstone_start_supported = false;
private:
    mp_row_consumer_reader_k_l* _reader;
    const shared_sstable& _sst;
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
        SCYLLA_ASSERT(cdef->is_multi_cell() && "frozen set should behave like a cell\n");
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

    mp_row_consumer_k_l(mp_row_consumer_reader_k_l* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        const query::partition_slice& slice,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _reader(reader)
        , _sst(sst)
        , _schema(schema)
        , _slice(slice)
        , _fwd(fwd)
        , _range_tombstones(*_schema, this->permit())
        , _treat_non_compound_rt_as_compound(!sst->has_correct_non_compound_range_tombstones())
    { }

    mp_row_consumer_k_l(mp_row_consumer_reader_k_l* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : mp_row_consumer_k_l(reader, schema, std::move(permit), schema->full_slice(), std::move(trace_state), fwd, sst) { }

    // Consume the row's key and deletion_time. The latter determines if the
    // row is a tombstone, and if so, when it has been deleted.
    // Note that the key is in serialized form, and should be deserialized
    // (according to the schema) before use.
    // As explained above, the key object is only valid during this call, and
    // if the implementation wishes to save it, it must copy the *contents*.
    proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) {
        if (!_is_mutation_end) {
            return proceed::yes;
        }
        auto pk = key.to_partition_key(*_schema);
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
        SCYLLA_ASSERT(!_ready);
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
        auto rt_opt = _ck_ranges_walker->split_tombstone(rt, _range_tombstones);
        if (rt_opt) {
            _in_progress = mutation_fragment(*_schema, permit(), std::move(*rt_opt));
        }
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

    // Consume one counter cell. Column name and value are serialized, and need
    // to be deserialized according to the schema.
    proceed consume_counter_cell(bytes_view col_name, fragmented_temporary_buffer::view value, int64_t timestamp) {
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

    // Consume one cell (column name and value). Both are serialized, and need
    // to be deserialized according to the schema.
    // When a cell is set with an expiration time, "ttl" is the time to live
    // (in seconds) originally set for this cell, and "expiration" is the
    // absolute time (in seconds since the UNIX epoch) when this cell will
    // expire. Typical cells, not set to expire, will get expiration = 0.
    proceed consume_cell(bytes_view col_name, fragmented_temporary_buffer::view value, int64_t timestamp, int64_t ttl, int64_t expiration) {
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

    // Consume a deleted cell (i.e., a cell tombstone).
    proceed consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) {
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
    // Called at the end of the row, after all cells.
    // Returns a flag saying whether the sstable consumer should stop now, or
    // proceed consuming more data.
    proceed consume_row_end() {
        if (_in_progress) {
            flush();
        }
        _is_mutation_end = true;
        _out_of_range = true;
        return proceed::no;
    }

    // Consume one row tombstone.
    proceed consume_shadowable_row_tombstone(bytes_view col_name, sstables::deletion_time deltime) {
        auto key = composite_view(column::fix_static_name(*_schema, col_name)).explode();
        auto ck = clustering_key_prefix::from_exploded_view(key);
        auto ret = flush_if_needed(std::move(ck));
        if (!_skip_in_progress) {
            _in_progress->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                bool was_dead{cr.tomb()};
                cr.apply(shadowable_tombstone(tombstone(deltime)));
                if (!was_dead && cr.tomb()) {
                    _sst->get_stats().on_row_tombstone_read();
                }
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

    // Consume one range tombstone.
    proceed consume_range_tombstone(
        bytes_view start_col, bytes_view end_col,
        sstables::deletion_time deltime) {
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
                        bool was_dead{cr.tomb()};
                        cr.apply(tombstone(deltime));
                        if (!was_dead && cr.tomb()) {
                            _sst->get_stats().on_row_tombstone_read();
                        }
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
                    if (auto rt_opt = _ck_ranges_walker->split_tombstone(std::move(rt), _range_tombstones)) {
                        _range_tombstones.apply(std::move(*rt_opt));
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

    // Called when the reader is fast forwarded to given element.
    void reset(indexable_element el) {
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

    position_in_partition_view position() {
        if (_in_progress) {
            return _in_progress->position();
        }
        if (_ready) {
            return _ready->position();
        }
        if (_is_mutation_end) {
            return position_in_partition_view::for_partition_end();
        }
        return position_in_partition_view::for_partition_start();
    }

    // Changes current fragment range.
    //
    // When there are no more fragments for current range,
    // is_out_of_range() will return true.
    //
    // The new range must not overlap with the previous range and
    // must be after it.
    //
    std::optional<position_in_partition_view> fast_forward_to(position_range r) {
        sstlog.trace("mp_row_consumer_k_l {}: fast_forward_to({})", fmt::ptr(this), r);
        _out_of_range = _is_mutation_end;
        _fwd_end = std::move(r).end();

        // range_tombstone::trim() requires !is_clustering_row().
        if (r.start().is_clustering_row()) {
            r.set_start(position_in_partition::before_key(r.start().key()));
        }
        if (r.end().is_clustering_row()) {
            r.set_end(position_in_partition::before_key(r.end().key()));
        }

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

    // The permit for this read
    reader_permit& permit() {
        return _permit;
    }

    tracing::trace_state_ptr trace_state() const {
        return _trace_state;
    }
};

// data_consume_rows_context remembers the context that an ongoing
// data_consume_rows() future is in.
class data_consume_rows_context : public data_consumer::continuous_data_consumer<data_consume_rows_context> {
private:
    enum class state {
        ROW_START,
        ATOM_START,
        NOT_CLOSING,
    } _state = state::ROW_START;

    mp_row_consumer_k_l& _consumer;
    shared_sstable _sst;

    temporary_buffer<char> _key;
    temporary_buffer<char> _val;
    fragmented_temporary_buffer _val_fragmented;

    // state for reading a cell
    bool _deleted;
    bool _counter;
    uint32_t _ttl, _expiration;

    bool _shadowable;

    processing_result_generator _gen;
    temporary_buffer<char>* _processing_data;
public:
    using consumer = mp_row_consumer_k_l;
     // assumes !primitive_consumer::active()
    bool non_consuming() const {
        return false;
    }

    // process() feeds the given data into the state machine.
    // The consumer may request at any point (e.g., after reading a whole
    // row) to stop the processing, in which case we trim the buffer to
    // leave only the unprocessed part. The caller must handle calling
    // process() again, and/or refilling the buffer, as needed.
    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
#if 0
        // Testing hack: call process() for tiny chunks separately, to verify
        // that primitive types crossing input buffer are handled correctly.
        constexpr size_t tiny_chunk = 1; // try various tiny sizes
        if (data.size() > tiny_chunk) {
            for (unsigned i = 0; i < data.size(); i += tiny_chunk) {
                auto chunk_size = std::min(tiny_chunk, data.size() - i);
                auto chunk = data.share(i, chunk_size);
                if (process(chunk) == mp_row_consumer_k_l::proceed::no) {
                    data.trim_front(i + chunk_size - chunk.size());
                    return mp_row_consumer_k_l::proceed::no;
                }
            }
            data.trim(0);
            return mp_row_consumer_k_l::proceed::yes;
        }
#endif
        sstlog.trace("data_consume_row_context {}: state={}, size={}", fmt::ptr(this), static_cast<int>(_state), data.size());
        _processing_data = &data;
        return _gen.generate();
    }
private:
    processing_result_generator do_process_state() {
        while (true) {
            if (_state == state::ROW_START) {
                _state = state::NOT_CLOSING;
                co_yield read_short_length_bytes(*_processing_data, _key);
                co_yield read_32(*_processing_data);
                co_yield read_64(*_processing_data);
                deletion_time del;
                del.local_deletion_time = _u32;
                del.marked_for_delete_at = _u64;
                _sst->get_stats().on_row_read();
                auto ret = _consumer.consume_row_start(key_view(to_bytes_view(_key)), del);
                // after calling the consume function, we can release the
                // buffers we held for it.
                _key.release();
                _state = state::ATOM_START;
                if (ret == mp_row_consumer_k_l::proceed::no) {
                    co_yield mp_row_consumer_k_l::proceed::no;
                }
            }
            while (true) {
                co_yield read_short_length_bytes(*_processing_data, _key);
                if (_u16 == 0) {
                    // end of row marker
                    _state = state::ROW_START;
                    co_yield _consumer.consume_row_end();
                    break;
                }
                _state = state::NOT_CLOSING;
                co_yield read_8(*_processing_data);
                auto const mask = column_mask(_u8);

                if ((mask & (column_mask::range_tombstone | column_mask::shadowable)) != column_mask::none) {
                    _shadowable = (mask & column_mask::shadowable) != column_mask::none;
                    co_yield read_short_length_bytes(*_processing_data, _val);
                    co_yield read_32(*_processing_data);
                    co_yield read_64(*_processing_data);
                    _sst->get_stats().on_range_tombstone_read();
                    deletion_time del;
                    del.local_deletion_time = _u32;
                    del.marked_for_delete_at = _u64;
                    auto ret = _shadowable
                            ? _consumer.consume_shadowable_row_tombstone(to_bytes_view(_key), del)
                            : _consumer.consume_range_tombstone(to_bytes_view(_key), to_bytes_view(_val), del);
                    _key.release();
                    _val.release();
                    _state = state::ATOM_START;
                    co_yield ret;
                    continue;
                } else if ((mask & column_mask::counter) != column_mask::none) {
                    _deleted = false;
                    _counter = true;
                    co_yield read_64(*_processing_data);
                    // _timestamp_of_last_deletion = _u64;
                } else if ((mask & column_mask::expiration) != column_mask::none) {
                    _deleted = false;
                    _counter = false;
                    co_yield read_32(*_processing_data);
                    _ttl = _u32;
                    co_yield read_32(*_processing_data);
                    _expiration = _u32;
                } else {
                    // FIXME: see ColumnSerializer.java:deserializeColumnBody
                    if ((mask & column_mask::counter_update) != column_mask::none) {
                        throw malformed_sstable_exception("FIXME COUNTER_UPDATE_MASK");
                    }
                    _ttl = _expiration = 0;
                    _deleted = (mask & column_mask::deletion) != column_mask::none;
                    _counter = false;
                }
                co_yield read_64(*_processing_data);
                co_yield read_32(*_processing_data);
                co_yield read_bytes(*_processing_data, _u32, _val_fragmented);
                mp_row_consumer_k_l::proceed ret;
                if (_deleted) {
                    if (_val_fragmented.size_bytes() != 4) {
                        throw malformed_sstable_exception("deleted cell expects local_deletion_time value");
                    }
                    _val = temporary_buffer<char>(4);
                    auto v = fragmented_temporary_buffer::view(_val_fragmented);
                    read_fragmented(v, 4, reinterpret_cast<bytes::value_type*>(_val.get_write()));
                    deletion_time del;
                    del.local_deletion_time = consume_be<uint32_t>(_val);
                    del.marked_for_delete_at = _u64;
                    ret = _consumer.consume_deleted_cell(to_bytes_view(_key), del);
                    _val.release();
                } else if (_counter) {
                    ret = _consumer.consume_counter_cell(to_bytes_view(_key),
                            fragmented_temporary_buffer::view(_val_fragmented), _u64);
                } else {
                    ret = _consumer.consume_cell(to_bytes_view(_key),
                            fragmented_temporary_buffer::view(_val_fragmented), _u64, _ttl, _expiration);
                }
                // after calling the consume function, we can release the
                // buffers we held for it.
                _key.release();
                _val_fragmented.remove_prefix(_val_fragmented.size_bytes());
                _state = state::ATOM_START;
                co_yield ret;
            }
        }
    }
public:

    data_consume_rows_context(const schema&,
                              const shared_sstable sst,
                              mp_row_consumer_k_l& consumer,
                              input_stream<char>&& input, uint64_t start, uint64_t maxlen)
                : continuous_data_consumer(consumer.permit(), std::move(input), start, maxlen)
                , _consumer(consumer)
                , _sst(std::move(sst))
                , _gen(do_process_state())
    {}

    void verify_end_state() {
        // If reading a partial row (i.e., when we have a clustering row
        // filter and using a promoted index), we may be in ATOM_START
        // state instead of ROW_START. In that case we did not read the
        // end-of-row marker and consume_row_end() was never called.
        if (_state == state::ATOM_START) {
            _consumer.consume_row_end();
            return;
        }
        if (_state != state::ROW_START || primitive_consumer::active()) {
            throw malformed_sstable_exception("end of input, but not end of row");
        }
    }

    void reset(indexable_element el) {
        switch (el) {
        case indexable_element::partition:
            _state = state::ROW_START;
            break;
        case indexable_element::cell:
            _state = state::ATOM_START;
            break;
        default:
            SCYLLA_ASSERT(0);
        }
        _consumer.reset(el);
        _gen = do_process_state();
    }

    reader_permit& permit() {
        return _consumer.permit();
    }
};

class sstable_mutation_reader : public mp_row_consumer_reader_k_l {
    using DataConsumeRowsContext = kl::data_consume_rows_context;
    using Consumer = mp_row_consumer_k_l;
    static_assert(RowConsumer<Consumer>);
    value_or_reference<query::partition_slice> _slice_holder;
    const query::partition_slice& _slice;
    Consumer _consumer;
    bool _will_likely_slice = false;
    bool _read_enabled = true;
    std::unique_ptr<DataConsumeRowsContext> _context;
    std::unique_ptr<index_reader> _index_reader;
    // We avoid unnecessary lookup for single partition reads thanks to this flag
    bool _single_partition_read = false;
    const dht::partition_range& _pr;
    streamed_mutation::forwarding _fwd;
    mutation_reader::forwarding _fwd_mr;
    read_monitor& _monitor;
public:
    sstable_mutation_reader(shared_sstable sst,
                            schema_ptr schema,
                            reader_permit permit,
                            const dht::partition_range& pr,
                            value_or_reference<query::partition_slice> slice,
                            tracing::trace_state_ptr trace_state,
                            streamed_mutation::forwarding fwd,
                            mutation_reader::forwarding fwd_mr,
                            read_monitor& mon)
            : mp_row_consumer_reader_k_l(std::move(schema), permit, std::move(sst))
            , _slice_holder(std::move(slice))
            , _slice(_slice_holder.get())
            , _consumer(this, _schema, std::move(permit), _slice, std::move(trace_state), fwd, _sst)
            // FIXME: I want to add `&& fwd_mr == mutation_reader::forwarding::no` below
            // but can't because many call sites use the default value for
            // `mutation_reader::forwarding` which is `yes`.
            , _single_partition_read(pr.is_singular())
            , _pr(pr)
            , _fwd(fwd)
            , _fwd_mr(fwd_mr)
            , _monitor(mon) { }

    // Reference to _consumer is passed to data_consume_rows() in the constructor so we must not allow move/copy
    sstable_mutation_reader(sstable_mutation_reader&&) = delete;
    sstable_mutation_reader(const sstable_mutation_reader&) = delete;
    ~sstable_mutation_reader() {
        if (_context || _index_reader) {
            sstlog.warn("sstable_mutation_reader was not closed. Closing in the background. Backtrace: {}", current_backtrace());
            // FIXME: discarded future.
            (void)close();
        }
    }
private:
    static bool will_likely_slice(const query::partition_slice& slice) {
        return (!slice.default_row_ranges().empty() && !slice.default_row_ranges()[0].is_full())
               || slice.get_specific_ranges();
    }
    index_reader& get_index_reader() {
        if (!_index_reader) {
            auto caching = use_caching(global_cache_index_pages && !_slice.options.contains(query::partition_slice::option::bypass_cache));
            _index_reader = std::make_unique<index_reader>(_sst, _consumer.permit(),
                                                           _consumer.trace_state(), caching, _single_partition_read);
        }
        return *_index_reader;
    }
    future<> advance_to_next_partition() {
        sstlog.trace("reader {}: advance_to_next_partition()", fmt::ptr(this));
        _before_partition = true;
        auto& consumer = _consumer;
        if (consumer.is_mutation_end()) {
            sstlog.trace("reader {}: already at partition boundary", fmt::ptr(this));
            _index_in_current_partition = false;
            return make_ready_future<>();
        }
        return (_index_in_current_partition
                ? _index_reader->advance_to_next_partition()
                : get_index_reader().advance_to(dht::ring_position_view::for_after_key(*_current_partition_key))).then([this] {
            _index_in_current_partition = true;
            auto [start, end] = _index_reader->data_file_positions();
            if (end && start > *end) {
                _read_enabled = false;
                return make_ready_future<>();
            }
            SCYLLA_ASSERT(_index_reader->element_kind() == indexable_element::partition);
            return skip_to(_index_reader->element_kind(), start).then([this] {
                _sst->get_stats().on_partition_seek();
            });
        });
    }
    future<> read_from_index() {
        sstlog.trace("reader {}: read from index", fmt::ptr(this));
        auto tomb = _index_reader->partition_tombstone();
        if (!tomb) {
            sstlog.trace("reader {}: no tombstone", fmt::ptr(this));
            return read_from_datafile();
        }
        auto pk = _index_reader->get_partition_key();
        auto key = dht::decorate_key(*_schema, std::move(pk));
        _consumer.setup_for_partition(key.key());
        on_next_partition(std::move(key), tombstone(*tomb));
        return make_ready_future<>();
    }
    future<> read_from_datafile() {
        sstlog.trace("reader {}: read from data file", fmt::ptr(this));
        return _context->consume_input();
    }
    // Assumes that we're currently positioned at partition boundary.
    future<> read_partition() {
        sstlog.trace("reader {}: reading partition", fmt::ptr(this));

        _end_of_stream = true; // on_next_partition() will set it to true
        if (!_read_enabled) {
            sstlog.trace("reader {}: eof", fmt::ptr(this));
            return make_ready_future<>();
        }

        if (!_consumer.is_mutation_end()) {
            throw malformed_sstable_exception(format("consumer not at partition boundary, position: {}",
                                                     position_in_partition_view::printer(*_schema, _consumer.position())), _sst->get_filename());
        }

        // It's better to obtain partition information from the index if we already have it.
        // We can save on IO if the user will skip past the front of partition immediately.
        //
        // It is also better to pay the cost of reading the index if we know that we will
        // need to use the index anyway soon.
        //
        if (_index_in_current_partition) {
            if (_context->eof()) {
                sstlog.trace("reader {}: eof", fmt::ptr(this));
                return make_ready_future<>();
            }
            if (_index_reader->partition_data_ready()) {
                return read_from_index();
            }
            if (_will_likely_slice) {
                return _index_reader->read_partition_data().then([this] {
                    return read_from_index();
                });
            }
        }

        // FIXME: advance index to current partition if _will_likely_slice
        return read_from_datafile();
    }
    // Can be called from any position.
    future<> read_next_partition() {
        sstlog.trace("reader {}: read next partition", fmt::ptr(this));
        // If next partition exists then on_next_partition will be called
        // and _end_of_stream will be set to false again.
        _end_of_stream = true;
        if (!_read_enabled || _single_partition_read) {
            sstlog.trace("reader {}: eof", fmt::ptr(this));
            return make_ready_future<>();
        }
        return advance_to_next_partition().then([this] {
            return read_partition();
        });
    }
    future<> advance_context(std::optional<position_in_partition_view> pos) {
        if (!pos || pos->is_before_all_fragments(*_schema)) {
            return make_ready_future<>();
        }
        SCYLLA_ASSERT (_current_partition_key);
        return [this] {
            if (!_index_in_current_partition) {
                _index_in_current_partition = true;
                return get_index_reader().advance_to(*_current_partition_key);
            }
            return make_ready_future();
        }().then([this, pos] {
            return get_index_reader().advance_to(*pos).then([this] {
                index_reader& idx = *_index_reader;
                auto index_position = idx.data_file_positions();
                if (index_position.start <= _context->position()) {
                    return make_ready_future<>();
                }
                return skip_to(idx.element_kind(), index_position.start).then([this] {
                    _sst->get_stats().on_partition_seek();
                });
            });
        });
    }
    bool is_initialized() const {
        return bool(_context);
    }
    future<> initialize() {
        if (_single_partition_read) {
            _sst->get_stats().on_single_partition_read();
            const auto& key = dht::ring_position_view(_pr.start()->value());
            position_in_partition_view pos = get_slice_upper_bound(*_schema, _slice, key);
            const auto present = co_await get_index_reader().advance_lower_and_check_if_present(key, pos);

            if (!present) {
                _sst->get_filter_tracker().add_false_positive();
                co_return;
            }

            _sst->get_filter_tracker().add_true_positive();
        } else {
            _sst->get_stats().on_range_partition_read();
            co_await get_index_reader().advance_to(_pr);
        }

        auto [begin, end] = _index_reader->data_file_positions();
        SCYLLA_ASSERT(end);

        if (_single_partition_read) {
            _read_enabled = (begin != *end);
            _context = data_consume_single_partition<DataConsumeRowsContext>(*_schema, _sst, _consumer, { begin, *end });
        } else {
            sstable::disk_read_range drr{begin, *end};
            auto last_end = _fwd_mr ? _sst->data_size() : drr.end;
            _read_enabled = bool(drr);
            _context = data_consume_rows<DataConsumeRowsContext>(*_schema, _sst, _consumer, std::move(drr), last_end);
        }

        _monitor.on_read_started(_context->reader_position());
        _index_in_current_partition = true;
        _will_likely_slice = will_likely_slice(_slice);
    }
    future<> ensure_initialized() {
        if (is_initialized()) {
            return make_ready_future<>();
        }
        return initialize();
    }
    future<> skip_to(indexable_element el, uint64_t begin) {
        sstlog.trace("sstable_reader: {}: skip_to({} -> {}, el={})", fmt::ptr(_context.get()), _context->position(), begin, static_cast<int>(el));
        if (begin <= _context->position()) {
            return make_ready_future<>();
        }
        _context->reset(el);
        return _context->skip_to(begin);
    }
public:
    void on_out_of_clustering_range() override {
        if (_fwd == streamed_mutation::forwarding::yes) {
            _end_of_stream = true;
        } else {
            this->push_mutation_fragment(mutation_fragment(*_schema, _permit, partition_end()));
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
                _before_partition = true;
                _end_of_stream = false;
                SCYLLA_ASSERT(_index_reader);
                auto f1 = _index_reader->advance_to(pr);
                return f1.then([this] {
                    auto [start, end] = _index_reader->data_file_positions();
                    SCYLLA_ASSERT(end);
                    if (start != *end) {
                        _read_enabled = true;
                        _index_in_current_partition = true;
                        _context->reset(indexable_element::partition);
                        return _context->fast_forward_to(start, *end);
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
            return initialize().then([this] {
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
                if (_before_partition) {
                    return read_partition();
                } else {
                    return read_next_partition();
                }
            } else {
                return do_until([this] { return is_buffer_full() || _partition_finished || _end_of_stream; }, [this] {
                    _consumer.push_ready_fragments();
                    if (is_buffer_full() || _partition_finished || _end_of_stream) {
                        return make_ready_future<>();
                    }
                    return advance_context(_consumer.maybe_skip()).then([this] {
                        return _context->consume_input();
                    });
                });
            }
        }).then_wrapped([this] (future<> f) {
            try {
                f.get();
            } catch(sstables::malformed_sstable_exception& e) {
                throw sstables::malformed_sstable_exception(format("Failed to read partition from SSTable {} due to {}", _sst->get_filename(), e.what()));
            }
        });
    }
    virtual future<> next_partition() override {
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
        return make_ready_future<>();
        // If _ds is not created then next_partition() has no effect because there was no partition_start emitted yet.
    }
    virtual future<> fast_forward_to(position_range cr) override {
        clear_buffer();
        if (!_partition_finished) {
            _end_of_stream = false;
            return advance_context(_consumer.fast_forward_to(std::move(cr)));
        } else {
            _end_of_stream = true;
            return make_ready_future<>();
        }
    }
    virtual future<> close() noexcept override {
        auto close_context = make_ready_future<>();
        if (_context) {
            _monitor.on_read_completed();
            // move _context to prevent double-close from destructor.
            close_context = _context->close().finally([_ = std::move(_context)] {});
        }

        auto close_index_reader = make_ready_future<>();
        if (_index_reader) {
            // move _index_reader to prevent double-close from destructor.
            close_index_reader = _index_reader->close().finally([_ = std::move(_index_reader)] {});
        }

        return when_all_succeed(std::move(close_context), std::move(close_index_reader)).discard_result().handle_exception([] (std::exception_ptr ep) {
            // close can not fail as it is called either from the destructor or from mutation_reader::close
            sstlog.warn("Failed closing of sstable_mutation_reader: {}. Ignored since the reader is already done.", ep);
        });
    }
};

static mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        value_or_reference<query::partition_slice> slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor) {
    return make_mutation_reader<sstable_mutation_reader>(
        std::move(sstable), std::move(schema), std::move(permit), range,
        std::move(slice), std::move(trace_state), fwd, fwd_mr, monitor);
}


mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor) {
    return make_reader(
        std::move(sstable), std::move(schema), std::move(permit), range, value_or_reference(slice), std::move(trace_state), fwd, fwd_mr, monitor);
}

mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        query::partition_slice&& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor) {
    return make_reader(
        std::move(sstable), std::move(schema), std::move(permit), range, value_or_reference(std::move(slice)), std::move(trace_state), fwd, fwd_mr, monitor);
}


class crawling_sstable_mutation_reader : public mp_row_consumer_reader_k_l {
    using DataConsumeRowsContext = kl::data_consume_rows_context;
    using Consumer = mp_row_consumer_k_l;
    static_assert(RowConsumer<Consumer>);
    Consumer _consumer;
    std::unique_ptr<DataConsumeRowsContext> _context;
    read_monitor& _monitor;
public:
    crawling_sstable_mutation_reader(shared_sstable sst, schema_ptr schema,
             reader_permit permit,
             tracing::trace_state_ptr trace_state,
             read_monitor& mon)
        : mp_row_consumer_reader_k_l(std::move(schema), permit, std::move(sst))
        , _consumer(this, _schema, std::move(permit), _schema->full_slice(), std::move(trace_state), streamed_mutation::forwarding::no, _sst)
        , _context(data_consume_rows<DataConsumeRowsContext>(*_schema, _sst, _consumer))
        , _monitor(mon) {
        _monitor.on_read_started(_context->reader_position());
    }
public:
    void on_out_of_clustering_range() override {
        push_mutation_fragment(mutation_fragment(*_schema, _permit, partition_end()));
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        on_internal_error(sstlog, "crawling_sstable_mutation_reader: doesn't support fast_forward_to(const dht::partition_range&)");
    }
    virtual future<> fast_forward_to(position_range cr) override {
        on_internal_error(sstlog, "crawling_sstable_mutation_reader: doesn't support fast_forward_to(position_range)");
    }
    virtual future<> next_partition() override {
        on_internal_error(sstlog, "crawling_sstable_mutation_reader: doesn't support next_partition()");
    }
    virtual future<> fill_buffer() override {
        if (_end_of_stream) {
            return make_ready_future<>();
        }
        if (_context->eof()) {
            _end_of_stream = true;
            return make_ready_future<>();
        }
        return _context->consume_input();
    }
    virtual future<> close() noexcept override {
        if (!_context) {
            return make_ready_future<>();
        }
        _monitor.on_read_completed();
        return _context->close().handle_exception([_ = std::move(_context)] (std::exception_ptr ep) {
            sstlog.warn("Failed closing of crawling_sstable_mutation_reader: {}. Ignored since the reader is already done.", ep);
        });
    }
};

mutation_reader make_crawling_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor) {
    return make_mutation_reader<crawling_sstable_mutation_reader>(std::move(sstable), std::move(schema), std::move(permit),
            std::move(trace_state), monitor);
}

} // namespace kl
} // namespace sstables

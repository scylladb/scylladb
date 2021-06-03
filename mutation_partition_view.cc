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

#include <seastar/core/simple-stream.hh>

#include "mutation_partition_view.hh"
#include "schema.hh"
#include "atomic_cell.hh"
#include "utils/data_input.hh"
#include "mutation_partition_serializer.hh"
#include "mutation_partition.hh"
#include "counters.hh"
#include "frozen_mutation.hh"
#include "partition_builder.hh"
#include "converting_mutation_partition_applier.hh"
#include "concrete_types.hh"
#include "types/user.hh"

#include "utils/UUID.hh"
#include "serializer.hh"
#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/mutation.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"

using namespace db;

static_assert(MutationViewVisitor<mutation_partition_view_virtual_visitor>);

mutation_partition_view_virtual_visitor::~mutation_partition_view_virtual_visitor() = default;

namespace {

using atomic_cell_variant = boost::variant<ser::live_cell_view,
                                           ser::expiring_cell_view,
                                           ser::dead_cell_view,
                                           ser::counter_cell_view,
                                           ser::unknown_variant_type>;

atomic_cell read_atomic_cell(const abstract_type& type, atomic_cell_variant cv, atomic_cell::collection_member cm = atomic_cell::collection_member::no)
{
    class atomic_cell_visitor : public boost::static_visitor<atomic_cell> {
        const abstract_type& _type;
        atomic_cell::collection_member _collection_member;
    public:
        explicit atomic_cell_visitor(const abstract_type& t, atomic_cell::collection_member cm)
            : _type(t), _collection_member(cm) { }
        atomic_cell operator()(ser::live_cell_view& lcv) const {
            return atomic_cell::make_live(_type, lcv.created_at(), lcv.value().view(), _collection_member);
        }
        atomic_cell operator()(ser::expiring_cell_view& ecv) const {
            return atomic_cell::make_live(_type, ecv.c().created_at(), ecv.c().value().view(), ecv.expiry(), ecv.ttl(), _collection_member);
        }
        atomic_cell operator()(ser::dead_cell_view& dcv) const {
            return atomic_cell::make_dead(dcv.tomb().timestamp(), dcv.tomb().deletion_time());
        }
        atomic_cell operator()(ser::counter_cell_view& ccv) const {
            class counter_cell_visitor : public boost::static_visitor<atomic_cell> {
                api::timestamp_type _created_at;
            public:
                explicit counter_cell_visitor(api::timestamp_type ts)
                    : _created_at(ts) { }

                atomic_cell operator()(ser::counter_cell_full_view& ccv) const {
                    // TODO: a lot of copying for something called view
                    counter_cell_builder ccb; // we know the final number of shards
                    for (auto csv : ccv.shards()) {
                        ccb.add_maybe_unsorted_shard(counter_shard(csv));
                    }
                    ccb.sort_and_remove_duplicates();
                    return ccb.build(_created_at);
                }
                atomic_cell operator()(ser::counter_cell_update_view& ccv) const {
                    return atomic_cell::make_live_counter_update(_created_at, ccv.delta());
                }
                atomic_cell operator()(ser::unknown_variant_type&) const {
                    throw std::runtime_error("Trying to deserialize counter cell in unknown state");
                }
            };
            auto v = ccv.value();
            return boost::apply_visitor(counter_cell_visitor(ccv.created_at()), v);
        }
        atomic_cell operator()(ser::unknown_variant_type&) const {
            throw std::runtime_error("Trying to deserialize cell in unknown state");
        }
    };
    return boost::apply_visitor(atomic_cell_visitor(type, cm), cv);
}

collection_mutation read_collection_cell(const abstract_type& type, ser::collection_cell_view cv)
{
    collection_mutation_description mut;
    mut.tomb = cv.tomb();
    auto&& elements = cv.elements();
    mut.cells.reserve(elements.size());

    visit(type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            auto& value_type = *ctype.value_comparator();
            for (auto&& e : elements) {
                mut.cells.emplace_back(e.key(), read_atomic_cell(value_type, e.value(), atomic_cell::collection_member::yes));
            }
        },
        [&] (const user_type_impl& utype) {
            for (auto&& e : elements) {
                bytes key = e.key();
                auto idx = deserialize_field_index(key);
                assert(idx < utype.size());

                mut.cells.emplace_back(key, read_atomic_cell(*utype.type(idx), e.value(), atomic_cell::collection_member::yes));
            }
        },
        [&] (const abstract_type& o) {
            throw std::runtime_error(format("attempted to read a collection cell with type: {}", o.name()));
        }
    ));

    return mut.serialize(type);
}

template<typename Visitor>
void read_and_visit_row(ser::row_view rv, const column_mapping& cm, column_kind kind, Visitor&& visitor)
{
    for (auto&& cv : rv.columns()) {
        auto id = cv.id();
        auto& col = cm.column_at(kind, id);

        class atomic_cell_or_collection_visitor : public boost::static_visitor<> {
            Visitor& _visitor;
            column_id _id;
            const column_mapping_entry& _col;
        public:
            explicit atomic_cell_or_collection_visitor(Visitor& v, column_id id, const column_mapping_entry& col)
                : _visitor(v), _id(id), _col(col) { }

            void operator()(atomic_cell_variant& acv) const {
                if (!_col.is_atomic()) {
                    throw std::runtime_error("A collection expected, got an atomic cell");
                }
                _visitor.accept_atomic_cell(_id, read_atomic_cell(*_col.type(), acv));
            }
            void operator()(ser::collection_cell_view& ccv) const {
                if (_col.is_atomic()) {
                    throw std::runtime_error("An atomic cell expected, got a collection");
                }
                // FIXME: Pass view to cell to avoid copy
                auto&& outer = current_allocator();
                with_allocator(standard_allocator(), [&] {
                    auto cell = read_collection_cell(*_col.type(), ccv);
                    with_allocator(outer, [&] {
                        _visitor.accept_collection(_id, cell);
                    });
                });
            }
            void operator()(ser::unknown_variant_type&) const {
                throw std::runtime_error("Trying to deserialize unknown cell type");
            }
        };
        auto&& cell = cv.c();
        boost::apply_visitor(atomic_cell_or_collection_visitor(visitor, id, col), cell);
    }
}

row_marker read_row_marker(boost::variant<ser::live_marker_view, ser::expiring_marker_view, ser::dead_marker_view, ser::no_marker_view, ser::unknown_variant_type> rmv)
{
    struct row_marker_visitor : boost::static_visitor<row_marker> {
        row_marker operator()(ser::live_marker_view& lmv) const {
            return row_marker(lmv.created_at());
        }
        row_marker operator()(ser::expiring_marker_view& emv) const {
            return row_marker(emv.lm().created_at(), emv.ttl(), emv.expiry());
        }
        row_marker operator()(ser::dead_marker_view& dmv) const {
            return row_marker(dmv.tomb());
        }
        row_marker operator()(ser::no_marker_view&) const {
            return row_marker();
        }
        row_marker operator()(ser::unknown_variant_type&) const {
            throw std::runtime_error("Trying to deserialize unknown row marker type");
        }
    };
    return boost::apply_visitor(row_marker_visitor(), rmv);
}

}

template<typename Visitor>
requires MutationViewVisitor<Visitor>
void mutation_partition_view::do_accept(const column_mapping& cm, Visitor& visitor) const {
    auto in = _in;
    auto mpv = ser::deserialize(in, boost::type<ser::mutation_partition_view>());

    visitor.accept_partition_tombstone(mpv.tomb());

    struct static_row_cell_visitor {
        Visitor& _visitor;

        void accept_atomic_cell(column_id id, atomic_cell ac) const {
           _visitor.accept_static_cell(id, std::move(ac));
        }
        void accept_collection(column_id id, const collection_mutation& cm) const {
           _visitor.accept_static_cell(id, cm);
        }
    };
    read_and_visit_row(mpv.static_row(), cm, column_kind::static_column, static_row_cell_visitor{visitor});

    for (auto&& rt : mpv.range_tombstones()) {
        visitor.accept_row_tombstone(rt);
    }

    for (auto&& cr : mpv.rows()) {
        auto t = row_tombstone(cr.deleted_at(), shadowable_tombstone(cr.shadowable_deleted_at()));
        visitor.accept_row(position_in_partition_view::for_key(cr.key()), t, read_row_marker(cr.marker()), is_dummy::no, is_continuous::yes);

        struct cell_visitor {
            Visitor& _visitor;

            void accept_atomic_cell(column_id id, atomic_cell ac) const {
               _visitor.accept_row_cell(id, std::move(ac));
            }
            void accept_collection(column_id id, const collection_mutation& cm) const {
               _visitor.accept_row_cell(id, cm);
            }
        };
        read_and_visit_row(cr.cells(), cm, column_kind::regular_column, cell_visitor{visitor});
    }
}

void mutation_partition_view::accept(const schema& s, partition_builder& visitor) const
{
    do_accept(s.get_column_mapping(), visitor);
}

void mutation_partition_view::accept(const column_mapping& cm, converting_mutation_partition_applier& visitor) const
{
    do_accept(cm, visitor);
}

void mutation_partition_view::accept(const column_mapping& cm, mutation_partition_view_virtual_visitor& visitor) const {
    do_accept(cm, visitor);
}

std::optional<clustering_key> mutation_partition_view::first_row_key() const
{
    auto in = _in;
    auto mpv = ser::deserialize(in, boost::type<ser::mutation_partition_view>());
    auto rows = mpv.rows();
    if (rows.empty()) {
        return { };
    }
    return rows.front().key();
}

std::optional<clustering_key> mutation_partition_view::last_row_key() const
{
    auto in = _in;
    auto mpv = ser::deserialize(in, boost::type<ser::mutation_partition_view>());
    auto rows = mpv.rows();
    if (rows.empty()) {
        return { };
    }
    return rows.back().key();
}

mutation_partition_view mutation_partition_view::from_view(ser::mutation_partition_view v)
{
    return { v.v };
}

mutation_fragment frozen_mutation_fragment::unfreeze(const schema& s, reader_permit permit)
{
    auto in = ser::as_input_stream(_bytes);
    auto view = ser::deserialize(in, boost::type<ser::mutation_fragment_view>());
    return seastar::visit(view.fragment(),
        [&] (ser::clustering_row_view crv) {
            class clustering_row_builder {
                const schema& _s;
                mutation_fragment _mf;
            public:
                clustering_row_builder(const schema& s, reader_permit permit, clustering_key key, row_tombstone t, row_marker m)
                    : _s(s), _mf(mutation_fragment::clustering_row_tag_t(), s, std::move(permit), std::move(key), std::move(t), std::move(m), row()) { }
                void accept_atomic_cell(column_id id, atomic_cell ac) {
                    _mf.mutate_as_clustering_row(_s, [&] (clustering_row& cr) mutable {
                        cr.cells().append_cell(id, std::move(ac));
                    });
                }
                void accept_collection(column_id id, const collection_mutation& cm) {
                    _mf.mutate_as_clustering_row(_s, [&] (clustering_row& cr) mutable {
                        cr.cells().append_cell(id, collection_mutation(*_s.regular_column_at(id).type, cm));
                    });
                }
                mutation_fragment get_mutation_fragment() && { return std::move(_mf); }
            };

            auto cr = crv.row();
            auto t = row_tombstone(cr.deleted_at(), shadowable_tombstone(cr.shadowable_deleted_at()));
            clustering_row_builder builder(s, permit, cr.key(), std::move(t), read_row_marker(cr.marker()));
            read_and_visit_row(cr.cells(), s.get_column_mapping(), column_kind::regular_column, builder);
            return std::move(builder).get_mutation_fragment();
        },
        [&] (ser::static_row_view sr) {
            class static_row_builder {
                const schema& _s;
                mutation_fragment _mf;
            public:
                explicit static_row_builder(const schema& s, reader_permit permit) : _s(s), _mf(_s, std::move(permit), static_row()) { }
                void accept_atomic_cell(column_id id, atomic_cell ac) {
                    _mf.mutate_as_static_row(_s, [&] (static_row& sr) mutable {
                        sr.cells().append_cell(id, std::move(ac));
                    });
                }
                void accept_collection(column_id id, const collection_mutation& cm) {
                    _mf.mutate_as_static_row(_s, [&] (static_row& sr) mutable {
                        sr.cells().append_cell(id, collection_mutation(*_s.static_column_at(id).type, cm));
                    });
                }
                mutation_fragment get_mutation_fragment() && { return std::move(_mf); }
            };

            static_row_builder builder(s, permit);
            read_and_visit_row(sr.cells(), s.get_column_mapping(), column_kind::static_column, builder);
            return std::move(builder).get_mutation_fragment();
        },
        [&] (ser::range_tombstone_view rt) {
            return mutation_fragment(s, permit, range_tombstone(rt));
        },
        [&] (ser::partition_start_view ps) {
            auto dkey = dht::decorate_key(s, ps.key());
            return mutation_fragment(s, permit, partition_start(std::move(dkey), ps.partition_tombstone()));
        },
        [&] (partition_end) {
            return mutation_fragment(s, permit, partition_end());
        },
        [] (ser::unknown_variant_type) -> mutation_fragment {
            throw std::runtime_error("Trying to deserialize unknown mutation fragment type");
        }
    );
}

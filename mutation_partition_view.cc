/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

namespace {

atomic_cell read_atomic_cell(boost::variant<ser::live_cell_view, ser::expiring_cell_view, ser::dead_cell_view, ser::unknown_variant_type> cv)
{
    struct atomic_cell_visitor : boost::static_visitor<atomic_cell> {
        atomic_cell operator()(ser::live_cell_view& lcv) const {
            return atomic_cell::make_live(lcv.created_at(), lcv.value());
        }
        atomic_cell operator()(ser::expiring_cell_view& ecv) const {
            return atomic_cell::make_live(ecv.c().created_at(), ecv.c().value(), ecv.expiry(), ecv.ttl());
        }
        atomic_cell operator()(ser::dead_cell_view& dcv) const {
            return atomic_cell::make_dead(dcv.tomb().timestamp(), dcv.tomb().deletion_time());
        }
        atomic_cell operator()(ser::unknown_variant_type&) const {
            throw std::runtime_error("Trying to deserialize cell in unknown state");
        }
    };
    return boost::apply_visitor(atomic_cell_visitor(), cv);
}

collection_mutation read_collection_cell(ser::collection_cell_view cv)
{
    collection_type_impl::mutation mut;
    mut.tomb = cv.tomb();
    auto&& elements = cv.elements();
    mut.cells.reserve(elements.size());
    for (auto&& e : elements) {
        mut.cells.emplace_back(e.key(), read_atomic_cell(e.value()));
    }
    return collection_type_impl::serialize_mutation_form(mut);
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

            void operator()(boost::variant<ser::live_cell_view, ser::expiring_cell_view, ser::dead_cell_view, ser::unknown_variant_type>& acv) const {
                if (!_col.type()->is_atomic()) {
                    throw std::runtime_error("A collection expected, got an atomic cell");
                }
                // FIXME: Pass view to cell to avoid copy
                auto&& outer = current_allocator();
                with_allocator(standard_allocator(), [&] {
                    auto cell = read_atomic_cell(acv);
                    with_allocator(outer, [&] {
                        _visitor.accept_atomic_cell(_id, cell);
                    });
                });
            }
            void operator()(ser::collection_cell_view& ccv) const {
                if (_col.type()->is_atomic()) {
                    throw std::runtime_error("An atomic cell expected, got a collection");
                }
                // FIXME: Pass view to cell to avoid copy
                auto&& outer = current_allocator();
                with_allocator(standard_allocator(), [&] {
                    auto cell = read_collection_cell(ccv);
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

void
mutation_partition_view::accept(const schema& s, mutation_partition_visitor& visitor) const {
    accept(s.get_column_mapping(), visitor);
}

void
mutation_partition_view::accept(const column_mapping& cm, mutation_partition_visitor& visitor) const {
    auto in = _in;
    auto mpv = ser::deserialize(in, boost::type<ser::mutation_partition_view>());

    visitor.accept_partition_tombstone(mpv.tomb());

    struct static_row_cell_visitor {
        mutation_partition_visitor& _visitor;

        void accept_atomic_cell(column_id id, atomic_cell ac) const {
           _visitor.accept_static_cell(id, ac);
        }
        void accept_collection(column_id id, collection_mutation cm) const {
           _visitor.accept_static_cell(id, cm);
        }
    };
    read_and_visit_row(mpv.static_row(), cm, column_kind::static_column, static_row_cell_visitor{visitor});

    for (auto&& rt : mpv.range_tombstones()) {
        visitor.accept_row_tombstone(rt.key(), rt.tomb());
    }
    
    for (auto&& cr : mpv.rows()) {
        visitor.accept_row(cr.key(), cr.deleted_at(), read_row_marker(cr.marker()));

        struct cell_visitor {
            mutation_partition_visitor& _visitor;

            void accept_atomic_cell(column_id id, atomic_cell ac) const {
               _visitor.accept_row_cell(id, ac);
            }
            void accept_collection(column_id id, collection_mutation cm) const {
               _visitor.accept_row_cell(id, cm);
            }
        };
        read_and_visit_row(cr.cells(), cm, column_kind::regular_column, cell_visitor{visitor});
    }
}

mutation_partition_view mutation_partition_view::from_view(ser::mutation_partition_view v)
{
    return { v.v };
}

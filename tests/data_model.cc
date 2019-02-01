/*
 * Copyright (C) 2019 ScyllaDB
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

#include "data_model.hh"

#include <boost/algorithm/string/join.hpp>

#include "schema_builder.hh"

namespace tests::data_model {

void mutation_description::remove_column(row& r, const sstring& name) {
    auto it = boost::range::find_if(r, [&] (const cell& c) {
        return c.column_name == name;
    });
    if (it != r.end()) {
        r.erase(it);
    }
}

mutation_description::mutation_description(key partition_key)
    : _partition_key(std::move(partition_key))
{ }

void mutation_description::add_static_cell(const sstring& column, value v) {
    _static_row.emplace_back(cell { column, std::move(v) });
}

void mutation_description::add_static_expiring_cell(const sstring& column, atomic_value v,
                                                    gc_clock::duration ttl,
                                                    gc_clock::time_point expiry_point) {
    _static_row.emplace_back(cell { column, std::move(v), expiry_info { ttl, expiry_point } });
}

void mutation_description::add_clustered_cell(const key& ck, const sstring& column, value v) {
    _clustered_rows[ck].cells.emplace_back(cell { column, std::move(v) });
}

void mutation_description::add_clustered_expiring_cell(const key& ck, const sstring& column, atomic_value v,
                                                       gc_clock::duration ttl, gc_clock::time_point expiry_point) {
    _clustered_rows[ck].cells.emplace_back(cell { column, std::move(v), expiry_info { ttl, expiry_point } });
}

void mutation_description::add_clustered_row_marker(const key& ck) {
    _clustered_rows[ck].marker = data_timestamp;
}

void mutation_description::remove_static_column(const sstring& name) {
    remove_column(_static_row, name);
}

void mutation_description::remove_regular_column(const sstring& name) {
    for (auto& [ ckey, cr ] : _clustered_rows) {
        (void)ckey;
        remove_column(cr.cells, name);
    }
}

void mutation_description::add_range_tombstone(const key& start, const key& end) {
    _range_tombstones.emplace_back(range_tombstone { start, end });
}

mutation mutation_description::build(schema_ptr s) const {
    auto m = mutation(s, partition_key::from_exploded(*s, _partition_key));
    for (auto& [ column, value_or_collection, expiring ] : _static_row) {
        auto cdef = s->get_column_definition(utf8_type->decompose(column));
        assert(cdef);
        std::visit(make_visitor(
            [&] (const atomic_value& v) {
                assert(cdef->is_atomic());
                if (!expiring) {
                    m.set_static_cell(*cdef, atomic_cell::make_live(*cdef->type, data_timestamp, v));
                } else {
                    m.set_static_cell(*cdef, atomic_cell::make_live(*cdef->type, data_timestamp, v,
                                                                    expiring->expiry_point, expiring->ttl));
                }
            },
            [&] (const collection& c) {
                assert(!cdef->is_atomic());
                assert(!expiring);
                auto ctype = static_pointer_cast<const collection_type_impl>(cdef->type);
                collection_type_impl::mutation mut;
                for (auto& [ key, value ] : c) {
                    mut.cells.emplace_back(key, atomic_cell::make_live(*ctype->value_comparator(), data_timestamp,
                                                                        value, atomic_cell::collection_member::yes));
                }
                m.set_static_cell(*cdef, ctype->serialize_mutation_form(std::move(mut)));
            }
        ), value_or_collection);
    }
    for (auto& [ ckey, cr ] : _clustered_rows) {
        auto& [ marker, cells ] = cr;
        auto ck = clustering_key::from_exploded(*s, ckey);
        for (auto& [ column, value_or_collection, expiring ] : cells) {
            auto cdef = s->get_column_definition(utf8_type->decompose(column));
            assert(cdef);
            std::visit(make_visitor(
            [&] (const atomic_value& v) {
                    assert(cdef->is_atomic());
                    if (!expiring) {
                        m.set_clustered_cell(ck, *cdef, atomic_cell::make_live(*cdef->type, data_timestamp, v));
                    } else {
                        m.set_clustered_cell(ck, *cdef, atomic_cell::make_live(*cdef->type, data_timestamp, v,
                                                                               expiring->expiry_point, expiring->ttl));
                    }
                },
            [&] (const collection& c) {
                    assert(!cdef->is_atomic());
                    auto ctype = static_pointer_cast<const collection_type_impl>(cdef->type);
                    collection_type_impl::mutation mut;
                    for (auto& [ key, value ] : c) {
                        mut.cells.emplace_back(key, atomic_cell::make_live(*ctype->value_comparator(), data_timestamp,
                                                                        value, atomic_cell::collection_member::yes));
                    }
                    m.set_clustered_cell(ck, *cdef, ctype->serialize_mutation_form(std::move(mut)));
                }
            ), value_or_collection);
        }
        if (marker != api::missing_timestamp) {
            m.partition().clustered_row(*s, ckey).apply(row_marker(marker));
        }
    }
    clustering_key::less_compare cmp(*s);
    for (auto& [ a, b ] : _range_tombstones) {
        auto start = clustering_key::from_exploded(*s, a);
        auto stop = clustering_key::from_exploded(*s, b);
        if (cmp(stop, start)) {
            std::swap(start, stop);
        }
        auto rt = ::range_tombstone(std::move(start), bound_kind::excl_start,
                                    std::move(stop), bound_kind::excl_end,
                                    tombstone(previously_removed_column_timestamp, gc_clock::time_point()));
        m.partition().apply_delete(*s, std::move(rt));
    }
    return m;
}

std::vector<table_description::column>::iterator table_description::find_column(std::vector<column>& columns, const sstring& name) {
    return boost::range::find_if(columns, [&] (const column& c) {
        return std::get<sstring>(c) == name;
    });
}

void table_description::add_column(std::vector<column>& columns, const sstring& name, data_type type) {
    assert(find_column(columns, name) == columns.end());
    columns.emplace_back(name, type);
}

void table_description::add_old_column(const sstring& name, data_type type) {
    _removed_columns.emplace_back(removed_column { name, type, previously_removed_column_timestamp });
}

void table_description::remove_column(std::vector<column>& columns, const sstring& name) {
    auto it = find_column(columns, name);
    assert(it != columns.end());
    _removed_columns.emplace_back(removed_column { name, std::get<data_type>(*it), column_removal_timestamp });
    columns.erase(it);
}

void table_description::alter_column_type(std::vector<column>& columns, const sstring& name, data_type new_type) {
    auto it = find_column(columns, name);
    assert(it != columns.end());
    std::get<data_type>(*it) = new_type;
}

schema_ptr table_description::build_schema() const {
    auto sb = schema_builder("ks", "cf");
    for (auto&& [ name, type ] : _partition_key) {
        sb.with_column(utf8_type->decompose(name), type, column_kind::partition_key);
    }
    for (auto&& [ name, type ] : _clustering_key) {
        sb.with_column(utf8_type->decompose(name), type, column_kind::clustering_key);
    }
    for (auto&& [ name, type ] : _static_columns) {
        sb.with_column(utf8_type->decompose(name), type, column_kind::static_column);
    }
    for (auto&& [ name, type ] : _regular_columns) {
        sb.with_column(utf8_type->decompose(name), type);
    }

    for (auto&& [ name, type, timestamp ] : _removed_columns) {
        sb.without_column(name, type, timestamp);
    }

    return sb.build();
}

std::vector<mutation> table_description::build_mutations(schema_ptr s) const {
    auto ms = boost::copy_range<std::vector<mutation>>(
        _mutations | boost::adaptors::transformed([&] (const mutation_description& md) {
            return md.build(s);
        })
    );
    boost::sort(ms, mutation_decorated_key_less_comparator());
    return ms;
}

table_description::table_description(std::vector<column> partition_key, std::vector<column> clustering_key)
    : _partition_key(std::move(partition_key))
    , _clustering_key(std::move(clustering_key))
{ }

void table_description::add_static_column(const sstring& name, data_type type) {
    _change_log.emplace_back(format("added static column \'{}\' of type \'{}\'", name, type->as_cql3_type()->to_string()));
    add_column(_static_columns, name, type);
}

void table_description::add_regular_column(const sstring& name, data_type type) {
    _change_log.emplace_back(format("added regular column \'{}\' of type \'{}\'", name, type->as_cql3_type()->to_string()));
    add_column(_regular_columns, name, type);
}

void table_description::add_old_static_column(const sstring& name, data_type type) {
    add_old_column(name, type);
}

void table_description::add_old_regular_column(const sstring& name, data_type type) {
    add_old_column(name, type);
}

void table_description::remove_static_column(const sstring& name) {
    _change_log.emplace_back(format("removed static column \'{}\'", name));
    remove_column(_static_columns, name);
    for (auto& m : _mutations) {
        m.remove_static_column(name);
    }
}

void table_description::remove_regular_column(const sstring& name) {
    _change_log.emplace_back(format("removed regular column \'{}\'", name));
    remove_column(_regular_columns, name);
    for (auto& m : _mutations) {
        m.remove_regular_column(name);
    }
}

void table_description::alter_partition_column_type(const sstring& name, data_type new_type) {
    _change_log.emplace_back(format("altered partition column \'{}\' type to \'{}\'", name, new_type->as_cql3_type()->to_string()));
    alter_column_type(_partition_key, name, new_type);
}

void table_description::alter_clustering_column_type(const sstring& name, data_type new_type) {
    _change_log.emplace_back(format("altered clustering column \'{}\' type to \'{}\'", name, new_type->as_cql3_type()->to_string()));
    alter_column_type(_clustering_key, name, new_type);
}

void table_description::alter_static_column_type(const sstring& name, data_type new_type) {
    _change_log.emplace_back(format("altered static column \'{}\' type to \'{}\'", name, new_type->as_cql3_type()->to_string()));
    alter_column_type(_static_columns, name, new_type);
}

void table_description::alter_regular_column_type(const sstring& name, data_type new_type) {
    _change_log.emplace_back(format("altered regular column \'{}\' type to \'{}\'", name, new_type->as_cql3_type()->to_string()));
    alter_column_type(_regular_columns, name, new_type);
}

void table_description::rename_partition_column(const sstring& from, const sstring& to) {
    _change_log.emplace_back(format("renamed partition column \'{}\' to \'{}\'", from, to));
    auto it = find_column(_partition_key, from);
    assert(it != _partition_key.end());
    std::get<sstring>(*it) = to;
}
void table_description::rename_clustering_column(const sstring& from, const sstring& to) {
    _change_log.emplace_back(format("renamed clustering column \'{}\' to \'{}\'", from, to));
    auto it = find_column(_clustering_key, from);
    assert(it != _clustering_key.end());
    std::get<sstring>(*it) = to;
}

table_description::table table_description::build() const {
    auto s = build_schema();
    return { boost::algorithm::join(_change_log, "\n"), s, build_mutations(s) };
}

}

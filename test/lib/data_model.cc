/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "test/lib/data_model.hh"

#include <boost/algorithm/string/join.hpp>
#include <boost/range/algorithm/sort.hpp>

#include "schema_builder.hh"
#include "concrete_types.hh"

namespace tests::data_model {


mutation_description::atomic_value::atomic_value(bytes value, api::timestamp_type timestamp) : value(std::move(value)), timestamp(timestamp)
{ }

mutation_description::atomic_value::atomic_value(bytes value, api::timestamp_type timestamp, gc_clock::duration ttl, gc_clock::time_point expiry_point)
    : value(std::move(value)), timestamp(timestamp), expiring(expiry_info{ttl, expiry_point})
{ }

mutation_description::collection::collection(std::initializer_list<collection_element> elements) : elements(elements)
{ }

mutation_description::collection::collection(std::vector<collection_element> elements) : elements(std::move(elements))
{ }

mutation_description::row_marker::row_marker(api::timestamp_type timestamp) : timestamp(timestamp)
{ }

mutation_description::row_marker::row_marker(api::timestamp_type timestamp, gc_clock::duration ttl, gc_clock::time_point expiry_point)
    : timestamp(timestamp), expiring(expiry_info{ttl, expiry_point})
{ }

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

void mutation_description::set_partition_tombstone(tombstone partition_tombstone) {
    _partition_tombstone = partition_tombstone;
}

void mutation_description::add_static_cell(const sstring& column, value v) {
    _static_row.emplace_back(cell { column, std::move(v) });
}

void mutation_description::add_clustered_cell(const key& ck, const sstring& column, value v) {
    _clustered_rows[ck].cells.emplace_back(cell { column, std::move(v) });
}

void mutation_description::add_clustered_row_marker(const key& ck, row_marker marker) {
    _clustered_rows[ck].marker = marker;
}

void mutation_description::add_clustered_row_tombstone(const key& ck, row_tombstone tomb) {
    _clustered_rows[ck].tomb = tomb;
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

void mutation_description::add_range_tombstone(const key& start, const key& end, tombstone tomb) {
    add_range_tombstone(nonwrapping_range<key>::make(start, end), tomb);
}

void mutation_description::add_range_tombstone(nonwrapping_range<key> range, tombstone tomb) {
    _range_tombstones.emplace_back(range_tombstone { std::move(range), tomb });
}

mutation mutation_description::build(schema_ptr s) const {
    auto m = mutation(s, partition_key::from_exploded(*s, _partition_key));
    m.partition().apply(_partition_tombstone);
    for (auto& [ column, value_or_collection ] : _static_row) {
        auto cdef = s->get_column_definition(utf8_type->decompose(column));
        assert(cdef);
        std::visit(make_visitor(
            [&] (const atomic_value& v) {
                assert(cdef->is_atomic());
                if (!v.expiring) {
                    m.set_static_cell(*cdef, atomic_cell::make_live(*cdef->type, v.timestamp, v.value));
                } else {
                    m.set_static_cell(*cdef, atomic_cell::make_live(*cdef->type, v.timestamp, v.value,
                                                                    v.expiring->expiry_point, v.expiring->ttl));
                }
            },
            [&] (const collection& c) {
                assert(!cdef->is_atomic());

                auto get_value_type = visit(*cdef->type, make_visitor(
                    [] (const collection_type_impl& ctype) -> std::function<const abstract_type&(bytes_view)> {
                        return [&] (bytes_view) -> const abstract_type& { return *ctype.value_comparator(); };
                    },
                    [] (const user_type_impl& utype) -> std::function<const abstract_type&(bytes_view)> {
                        return [&] (bytes_view key) -> const abstract_type& { return *utype.type(deserialize_field_index(key)); };
                    },
                    [] (const abstract_type& o) -> std::function<const abstract_type&(bytes_view)> {
                        assert(false);
                    }
                ));

                collection_mutation_description mut;
                mut.tomb = c.tomb;
                for (auto& [ key, value ] : c.elements) {
                    if (!value.expiring) {
                        mut.cells.emplace_back(key, atomic_cell::make_live(get_value_type(key), value.timestamp,
                                                                            value.value, atomic_cell::collection_member::yes));
                    } else {
                        mut.cells.emplace_back(key, atomic_cell::make_live(get_value_type(key),
                                                                           value.timestamp,
                                                                           value.value,
                                                                           value.expiring->expiry_point,
                                                                           value.expiring->ttl,
                                                                           atomic_cell::collection_member::yes));
                    }
                }
                m.set_static_cell(*cdef, mut.serialize(*cdef->type));
            }
        ), value_or_collection);
    }
    for (auto& [ ckey, cr ] : _clustered_rows) {
        auto& [ marker, tomb, cells ] = cr;
        auto ck = clustering_key::from_exploded(*s, ckey);
        for (auto& [ column, value_or_collection ] : cells) {
            auto cdef = s->get_column_definition(utf8_type->decompose(column));
            assert(cdef);
            std::visit(make_visitor(
            [&] (const atomic_value& v) {
                    assert(cdef->is_atomic());
                    if (!v.expiring) {
                        m.set_clustered_cell(ck, *cdef, atomic_cell::make_live(*cdef->type, v.timestamp, v.value));
                    } else {
                        m.set_clustered_cell(ck, *cdef, atomic_cell::make_live(*cdef->type, v.timestamp, v.value,
                                                                               v.expiring->expiry_point, v.expiring->ttl));
                    }
                },
            [&] (const collection& c) {
                    assert(!cdef->is_atomic());

                    auto get_value_type = visit(*cdef->type, make_visitor(
                        [] (const collection_type_impl& ctype) -> std::function<const abstract_type&(bytes_view)> {
                            return [&] (bytes_view) -> const abstract_type& { return *ctype.value_comparator(); };
                        },
                        [] (const user_type_impl& utype) -> std::function<const abstract_type&(bytes_view)> {
                            return [&] (bytes_view key) -> const abstract_type& { return *utype.type(deserialize_field_index(key)); };
                        },
                        [] (const abstract_type& o) -> std::function<const abstract_type&(bytes_view)> {
                            assert(false);
                        }
                    ));

                    collection_mutation_description mut;
                    mut.tomb = c.tomb;
                    for (auto& [ key, value ] : c.elements) {
                        if (!value.expiring) {
                            mut.cells.emplace_back(key, atomic_cell::make_live(get_value_type(key), value.timestamp,
                                                                            value.value, atomic_cell::collection_member::yes));
                        } else {
                            mut.cells.emplace_back(key, atomic_cell::make_live(get_value_type(key),
                                                                               value.timestamp,
                                                                               value.value,
                                                                               value.expiring->expiry_point,
                                                                               value.expiring->ttl,
                                                                               atomic_cell::collection_member::yes));
                        }

                    }
                    m.set_clustered_cell(ck, *cdef, mut.serialize(*cdef->type));
                }
            ), value_or_collection);
        }
        if (marker.timestamp != api::missing_timestamp) {
            if (marker.expiring) {
                m.partition().clustered_row(*s, ckey).apply(::row_marker(marker.timestamp, marker.expiring->ttl, marker.expiring->expiry_point));
            } else {
                m.partition().clustered_row(*s, ckey).apply(::row_marker(marker.timestamp));
            }
        }
        if (tomb) {
            m.partition().clustered_row(*s, ckey).apply(tomb);
        }
    }
    clustering_key::less_compare cmp(*s);
    for (auto& [ range, tomb ] : _range_tombstones) {
        auto clustering_range = range.transform([&s = *s] (const key& k) {
            return clustering_key::from_exploded(s, k);
        });
        if (!clustering_range.is_singular()) {
            auto start = clustering_range.start();
            auto end = clustering_range.end();
            if (start && end && cmp(end->value(), start->value())) {
                clustering_range = nonwrapping_range<clustering_key>(std::move(end), std::move(start));
            }
        }
        auto rt = ::range_tombstone(
                bound_view::from_range_start(clustering_range),
                bound_view::from_range_end(clustering_range),
                tomb);
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

schema_ptr table_description::build_schema(schema_registry& registry) const {
    auto sb = schema_builder(registry, "ks", "cf");
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
    _change_log.emplace_back(format("added static column \'{}\' of type \'{}\'", name, type->as_cql3_type().to_string()));
    add_column(_static_columns, name, type);
}

void table_description::add_regular_column(const sstring& name, data_type type) {
    _change_log.emplace_back(format("added regular column \'{}\' of type \'{}\'", name, type->as_cql3_type().to_string()));
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
    _change_log.emplace_back(format("altered partition column \'{}\' type to \'{}\'", name, new_type->as_cql3_type().to_string()));
    alter_column_type(_partition_key, name, new_type);
}

void table_description::alter_clustering_column_type(const sstring& name, data_type new_type) {
    _change_log.emplace_back(format("altered clustering column \'{}\' type to \'{}\'", name, new_type->as_cql3_type().to_string()));
    alter_column_type(_clustering_key, name, new_type);
}

void table_description::alter_static_column_type(const sstring& name, data_type new_type) {
    _change_log.emplace_back(format("altered static column \'{}\' type to \'{}\'", name, new_type->as_cql3_type().to_string()));
    alter_column_type(_static_columns, name, new_type);
}

void table_description::alter_regular_column_type(const sstring& name, data_type new_type) {
    _change_log.emplace_back(format("altered regular column \'{}\' type to \'{}\'", name, new_type->as_cql3_type().to_string()));
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

table_description::table table_description::build(schema_registry& registry) const {
    auto s = build_schema(registry);
    return { boost::algorithm::join(_change_log, "\n"), s, build_mutations(s) };
}

}

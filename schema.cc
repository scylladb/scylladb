/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "utils/UUID_gen.hh"
#include "cql3/column_identifier.hh"
#include "schema.hh"
#include <boost/algorithm/cxx11/any_of.hpp>


template<typename Sequence>
std::vector<data_type>
get_column_types(const Sequence& column_definitions) {
    std::vector<data_type> result;
    for (auto&& col : column_definitions) {
        result.push_back(col.type);
    }
    return result;
}

::shared_ptr<cql3::column_specification>
schema::make_column_specification(const column_definition& def) {
    auto id = ::make_shared<cql3::column_identifier>(def.name(), column_name_type(def));
    return ::make_shared<cql3::column_specification>(_raw._ks_name, _raw._cf_name, std::move(id), def.type);
}

void
schema::build_columns(const std::vector<column>& columns, column_definition::column_kind kind,
    std::vector<column_definition>& dst)
{
    dst.reserve(columns.size());
    for (column_id i = 0; i < columns.size(); i++) {
        auto& col = columns[i];
        dst.emplace_back(std::move(col.name), std::move(col.type), i, kind);
        column_definition& def = dst.back();
        def.column_specification = make_column_specification(def);
    }
}

void schema::rebuild() {
    _partition_key_type = make_lw_shared<compound_type<>>(get_column_types(_raw._partition_key));
    _clustering_key_type = make_lw_shared<compound_type<>>(get_column_types(_raw._clustering_key));
    _clustering_key_prefix_type = make_lw_shared(_clustering_key_type->as_prefix());

    _columns_by_name.clear();
    _regular_columns_by_name.clear();

    for (const column_definition& def : all_columns_in_select_order()) {
        _columns_by_name[def.name()] = &def;
    }

    for (const column_definition& def : regular_columns()) {
        _regular_columns_by_name[def.name()] = &def;
    }
}

schema::raw_schema::raw_schema(utils::UUID id)
    : _id(id)
{ }

schema::schema(std::experimental::optional<utils::UUID> id,
    sstring ks_name,
    sstring cf_name,
    std::vector<column> partition_key,
    std::vector<column> clustering_key,
    std::vector<column> regular_columns,
    std::vector<column> static_columns,
    data_type regular_column_name_type,
    sstring comment)
        : _raw(id ? *id : utils::UUID_gen::get_time_UUID())
        , _regular_columns_by_name(serialized_compare(regular_column_name_type))
{
    _raw._comment = std::move(comment);
    _raw._ks_name = std::move(ks_name);
    _raw._cf_name = std::move(cf_name);
    _raw._regular_column_name_type = regular_column_name_type;

    build_columns(partition_key, column_definition::column_kind::PARTITION, _raw._partition_key);
    build_columns(clustering_key, column_definition::column_kind::CLUSTERING, _raw._clustering_key);

    std::sort(regular_columns.begin(), regular_columns.end(), column::name_compare(regular_column_name_type));
    build_columns(regular_columns, column_definition::column_kind::REGULAR, _raw._regular_columns);

    std::sort(static_columns.begin(), static_columns.end(), column::name_compare(utf8_type));
    build_columns(static_columns, column_definition::column_kind::STATIC, _raw._static_columns);

    rebuild();
}

schema::schema(const schema& o)
    : _raw(o._raw)
    , _regular_columns_by_name(serialized_compare(_raw._regular_column_name_type))
{
    rebuild();
}

bool
schema::has_collections() {
    return boost::algorithm::any_of(all_columns_in_select_order(), [] (const column_definition& cdef) {
        return cdef.type->is_collection();
    });
}

column_definition::column_definition(bytes name, data_type type, column_id id, column_kind kind)
    : _name(std::move(name))
    , type(std::move(type))
    , id(id)
    , kind(kind)
{ }

const column_definition*
schema::get_column_definition(const bytes& name) {
    auto i = _columns_by_name.find(name);
    if (i == _columns_by_name.end()) {
        return nullptr;
    }
    return i->second;
}

const sstring&
column_definition::name_as_text() const {
    return column_specification->name->text();
}

const bytes&
column_definition::name() const {
    return _name;
}

bool
column_definition::is_compact_value() const {
    warn(unimplemented::cause::COMPACT_TABLES);
    return false;
}

// Based on org.apache.cassandra.config.CFMetaData#generateLegacyCfId
utils::UUID
generate_legacy_id(const sstring& ks_name, const sstring& cf_name) {
    return utils::UUID_gen::get_name_UUID(ks_name + cf_name);
}

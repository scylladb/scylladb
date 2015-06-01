/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "utils/UUID_gen.hh"
#include "cql3/column_identifier.hh"
#include "schema.hh"
#include "schema_builder.hh"
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

void schema::rebuild() {
    _partition_key_type = make_lw_shared<compound_type<>>(get_column_types(partition_key_columns()));
    _clustering_key_type = make_lw_shared<compound_type<>>(get_column_types(clustering_key_columns()));
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

schema::schema(const raw_schema& raw)
    : _raw(raw)
    , _offsets([this] {
        auto& cols = _raw._columns;
        std::array<size_t, 4> count = { 0, 0, 0, 0 };
        auto i = cols.begin();
        auto e = cols.end();
        for (auto k : { column_kind::partition_key, column_kind::clustering_key, column_kind::static_column, column_kind::regular_column }) {
            auto j = std::partition(i, e, [k](const auto& c) {
                return c.kind == k;
            });
            count[size_t(k)] = std::distance(i, j);
            i = j;
        }
        return std::array<size_t, 3> { count[0], count[0] + count[1], count[0] + count[1] + count[2] };
    }())
    , _regular_columns_by_name(serialized_compare(_raw._regular_column_name_type))
{
    struct name_compare {
        data_type type;
        name_compare(data_type type) : type(type) {}
        bool operator()(const column_definition& cd1, const column_definition& cd2) const {
            return type->less(cd1.name(), cd2.name());
        }
    };

    std::sort(
            _raw._columns.begin() + column_offset(column_kind::static_column),
            _raw._columns.begin()
                    + column_offset(column_kind::regular_column),
            name_compare(utf8_type));
    std::sort(
            _raw._columns.begin()
                    + column_offset(column_kind::regular_column),
            _raw._columns.end(), name_compare(regular_column_name_type()));

    column_id id = 0;
    for (auto& def : _raw._columns) {
        def.column_specification = make_column_specification(def);
        def.id = id - column_offset(def.kind);
        ++id;
    }

    rebuild();
}

schema::schema(std::experimental::optional<utils::UUID> id,
    sstring ks_name,
    sstring cf_name,
    std::vector<column> partition_key,
    std::vector<column> clustering_key,
    std::vector<column> regular_columns,
    std::vector<column> static_columns,
    data_type regular_column_name_type,
    sstring comment)
    : schema([&] {
        raw_schema raw(id ? *id : utils::UUID_gen::get_time_UUID());

        raw._comment = std::move(comment);
        raw._ks_name = std::move(ks_name);
        raw._cf_name = std::move(cf_name);
        raw._regular_column_name_type = regular_column_name_type;

        auto build_columns = [&raw](std::vector<column>& columns, column_kind kind) {
            for (auto& sc : columns) {
                raw._columns.emplace_back(std::move(sc.name), std::move(sc.type), kind);
            }
        };

        build_columns(partition_key, column_kind::partition_key);
        build_columns(clustering_key, column_kind::clustering_key);
        build_columns(static_columns, column_kind::static_column);
        build_columns(regular_columns, column_kind::regular_column);

        return raw;
    }())
{}

schema::schema(const schema& o)
    : _raw(o._raw)
    , _offsets(o._offsets)
    , _regular_columns_by_name(serialized_compare(_raw._regular_column_name_type))
{
    rebuild();
}

bool
schema::has_collections() const {
    return boost::algorithm::any_of(all_columns_in_select_order(), [] (const column_definition& cdef) {
        return cdef.type->is_collection();
    });
}

index_info::index_info(::index_type idx_type,
        std::experimental::optional<sstring> idx_name,
        std::experimental::optional<index_options_map> idx_options)
    : index_type(idx_type), index_name(idx_name), index_options(idx_options)
{}

column_definition::column_definition(bytes name, data_type type, column_kind kind, index_info idx)
        : _name(std::move(name)), type(std::move(type)), kind(kind), idx_info(std::move(idx))
{}

const column_definition*
schema::get_column_definition(const bytes& name) const {
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

schema_builder::schema_builder(const sstring& ks_name, const sstring& cf_name,
        std::experimental::optional<utils::UUID> id, data_type rct)
        : _raw(id ? *id : utils::UUID_gen::get_time_UUID())
{
    _raw._ks_name = ks_name;
    _raw._cf_name = cf_name;
    _raw._regular_column_name_type = rct;
}

schema_builder::schema_builder(const schema_ptr s)
    : schema_builder(s->_raw)
{}

schema_builder::schema_builder(const schema::raw_schema& raw)
    : _raw(raw)
{}

column_definition& schema_builder::find_column(const cql3::column_identifier& c) {
    auto i = std::find_if(_raw._columns.begin(), _raw._columns.end(), [c](auto& p) {
        return p.name() == c.name();
     });
    if (i != _raw._columns.end()) {
        return *i;
    }
    throw std::invalid_argument(sprint("No such column %s", c.name()));
}

schema_builder& schema_builder::with_column(const column_definition& c) {
    return with_column(bytes(c.name()), data_type(c.type), index_info(c.idx_info), column_kind(c.kind));
}

schema_builder& schema_builder::with_column(bytes name, data_type type, column_kind kind) {
    return with_column(name, type, index_info(), kind);
}

schema_builder& schema_builder::with_column(bytes name, data_type type, index_info info, column_kind kind) {
    _raw._columns.emplace_back(name, type, kind, info);
    return *this;
}

schema_ptr schema_builder::build() {
    return make_lw_shared<schema>(schema(_raw));
}

/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "utils/UUID_gen.hh"
#include "cql3/column_identifier.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptor/transformed.hpp>

constexpr int32_t schema::NAME_LENGTH;

const std::experimental::optional<sstring> schema::DEFAULT_COMPRESSOR = sstring("LZ4Compressor");

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
        std::array<size_t, 5> count = { 0, 0, 0, 0, 0 };
        auto i = cols.begin();
        auto e = cols.end();
        for (auto k : { column_kind::partition_key, column_kind::clustering_key, column_kind::static_column, column_kind::regular_column, column_kind::compact_column }) {
            auto j = std::stable_partition(i, e, [k](const auto& c) {
                return c.kind == k;
            });
            count[size_t(k)] = std::distance(i, j);
            i = j;
        }
        return std::array<size_t, 4> { count[0], count[0] + count[1], count[0] + count[1] + count[2], count[0] + count[1] + count[2] + count[3] };
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

    thrift()._compound = is_compound();

    std::sort(
            _raw._columns.begin() + column_offset(column_kind::static_column),
            _raw._columns.begin()
                    + column_offset(column_kind::regular_column),
            name_compare(utf8_type));
    std::sort(
            _raw._columns.begin()
                    + column_offset(column_kind::regular_column),
            _raw._columns.end(), name_compare(regular_column_name_type()));

    std::sort(_raw._columns.begin(),
              _raw._columns.begin() + column_offset(column_kind::clustering_key),
              [] (auto x, auto y) { return x.id < y.id; });
    std::sort(_raw._columns.begin() + column_offset(column_kind::clustering_key),
              _raw._columns.begin() + column_offset(column_kind::static_column),
              [] (auto x, auto y) { return x.id < y.id; });

    column_id id = 0;
    for (auto& def : _raw._columns) {
        def.column_specification = make_column_specification(def);
        assert(!def.id || def.id == id - column_offset(def.kind));
        def.id = id - column_offset(def.kind);

        def._thrift_bits = column_definition::thrift_bits();

        {
            // is_on_all_components
            // TODO : In origin, this predicate is "componentIndex == null", which is true in
            // a number of cases, some of which I've most likely missed...
            switch (def.kind) {
            case column_kind::partition_key:
                // In origin, ci == null is true for a PK column where CFMetaData "keyValidator" is non-composite.
                // Which is true of #pk == 1
                def._thrift_bits.is_on_all_components = partition_key_size() == 1;
                break;
            default:
                // Or any other column where "comparator" is not compound
                def._thrift_bits.is_on_all_components = !thrift().has_compound_comparator();
                break;
            }
        }

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

sstring schema::thrift_key_validator() const {
    if (partition_key_size() == 1) {
        return partition_key_columns().begin()->type->name();
    } else {
        sstring type_params = ::join(", ", partition_key_columns()
                            | boost::adaptors::transformed(std::mem_fn(&column_definition::type))
                            | boost::adaptors::transformed(std::mem_fn(&abstract_type::name)));
        return "org.apache.cassandra.db.marshal.CompositeType(" + type_params + ")";
    }
}

bool
schema::has_collections() const {
    return boost::algorithm::any_of(all_columns_in_select_order(), [] (const column_definition& cdef) {
        return cdef.type->is_collection();
    });
}

bool operator==(const schema& x, const schema& y)
{
    return x._raw._id == y._raw._id
        && x._raw._ks_name == y._raw._ks_name
        && x._raw._cf_name == y._raw._cf_name
        && x._raw._columns == y._raw._columns
        && x._raw._comment == y._raw._comment
        && x._raw._default_time_to_live == y._raw._default_time_to_live
        && x._raw._regular_column_name_type->equals(y._raw._regular_column_name_type)
        && x._raw._bloom_filter_fp_chance == y._raw._bloom_filter_fp_chance;
}

index_info::index_info(::index_type idx_type,
        std::experimental::optional<sstring> idx_name,
        std::experimental::optional<index_options_map> idx_options)
    : index_type(idx_type), index_name(idx_name), index_options(idx_options)
{}

column_definition::column_definition(bytes name, data_type type, column_kind kind, column_id component_index, index_info idx)
        : _name(std::move(name)), type(std::move(type)), id(component_index), kind(kind), idx_info(std::move(idx))
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

bool column_definition::is_on_all_components() const {
    return _thrift_bits.is_on_all_components;
}

bool operator==(const column_definition& x, const column_definition& y)
{
    return x._name == y._name
        && x.type->equals(y.type)
        && x.id == y.id
        && x.kind == y.kind;
}

// Based on org.apache.cassandra.config.CFMetaData#generateLegacyCfId
utils::UUID
generate_legacy_id(const sstring& ks_name, const sstring& cf_name) {
    return utils::UUID_gen::get_name_UUID(ks_name + cf_name);
}

bool thrift_schema::has_compound_comparator() const {
    return _compound;
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

void schema_builder::add_default_index_names(database& db) {
    auto s = db.find_schema(ks_name(), cf_name());

    if (s) {
        for (auto& sc : _raw._columns) {
            if (sc.idx_info.index_type == index_type::none) {
                continue;
            }
            auto* c = s->get_column_definition(sc.name());
            if (c == nullptr || !c->idx_info.index_name) {
                continue;
            }
            if (sc.idx_info.index_name
                    && sc.idx_info.index_name != c->idx_info.index_name) {
                throw exceptions::configuration_exception(
                        sprint(
                                "Can't modify index name: was '%s' changed to '%s'",
                                *c->idx_info.index_name,
                                *sc.idx_info.index_name));

            }
            sc.idx_info.index_name = c->idx_info.index_name;
        }
    }


    auto existing_names = db.existing_index_names();
    for (auto& sc : _raw._columns) {
        if (sc.idx_info.index_type != index_type::none && sc.idx_info.index_name) {
            sstring base_name = cf_name() + "_" + *sc.idx_info.index_name + "_idx";
            auto i = std::remove_if(base_name.begin(), base_name.end(), [](char c) {
               return ::isspace(c);
            });
            base_name.erase(i, base_name.end());
            auto index_name = base_name;
            int n = 0;
            while (existing_names.count(index_name)) {
                index_name = base_name + "_" + to_sstring(++n);
            }
            sc.idx_info.index_name = index_name;
        }
    }
}

schema_builder& schema_builder::with_column(const column_definition& c) {
    return with_column(bytes(c.name()), data_type(c.type), index_info(c.idx_info), column_kind(c.kind), c.position());
}

schema_builder& schema_builder::with_column(bytes name, data_type type, column_kind kind) {
    return with_column(name, type, index_info(), kind);
}

schema_builder& schema_builder::with_column(bytes name, data_type type, index_info info, column_kind kind) {
    // component_index will be determined by schema cosntructor
    return with_column(name, type, info, kind, 0);
}

schema_builder& schema_builder::with_column(bytes name, data_type type, index_info info, column_kind kind, column_id component_index) {
    _raw._columns.emplace_back(name, type, kind, component_index, info);
    return *this;
}

schema_ptr schema_builder::build() {
    return make_lw_shared<schema>(schema(_raw));
}

schema_ptr schema_builder::build(compact_storage cp) {
    schema s(_raw);

    // Dense means that no part of the comparator stores a CQL column name. This means
    // COMPACT STORAGE with at least one columnAliases (otherwise it's a thrift "static" CF).
    s._raw._is_dense = (cp == compact_storage::yes) && (s.clustering_key_size() > 0);

    if (s.clustering_key_size() == 0) {
        if (cp == compact_storage::yes) {
            s._raw._is_compound = false;
        } else {
            s._raw._is_compound = true;
        }
    } else {
        if ((cp == compact_storage::yes) && s.clustering_key_size() == 1) {
            s._raw._is_compound = false;
        } else {
            s._raw._is_compound = true;
        }
    }

    if (s._raw._is_dense) {
        // In Origin, dense CFs always have at least one regular column
        if (s.regular_columns_count() == 0) {
            s._raw._columns.emplace_back(bytes(""), s.regular_column_name_type(), column_kind::regular_column, 0, index_info());
        }

        if (s.regular_columns_count() != 1) {
            throw exceptions::configuration_exception(sprint("Expecting exactly one regular column. Found %d", s.regular_columns_count()));
        }
        s._raw._columns.at(s.column_offset(column_kind::regular_column)).kind = column_kind::compact_column;

        // We need to rebuild the schema in case we added some column. This is way simpler than trying to factor out the relevant code
        // from the constructor
    }
    return make_lw_shared<schema>(schema(s._raw));
}

// Useful functions to manipulate the schema's comparator field
namespace cell_comparator {

static constexpr auto _composite_str = "org.apache.cassandra.db.marshal.CompositeType";
static constexpr auto _collection_str = "org.apache.cassandra.db.marshal.ColumnToCollectionType";

static sstring collection_name(const collection_type& t) {
    sstring collection_str(_collection_str);
    collection_str += "(00000000:" + t->name() + ")";
    return collection_str;
}

static sstring compound_name(const schema& s) {
    sstring compound(_composite_str);

    compound += "(";
    if (s.clustering_key_size()) {
        for (auto &t : s.clustering_key_columns()) {
            compound += t.type->name() + ",";
        }
    } else {
        compound += s.regular_column_name_type()->name() + ",";
    }

    if (s.has_collections()) {
        for (auto& t: s.regular_columns()) {
            if (t.type->is_collection()) {
                auto ct = static_pointer_cast<const collection_type_impl>(t.type);
                compound += collection_name(ct) + ",";
            }
        }
    }
    // last one will be a ',', just replace it.
    compound.back() = ')';
    return compound;
}

sstring to_sstring(const schema& s) {
    if (!s.is_compound()) {
        return s.regular_column_name_type()->name();
    } else {
        return compound_name(s);
    }
}

bool check_compound(sstring comparator) {
    static sstring compound(_composite_str);
    return comparator.compare(0, compound.size(), compound) == 0;
}
}

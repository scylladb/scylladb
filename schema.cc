/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
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

#include "utils/UUID_gen.hh"
#include "cql3/column_identifier.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "md5_hasher.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "db/marshal/type_parser.hh"
#include "version.hh"
#include "schema_registry.hh"
#include <boost/range/algorithm.hpp>

constexpr int32_t schema::NAME_LENGTH;

const std::experimental::optional<sstring> schema::DEFAULT_COMPRESSOR = sstring("LZ4Compressor");

sstring to_sstring(column_kind k) {
    switch (k) {
    case column_kind::partition_key:  return "PARTITION_KEY";
    case column_kind::clustering_key: return "CLUSTERING_COLUMN";
    case column_kind::static_column:  return "STATIC";
    case column_kind::regular_column: return "REGULAR";
    case column_kind::compact_column: return "COMPACT_VALUE";
    }
    throw std::invalid_argument("unknown column kind");
}

sstring to_sstring(index_type t) {
    switch (t) {
    case index_type::keys:       return "KEYS";
    case index_type::custom:     return "CUSTOM";
    case index_type::composites: return "COMPOSITES";
    case index_type::none:       return "null";
    }
    throw std::invalid_argument("unknown index type");
}

template class db::serializer<column_mapping>;

template<>
db::serializer<column_mapping>::serializer(const column_mapping& cm)
        : _item(cm)
        , _size([&cm] {
            size_t size = 2 * data_output::serialized_size<column_count_type>();
            for (auto&& col : cm._columns) {
                size += db::serializer<bytes>(col.name()).size();
                size += db::serializer<sstring>(col.type()->name()).size();
            }
            return size;
        }())
{ }

template<>
void
db::serializer<column_mapping>::write(output& out, const column_mapping& cm) {
    static_assert(std::is_same<column_count_type, uint32_t>::value, "ABI change");
    out.write<column_count_type>(cm._columns.size());
    out.write<column_count_type>(cm._n_static);
    for (const column_mapping::column& col : cm._columns) {
        db::serializer<bytes>(col.name()).write(out);
        db::serializer<sstring>(col.type()->name()).write(out);
    }
}

template<>
void db::serializer<column_mapping>::write(bytes_ostream& out) const {
    auto buf = out.write_place_holder(_size);
    data_output data_out((char*)buf, _size);
    write(data_out, _item);
}

template<>
column_mapping db::serializer<column_mapping>::read(input& in) {
    auto n_columns = in.read<column_count_type>();
    auto n_static = in.read<column_count_type>();
    std::vector<column_mapping::column> columns;
    columns.reserve(n_columns);
    for (column_count_type i = 0; i < n_columns; ++i) {
        auto name = db::serializer<bytes>::read(in);
        auto type_name = db::serializer<sstring>::read(in);
        auto type = db::marshal::type_parser::parse(type_name);
        columns.emplace_back(column_mapping::column{std::move(name), std::move(type)});
    }
    return column_mapping(std::move(columns), n_static);
}

template<>
void db::serializer<column_mapping>::skip(input& in) {
    auto n_columns = in.read<column_count_type>();
    in.read<column_count_type>();
    for (column_count_type i = 0; i < n_columns; ++i) {
        db::serializer<bytes>::skip(in);
        db::serializer<sstring>::skip(in);
    }
}

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
    _clustering_key_type = make_lw_shared<compound_prefix>(get_column_types(clustering_key_columns()));

    _columns_by_name.clear();
    _regular_columns_by_name.clear();

    for (const column_definition& def : all_columns_in_select_order()) {
        _columns_by_name[def.name()] = &def;
    }

    static_assert(row_column_ids_are_ordered_by_name::value, "row columns don't need to be ordered by name");
    if (!std::is_sorted(regular_columns().begin(), regular_columns().end(), column_definition::name_comparator())) {
        throw std::runtime_error("Regular columns should be sorted by name");
    }
    if (!std::is_sorted(static_columns().begin(), static_columns().end(), column_definition::name_comparator())) {
        throw std::runtime_error("Static columns should be sorted by name");
    }

    for (const column_definition& def : regular_columns()) {
        _regular_columns_by_name[def.name()] = &def;
    }

    {
        std::vector<column_mapping::column> cm_columns;
        for (const column_definition& def : boost::range::join(static_columns(), regular_columns())) {
            cm_columns.emplace_back(column_mapping::column{def.name(), def.type});
        }
        _column_mapping = column_mapping(std::move(cm_columns), static_columns_count());
    }
}

const column_mapping& schema::get_column_mapping() const {
    return _column_mapping;
}

schema::raw_schema::raw_schema(utils::UUID id)
    : _id(id)
{ }

schema::schema(const raw_schema& raw)
    : _raw(raw)
    , _offsets([this] {
        if (_raw._columns.size() > std::numeric_limits<column_count_type>::max()) {
            throw std::runtime_error(sprint("Column count limit (%d) overflowed: %d",
                                            std::numeric_limits<column_count_type>::max(), _raw._columns.size()));
        }

        auto& cols = _raw._columns;
        std::array<column_count_type, 5> count = { 0, 0, 0, 0, 0 };
        auto i = cols.begin();
        auto e = cols.end();
        for (auto k : { column_kind::partition_key, column_kind::clustering_key, column_kind::static_column, column_kind::regular_column, column_kind::compact_column }) {
            auto j = std::stable_partition(i, e, [k](const auto& c) {
                return c.kind == k;
            });
            count[column_count_type(k)] = std::distance(i, j);
            i = j;
        }
        return std::array<column_count_type, 4> {
                count[0],
                count[0] + count[1],
                count[0] + count[1] + count[2],
                count[0] + count[1] + count[2] + count[3]
        };
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

        auto dropped_at_it = _raw._dropped_columns.find(def.name_as_text());
        if (dropped_at_it != _raw._dropped_columns.end()) {
            def._dropped_at = std::max(def._dropped_at, dropped_at_it->second);
        }

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
            case column_kind::compact_column:
                // compact values are alone, so they have no index
                def._thrift_bits.is_on_all_components = true;
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
                if (sc.type->is_multi_cell()) {
                    raw._collections.emplace(sc.name, sc.type);
                }
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

schema::~schema() {
    if (_registry_entry) {
        _registry_entry->detach_schema();
    }
}

schema_registry_entry*
schema::registry_entry() const noexcept {
    return _registry_entry;
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
schema::has_multi_cell_collections() const {
    return boost::algorithm::any_of(all_columns_in_select_order(), [] (const column_definition& cdef) {
        return cdef.type->is_collection() && cdef.type->is_multi_cell();
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
        && x._raw._bloom_filter_fp_chance == y._raw._bloom_filter_fp_chance
        && x._raw._compressor_params == y._raw._compressor_params
        && x._raw._is_dense == y._raw._is_dense
        && x._raw._is_compound == y._raw._is_compound
        && x._raw._type == y._raw._type
        && x._raw._gc_grace_seconds == y._raw._gc_grace_seconds
        && x._raw._dc_local_read_repair_chance == y._raw._dc_local_read_repair_chance
        && x._raw._read_repair_chance == y._raw._read_repair_chance
        && x._raw._min_compaction_threshold == y._raw._min_compaction_threshold
        && x._raw._max_compaction_threshold == y._raw._max_compaction_threshold
        && x._raw._min_index_interval == y._raw._min_index_interval
        && x._raw._max_index_interval == y._raw._max_index_interval
        && x._raw._memtable_flush_period == y._raw._memtable_flush_period
        && x._raw._speculative_retry == y._raw._speculative_retry
        && x._raw._compaction_strategy == y._raw._compaction_strategy
        && x._raw._compaction_strategy_options == y._raw._compaction_strategy_options
        && x._raw._caching_options == y._raw._caching_options
        && x._raw._dropped_columns == y._raw._dropped_columns;
#if 0
        && Objects.equal(triggers, other.triggers)
#endif
}

index_info::index_info(::index_type idx_type,
        std::experimental::optional<sstring> idx_name,
        std::experimental::optional<index_options_map> idx_options)
    : index_type(idx_type), index_name(idx_name), index_options(idx_options)
{}

column_definition::column_definition(bytes name, data_type type, column_kind kind, column_id component_index, index_info idx, api::timestamp_type dropped_at)
        : _name(std::move(name)), _dropped_at(dropped_at), type(std::move(type)), id(component_index), kind(kind), idx_info(std::move(idx))
{}

std::ostream& operator<<(std::ostream& os, const column_definition& cd) {
    os << "ColumnDefinition{";
    os << "name=" << cd.name_as_text();
    os << ", type=" << cd.type->name();
    os << ", kind=" << to_sstring(cd.kind);
    os << ", componentIndex=" << (cd.has_component_index() ? std::to_string(cd.component_index()) : "null");
    os << ", indexName=" << (cd.idx_info.index_name ? *cd.idx_info.index_name : "null");
    os << ", indexType=" << to_sstring(cd.idx_info.index_type);
    os << ", droppedAt=" << cd._dropped_at;
    os << "}";
    return os;
}

const column_definition*
schema::get_column_definition(const bytes& name) const {
    auto i = _columns_by_name.find(name);
    if (i == _columns_by_name.end()) {
        return nullptr;
    }
    return i->second;
}

const column_definition&
schema::column_at(column_kind kind, column_id id) const {
    return _raw._columns.at(column_offset(kind) + id);
}

std::ostream& operator<<(std::ostream& os, const schema& s) {
    os << "org.apache.cassandra.config.CFMetaData@" << &s << "[";
    os << "cfId=" << s._raw._id;
    os << ",ksName=" << s._raw._ks_name;
    os << ",cfName=" << s._raw._cf_name;
    os << ",cfType=" << cf_type_to_sstring(s._raw._type);
    os << ",comparator=" << cell_comparator::to_sstring(s);
    os << ",comment=" << s._raw._comment;
    os << ",readRepairChance=" << s._raw._read_repair_chance;
    os << ",dcLocalReadRepairChance=" << s._raw._dc_local_read_repair_chance;
    os << ",gcGraceSeconds=" << s._raw._gc_grace_seconds;
    os << ",defaultValidator=" << s._raw._default_validator->name();
    os << ",keyValidator=" << s.thrift_key_validator();
    os << ",minCompactionThreshold=" << s._raw._min_compaction_threshold;
    os << ",maxCompactionThreshold=" << s._raw._max_compaction_threshold;
    os << ",columnMetadata=[";
    int n = 0;
    for (auto& cdef : s._raw._columns) {
        if (n++ != 0) {
            os << ", ";
        }
        os << cdef;
    }
    os << "]";
    os << ",compactionStrategyClass=class org.apache.cassandra.db.compaction." << sstables::compaction_strategy::name(s._raw._compaction_strategy);
    os << ",compactionStrategyOptions={";
    n = 0;
    for (auto& p : s._raw._compaction_strategy_options) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ",compressionParameters={";
    n = 0;
    for (auto& p : s._raw._compressor_params.get_options() ) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ",bloomFilterFpChance=" << s._raw._bloom_filter_fp_chance;
    os << ",memtableFlushPeriod=" << s._raw._memtable_flush_period;
    os << ",caching=" << s._raw._caching_options.to_sstring();
    os << ",defaultTimeToLive=" << s._raw._default_time_to_live.count();
    os << ",minIndexInterval=" << s._raw._min_index_interval;
    os << ",maxIndexInterval=" << s._raw._max_index_interval;
    os << ",speculativeRetry=" << s._raw._speculative_retry.to_sstring();
    os << ",droppedColumns={}";
    os << ",triggers=[]";
    os << ",isDense=" << std::boolalpha << s._raw._is_dense;
    os << ",version=" << s.version();
    os << ",droppedColumns={";
    n = 0;
    for (auto& dc : s._raw._dropped_columns) {
        if (n++ != 0) {
            os << ", ";
        }
        os << dc.first << " : " << dc.second;
    }
    os << "}";
    os << "]";
    return os;
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
        && x.kind == y.kind
        && x._dropped_at == y._dropped_at;
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
{
    static_assert(schema::row_column_ids_are_ordered_by_name::value, "row columns don't need to be ordered by name");
    // Schema builder may add or remove columns and their ids need to be
    // recomputed in build().
    for (auto& def : _raw._columns | boost::adaptors::filtered([] (auto& def) { return !def.is_primary_key(); })) {
            def.id = 0;
    }
}

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
    if (type->is_multi_cell()) {
        with_collection(name, type);
    }
    return *this;
}

schema_builder& schema_builder::without_column(bytes name)
{
    auto it = boost::range::find_if(_raw._columns, [&] (auto& column) {
        return column.name() == name;
    });
    assert(it != _raw._columns.end());
    auto now = api::new_timestamp();
    auto ret = _raw._dropped_columns.emplace(it->name_as_text(), now);
    if (!ret.second) {
        ret.first->second = std::max(ret.first->second, now);
    }
    _raw._columns.erase(it);
    return *this;
}

schema_builder& schema_builder::without_column(sstring name, api::timestamp_type timestamp)
{
    auto ret = _raw._dropped_columns.emplace(name, timestamp);
    if (!ret.second) {
        ret.first->second = std::max(ret.first->second, timestamp);
    }
    return *this;
}

schema_builder& schema_builder::with_column_rename(bytes from, bytes to)
{
    auto it = std::find_if(_raw._columns.begin(), _raw._columns.end(), [&] (auto& col) {
        return col.name() == from;
    });
    assert(it != _raw._columns.end());
    auto& def = *it;
    column_definition new_def(to, def.type, def.kind, def.component_index(), def.idx_info);
    _raw._columns.erase(it);
    return with_column(new_def);
}

schema_builder& schema_builder::with_altered_column_type(bytes name, data_type new_type)
{
    auto it = boost::find_if(_raw._columns, [&name] (auto& c) { return c.name() == name; });
    assert(it != _raw._columns.end());
    it->type = new_type;

    if (new_type->is_multi_cell()) {
        auto c_it = _raw._collections.find(name);
        assert(c_it != _raw._collections.end());
        c_it->second = new_type;
    }
    return *this;
}

schema_builder& schema_builder::with_collection(bytes name, data_type type)
{
    _raw._collections.emplace(name, type);
    return *this;
}

schema_builder& schema_builder::with(compact_storage cs) {
    _compact_storage = cs;
    return *this;
}

schema_builder& schema_builder::with_version(table_schema_version v) {
    _version = v;
    return *this;
}

schema_ptr schema_builder::build() {
    if (_version) {
        _raw._version = *_version;
    } else {
        _raw._version = utils::UUID_gen::get_time_UUID();
    }

    if (!_compact_storage) {
        return make_lw_shared<schema>(schema(_raw));
    }

    schema s(_raw);

    // Dense means that no part of the comparator stores a CQL column name. This means
    // COMPACT STORAGE with at least one columnAliases (otherwise it's a thrift "static" CF).
    s._raw._is_dense = (*_compact_storage == compact_storage::yes) && (s.clustering_key_size() > 0);

    if (s.clustering_key_size() == 0) {
        if (*_compact_storage == compact_storage::yes) {
            s._raw._is_compound = false;
        } else {
            s._raw._is_compound = true;
        }
    } else {
        if ((*_compact_storage == compact_storage::yes) && s.clustering_key_size() == 1) {
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
    }
    // We need to rebuild the schema in case we added some column. This is way simpler than trying to factor out the relevant code
    // from the constructor
    return make_lw_shared<schema>(schema(s._raw));
}

schema_ptr schema_builder::build(compact_storage cp) {
    return with(cp).build();
}

// Useful functions to manipulate the schema's comparator field
namespace cell_comparator {

static constexpr auto _composite_str = "org.apache.cassandra.db.marshal.CompositeType";
static constexpr auto _collection_str = "org.apache.cassandra.db.marshal.ColumnToCollectionType";

static bool always_include_default() {
    static thread_local bool def = version::version::current() < version::version(2, 2);
    return def;
};

static sstring compound_name(const schema& s) {
    sstring compound(_composite_str);

    compound += "(";
    if (s.clustering_key_size()) {
        for (auto &t : s.clustering_key_columns()) {
            compound += t.type->name() + ",";
        }
    }
    if (always_include_default() || (s.clustering_key_size() == 0)) {
        compound += s.regular_column_name_type()->name() + ",";
    }

    if (!s.collections().empty()) {
        compound += _collection_str;
        compound += "(";
        for (auto& c : s.collections()) {
            auto ct = static_pointer_cast<const collection_type_impl>(c.second);
            compound += sprint("%s:%s,", to_hex(c.first), ct->name());
        }
        compound.back() = ')';
        compound += ",";
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

void read_collections(schema_builder& builder, sstring comparator)
{
    // The format of collection entries in the comparator is:
    // org.apache.cassandra.db.marshal.ColumnToCollectionType(<name1>:<type1>, ...)

    auto find_closing_parenthesis = [] (sstring_view str, size_t start) {
        auto pos = start;
        auto nest_level = 0;
        do {
            pos = str.find_first_of("()", pos);
            if (pos == sstring::npos) {
                throw marshal_exception();
            }
            if (str[pos] == ')') {
                nest_level--;
            } else if (str[pos] == '(') {
                nest_level++;
            }
            pos++;
        } while (nest_level > 0);
        return pos - 1;
    };

    auto collection_str_length = strlen(_collection_str);

    auto pos = comparator.find(_collection_str);
    if (pos == sstring::npos) {
        return;
    }
    pos += collection_str_length + 1;
    while (pos < comparator.size()) {
        size_t end = comparator.find('(', pos);
        if (end == sstring::npos) {
            throw marshal_exception();
        }
        end = find_closing_parenthesis(comparator, end) + 1;

        auto colon = comparator.find(':', pos);
        if (colon == sstring::npos || colon > end) {
            throw marshal_exception();
        }

        auto name = from_hex(sstring_view(comparator.c_str() + pos, colon - pos));

        colon++;
        auto type_str = sstring_view(comparator.c_str() + colon, end - colon);
        auto type = db::marshal::type_parser::parse(type_str);

        builder.with_collection(name, type);

        if (end < comparator.size() && comparator[end] == ',') {
            pos = end + 1;
        } else if (end < comparator.size() && comparator[end] == ')') {
            pos = sstring::npos;
        } else {
            throw marshal_exception();
        }
    }
}

}

schema::const_iterator
schema::regular_begin() const {
    return regular_columns().begin();
}

schema::const_iterator
schema::regular_end() const {
    return regular_columns().end();
}

schema::const_iterator
schema::regular_lower_bound(const bytes& name) const {
    // TODO: use regular_columns and a version of std::lower_bound() with heterogeneous comparator
    auto i = _regular_columns_by_name.lower_bound(name);
    if (i == _regular_columns_by_name.end()) {
        return regular_end();
    } else {
        return regular_begin() + i->second->id;
    }
}

schema::const_iterator
schema::regular_upper_bound(const bytes& name) const {
    // TODO: use regular_columns and a version of std::upper_bound() with heterogeneous comparator
    auto i = _regular_columns_by_name.upper_bound(name);
    if (i == _regular_columns_by_name.end()) {
        return regular_end();
    } else {
        return regular_begin() + i->second->id;
    }
}

data_type
schema::column_name_type(const column_definition& def) const {
    return def.kind == column_kind::regular_column ? _raw._regular_column_name_type : utf8_type;
}

const column_definition&
schema::regular_column_at(column_id id) const {
    if (id > regular_columns_count()) {
        throw std::out_of_range("column_id");
    }
    return _raw._columns.at(column_offset(column_kind::regular_column) + id);
}

const column_definition&
schema::static_column_at(column_id id) const {
    if (id > static_columns_count()) {
        throw std::out_of_range("column_id");
    }
    return _raw._columns.at(column_offset(column_kind::static_column) + id);
}

bool
schema::is_last_partition_key(const column_definition& def) const {
    return &_raw._columns.at(partition_key_size() - 1) == &def;
}

bool
schema::has_static_columns() const {
    return !static_columns().empty();
}

column_count_type
schema::partition_key_size() const {
    return column_offset(column_kind::clustering_key);
}

column_count_type
schema::clustering_key_size() const {
    return column_offset(column_kind::static_column) - column_offset(column_kind::clustering_key);
}

column_count_type
schema::static_columns_count() const {
    return column_offset(column_kind::regular_column) - column_offset(column_kind::static_column);
}

column_count_type
schema::compact_columns_count() const {
    return _raw._columns.size() - column_offset(column_kind::compact_column);
}

column_count_type
schema::regular_columns_count() const {
    return _raw._columns.size() - column_offset(column_kind::regular_column);
}

schema::const_iterator_range_type
schema::partition_key_columns() const {
    return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::partition_key)
            , _raw._columns.begin() + column_offset(column_kind::clustering_key));
}

schema::const_iterator_range_type
schema::clustering_key_columns() const {
    return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::clustering_key)
            , _raw._columns.begin() + column_offset(column_kind::static_column));
}

schema::const_iterator_range_type
schema::static_columns() const {
    return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::static_column)
            , _raw._columns.begin() + column_offset(column_kind::regular_column));
}

schema::const_iterator_range_type
schema::regular_columns() const {
    return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::regular_column)
            , _raw._columns.end());
}

const column_definition&
schema::compact_column() const {
    if (compact_columns_count() > 1) {
        throw std::runtime_error("unexpected number of compact columns");
    }
    return *(_raw._columns.begin() + column_offset(column_kind::compact_column));
}

const schema::columns_type&
schema::all_columns_in_select_order() const {
    return _raw._columns;
}

uint32_t
schema::position(const column_definition& column) const {
    if (column.is_primary_key()) {
        return column.id;
    }
    return clustering_key_size();
}

bool schema::is_synced() const {
    return _registry_entry && _registry_entry->is_synced();
}

bool schema::equal_columns(const schema& other) const {
    return boost::equal(all_columns_in_select_order(), other.all_columns_in_select_order());
}

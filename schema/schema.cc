/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/on_internal_error.hh>
#include <map>
#include "db/view/view.hh"
#include "timestamp.hh"
#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
#include "cql3/column_identifier.hh"
#include "cql3/util.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "db/marshal/type_parser.hh"
#include "schema_registry.hh"
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <type_traits>
#include "view_info.hh"
#include "partition_slice_builder.hh"
#include "replica/database.hh"
#include "dht/token-sharding.hh"
#include "cdc/cdc_extension.hh"
#include "tombstone_gc_extension.hh"
#include "db/paxos_grace_seconds_extension.hh"
#include "utils/rjson.hh"
#include "tombstone_gc_options.hh"
#include "db/per_partition_rate_limit_extension.hh"
#include "db/tags/utils.hh"
#include "db/tags/extension.hh"
#include "index/target_parser.hh"

constexpr int32_t schema::NAME_LENGTH;

extern logging::logger dblog;

sstring to_sstring(column_kind k) {
    switch (k) {
    case column_kind::partition_key:  return "PARTITION_KEY";
    case column_kind::clustering_key: return "CLUSTERING_COLUMN";
    case column_kind::static_column:  return "STATIC";
    case column_kind::regular_column: return "REGULAR";
    }
    throw std::invalid_argument("unknown column kind");
}

bool is_compatible(column_kind k1, column_kind k2) {
    return k1 == k2;
}

column_mapping_entry::column_mapping_entry(bytes name, sstring type_name)
    : column_mapping_entry(std::move(name), db::marshal::type_parser::parse(type_name))
{
}

column_mapping_entry::column_mapping_entry(const column_mapping_entry& o)
    : column_mapping_entry(o._name, o._type->name())
{
}

column_mapping_entry& column_mapping_entry::operator=(const column_mapping_entry& o) {
    auto copy = o;
    return operator=(std::move(copy));
}

bool operator==(const column_mapping_entry& lhs, const column_mapping_entry& rhs) {
    return lhs.name() == rhs.name() && lhs.type() == rhs.type();
}

bool operator==(const column_mapping& lhs, const column_mapping& rhs) {
    const auto& lhs_columns = lhs.columns(), rhs_columns = rhs.columns();
    if (lhs_columns.size() != rhs_columns.size()) {
        return false;
    }
    for (size_t i = 0, end = lhs_columns.size(); i < end; ++i) {
        const column_mapping_entry& lhs_entry = lhs_columns[i], rhs_entry = rhs_columns[i];
        if (lhs_entry != rhs_entry) {
            return false;
        }
    }
    return true;
}

const column_mapping_entry& column_mapping::column_at(column_kind kind, column_id id) const {
    SCYLLA_ASSERT(kind == column_kind::regular_column || kind == column_kind::static_column);
    return kind == column_kind::regular_column ? regular_column_at(id) : static_column_at(id);
}

const column_mapping_entry& column_mapping::static_column_at(column_id id) const {
    if (id >= _n_static) {
        on_internal_error(dblog, format("static column id {:d} >= {:d}", id, _n_static));
    }
    return _columns[id];
}

const column_mapping_entry& column_mapping::regular_column_at(column_id id) const {
    auto n_regular = _columns.size() - _n_static;
    if (id >= n_regular) {
        on_internal_error(dblog, format("regular column id {:d} >= {:d}", id, n_regular));
    }
    return _columns[id + _n_static];
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

auto fmt::formatter<column_mapping>::format(const column_mapping& cm, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    column_id n_static = cm.n_static();
    column_id n_regular = cm.columns().size() - n_static;

    auto pr_entry = [] (column_id i, const column_mapping_entry& e) {
        // Without schema we don't know if name is UTF8. If we had schema we could use
        // s->regular_column_name_type()->to_string(e.name()).
        return fmt::format("{{id={}, name=0x{}, type={}}}", i, e.name(), e.type()->name());
    };
    return fmt::format_to(ctx.out(), "{{static=[{}], regular=[{}]}}",
               fmt::join(boost::irange<column_id>(0, n_static) |
                         boost::adaptors::transformed([&] (column_id i) { return pr_entry(i, cm.static_column_at(i)); }), ", "),
               fmt::join(boost::irange<column_id>(0, n_regular) |
                         boost::adaptors::transformed([&] (column_id i) { return pr_entry(i, cm.regular_column_at(i)); }), ", "));
}

thread_local std::map<sstring, std::unique_ptr<dht::i_partitioner>> partitioners;
thread_local std::map<std::pair<unsigned, unsigned>, std::unique_ptr<dht::static_sharder>> sharders;
sstring default_partitioner_name = "org.apache.cassandra.dht.Murmur3Partitioner";
unsigned default_partitioner_ignore_msb = 12;

static const dht::i_partitioner& get_partitioner(const sstring& name) {
    auto it = partitioners.find(name);
    if (it == partitioners.end()) {
        auto p = dht::make_partitioner(name);
        it = partitioners.insert({name, std::move(p)}).first;
    }
    return *it->second;
}

void schema::set_default_partitioner(const sstring& class_name, unsigned ignore_msb) {
    default_partitioner_name = class_name;
    default_partitioner_ignore_msb = ignore_msb;
}

static const dht::static_sharder& get_sharder(unsigned shard_count, unsigned ignore_msb) {
    auto it = sharders.find({shard_count, ignore_msb});
    if (it == sharders.end()) {
        auto sharder = std::make_unique<dht::static_sharder>(shard_count, ignore_msb);
        it = sharders.emplace(std::make_pair(shard_count, ignore_msb), std::move(sharder)).first;
    }
    return *it->second;
}

const dht::i_partitioner& schema::get_partitioner() const {
    return _raw._partitioner.get();
}

const dht::static_sharder* schema::try_get_static_sharder() const {
    auto t = maybe_table();
    if (t && !t->uses_static_sharding()) {
        // Use table()->get_effective_replication_map()->get_sharder() instead.
        return nullptr;
    }
    return &_raw._sharder.get();
}

const dht::static_sharder& schema::get_sharder() const {
    auto* s = try_get_static_sharder();
    if (!s) {
        // Use table()->get_effective_replication_map()->get_sharder() instead.
        on_internal_error(dblog, format("Attempted to obtain static sharder for table {}.{}", ks_name(), cf_name()));
    }
    return *s;
}

replica::table* schema::maybe_table() const {
    if (_registry_entry) {
        return _registry_entry->table();
    }
    return nullptr;
}

replica::table& schema::table() const {
    auto t = maybe_table();
    if (!t) {
        seastar::throw_with_backtrace<replica::no_such_column_family>(id());
    }
    return *t;
}

bool schema::has_custom_partitioner() const {
    return _raw._partitioner.get().name() != default_partitioner_name;
}

lw_shared_ptr<cql3::column_specification>
schema::make_column_specification(const column_definition& def) const {
    auto id = ::make_shared<cql3::column_identifier>(def.name(), column_name_type(def));
    return make_lw_shared<cql3::column_specification>(_raw._ks_name, _raw._cf_name, std::move(id), def.type);
}

v3_columns::v3_columns(std::vector<column_definition> cols, bool is_dense, bool is_compound)
    : _is_dense(is_dense)
    , _is_compound(is_compound)
    , _columns(std::move(cols))
{
    for (column_definition& def : _columns) {
        _columns_by_name[def.name()] = &def;
    }
}

v3_columns v3_columns::from_v2_schema(const schema& s) {
    data_type static_column_name_type = utf8_type;
    std::vector<column_definition> cols;

    if (s.is_static_compact_table()) {
        if (s.has_static_columns()) {
            throw std::runtime_error(
                format("v2 static compact table should not have static columns: {}.{}", s.ks_name(), s.cf_name()));
        }
        if (s.clustering_key_size()) {
            throw std::runtime_error(
                format("v2 static compact table should not have clustering columns: {}.{}", s.ks_name(), s.cf_name()));
        }
        static_column_name_type = s.regular_column_name_type();
        for (auto& c : s.all_columns()) {
            // Note that for "static" no-clustering compact storage we use static for the defined columns
            if (c.kind == column_kind::regular_column) {
                auto new_def = c;
                new_def.kind = column_kind::static_column;
                cols.push_back(new_def);
            } else {
                cols.push_back(c);
            }
        }
        schema_builder::default_names names(s._raw);
        cols.emplace_back(to_bytes(names.clustering_name()), static_column_name_type, column_kind::clustering_key, 0);
        cols.emplace_back(to_bytes(names.compact_value_name()), s.make_legacy_default_validator(), column_kind::regular_column, 0);
    } else {
        cols = s.all_columns();
    }

    for (column_definition& def : cols) {
        data_type name_type = def.is_static() ? static_column_name_type : utf8_type;
        auto id = ::make_shared<cql3::column_identifier>(def.name(), name_type);
        def.column_specification = make_lw_shared<cql3::column_specification>(s.ks_name(), s.cf_name(), std::move(id), def.type);
    }

    return v3_columns(std::move(cols), s.is_dense(), s.is_compound());
}

void v3_columns::apply_to(schema_builder& builder) const {
    if (is_static_compact()) {
        for (auto& c : _columns) {
            if (c.kind == column_kind::regular_column) {
                builder.set_default_validation_class(c.type);
            } else if (c.kind == column_kind::static_column) {
                auto new_def = c;
                new_def.kind = column_kind::regular_column;
                builder.with_column_ordered(new_def);
            } else if (c.kind == column_kind::clustering_key) {
                builder.set_regular_column_name_type(c.type);
            } else {
                builder.with_column_ordered(c);
            }
        }
    } else {
        for (auto& c : _columns) {
            if (is_compact() && c.kind == column_kind::regular_column) {
                builder.set_default_validation_class(c.type);
            }
            builder.with_column_ordered(c);
        }
    }
}

bool v3_columns::is_static_compact() const {
    return !_is_dense && !_is_compound;
}

bool v3_columns::is_compact() const {
    return _is_dense || !_is_compound;
}

const std::unordered_map<bytes, const column_definition*>& v3_columns::columns_by_name() const {
    return _columns_by_name;
}

const std::vector<column_definition>& v3_columns::all_columns() const {
    return _columns;
}

void schema::rebuild() {
    _partition_key_type = make_lw_shared<compound_type<>>(get_column_types(partition_key_columns()));
    _clustering_key_type = make_lw_shared<compound_prefix>(get_column_types(clustering_key_columns()));
    _clustering_key_size = column_offset(column_kind::static_column) - column_offset(column_kind::clustering_key);
    _regular_column_count = _raw._columns.size() - column_offset(column_kind::regular_column);
    _static_column_count = column_offset(column_kind::regular_column) - column_offset(column_kind::static_column);
    _columns_by_name.clear();

    for (const column_definition& def : all_columns()) {
        _columns_by_name[def.name()] = &def;
    }

    static_assert(row_column_ids_are_ordered_by_name::value, "row columns don't need to be ordered by name");
    if (!std::is_sorted(regular_columns().begin(), regular_columns().end(), column_definition::name_comparator(regular_column_name_type()))) {
        throw std::runtime_error("Regular columns should be sorted by name");
    }
    if (!std::is_sorted(static_columns().begin(), static_columns().end(), column_definition::name_comparator(static_column_name_type()))) {
        throw std::runtime_error("Static columns should be sorted by name");
    }

    {
        std::vector<column_mapping_entry> cm_columns;
        for (const column_definition& def : boost::range::join(static_columns(), regular_columns())) {
            cm_columns.emplace_back(column_mapping_entry{def.name(), def.type});
        }
        _column_mapping = column_mapping(std::move(cm_columns), static_columns_count());
    }

    if (is_counter()) {
        for (auto&& cdef : boost::range::join(static_columns(), regular_columns())) {
            if (!cdef.type->is_counter()) {
                throw exceptions::configuration_exception(format("Cannot add a non counter column ({}) in a counter column family", cdef.name_as_text()));
            }
        }
    } else {
        for (auto&& cdef : all_columns()) {
            if (cdef.type->is_counter()) {
                throw exceptions::configuration_exception(format("Cannot add a counter column ({}) in a non counter column family", cdef.name_as_text()));
            }
        }
    }

    _v3_columns = v3_columns::from_v2_schema(*this);
    _full_slice = make_shared<query::partition_slice>(partition_slice_builder(*this).build());
}

const column_mapping& schema::get_column_mapping() const {
    return _column_mapping;
}

schema::raw_schema::raw_schema(table_id id)
    : _id(id)
    , _partitioner(::get_partitioner(default_partitioner_name))
    , _sharder(::get_sharder(smp::count, default_partitioner_ignore_msb))
{ }

schema::schema(private_tag, const raw_schema& raw, std::optional<raw_view_info> raw_view_info, const schema_static_props& props)
    : _raw(raw)
    , _static_props(props)
    , _offsets([this] {
        if (_raw._columns.size() > std::numeric_limits<column_count_type>::max()) {
            throw std::runtime_error(format("Column count limit ({:d}) overflowed: {:d}",
                                            std::numeric_limits<column_count_type>::max(), _raw._columns.size()));
        }

        auto& cols = _raw._columns;
        std::array<column_count_type, 4> count = { 0, 0, 0, 0 };
        auto i = cols.begin();
        auto e = cols.end();
        for (auto k : { column_kind::partition_key, column_kind::clustering_key, column_kind::static_column, column_kind::regular_column }) {
            auto j = std::stable_partition(i, e, [k](const auto& c) {
                return c.kind == k;
            });
            count[column_count_type(k)] = std::distance(i, j);
            i = j;
        }
        return std::array<column_count_type, 3> {
                count[0],
                count[0] + count[1],
                count[0] + count[1] + count[2],
        };
    }())
{
    std::sort(
            _raw._columns.begin() + column_offset(column_kind::static_column),
            _raw._columns.begin()
                    + column_offset(column_kind::regular_column),
            column_definition::name_comparator(static_column_name_type()));
    std::sort(
            _raw._columns.begin()
                    + column_offset(column_kind::regular_column),
            _raw._columns.end(), column_definition::name_comparator(regular_column_name_type()));

    std::stable_sort(_raw._columns.begin(),
              _raw._columns.begin() + column_offset(column_kind::clustering_key),
              [] (auto x, auto y) { return x.id < y.id; });
    std::stable_sort(_raw._columns.begin() + column_offset(column_kind::clustering_key),
              _raw._columns.begin() + column_offset(column_kind::static_column),
              [] (auto x, auto y) { return x.id < y.id; });

    column_id id = 0;
    for (auto& def : _raw._columns) {
        def.column_specification = make_column_specification(def);
        SCYLLA_ASSERT(!def.id || def.id == id - column_offset(def.kind));
        def.ordinal_id = static_cast<ordinal_column_id>(id);
        def.id = id - column_offset(def.kind);

        auto dropped_at_it = _raw._dropped_columns.find(def.name_as_text());
        if (dropped_at_it != _raw._dropped_columns.end()) {
            def._dropped_at = std::max(def._dropped_at, dropped_at_it->second.timestamp);
        }

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
            case column_kind::regular_column:
                if (_raw._is_dense) {
                    // regular values in dense tables are alone, so they have no index
                    def._thrift_bits.is_on_all_components = true;
                    break;
                }
                [[fallthrough]];
            default:
                // Or any other column where "comparator" is not compound
                break;
            }
        }

        ++id;
    }

    rebuild();
    if (raw_view_info) {
        _view_info = std::make_unique<::view_info>(*this, *raw_view_info);
    }
}

schema::schema(const schema& o, const std::function<void(schema&)>& transform)
    : _raw(o._raw)
    , _static_props(o._static_props)
    , _offsets(o._offsets)
{
    // Do the transformation after all the raw fields are initialized, but
    // *before* the derived fields are generated (from the raw ones).
    if (transform) {
        transform(*this);
    }

    rebuild();
    if (o.is_view()) {
        _view_info = std::make_unique<::view_info>(*this, o.view_info()->raw());
        if (o.view_info()->base_info()) {
            _view_info->set_base_info(o.view_info()->base_info());
        }
    }
}

schema::schema(const schema& o)
    : schema(o, {})
{
}

schema::schema(reversed_tag, const schema& o)
    : schema(o, [] (schema& s) {
        s._raw._version = reversed(s._raw._version);
        for (auto& col : s._raw._columns) {
            if (col.kind == column_kind::clustering_key) {
                col.type = reversed(col.type);
            }
        }
    })
{
}

lw_shared_ptr<const schema> make_shared_schema(std::optional<table_id> id, std::string_view ks_name,
    std::string_view cf_name, std::vector<schema::column> partition_key, std::vector<schema::column> clustering_key,
    std::vector<schema::column> regular_columns, std::vector<schema::column> static_columns,
    data_type regular_column_name_type, sstring comment) {
    schema_builder builder(std::move(ks_name), std::move(cf_name), std::move(id), std::move(regular_column_name_type));
    for (auto&& column : partition_key) {
        builder.with_column(std::move(column.name), std::move(column.type), column_kind::partition_key);
    }
    for (auto&& column : clustering_key) {
        builder.with_column(std::move(column.name), std::move(column.type), column_kind::clustering_key);
    }
    for (auto&& column : regular_columns) {
        builder.with_column(std::move(column.name), std::move(column.type));
    }
    for (auto&& column : static_columns) {
        builder.with_column(std::move(column.name), std::move(column.type), column_kind::static_column);
    }
    builder.set_comment(comment);
    return builder.build();
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

bool
schema::has_multi_cell_collections() const {
    return boost::algorithm::any_of(all_columns(), [] (const column_definition& cdef) {
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
        && x._raw._regular_column_name_type == y._raw._regular_column_name_type
        && x._raw._bloom_filter_fp_chance == y._raw._bloom_filter_fp_chance
        && x._raw._compressor_params == y._raw._compressor_params
        && x._raw._is_dense == y._raw._is_dense
        && x._raw._is_compound == y._raw._is_compound
        && x._raw._type == y._raw._type
        && x._raw._gc_grace_seconds == y._raw._gc_grace_seconds
        && x.paxos_grace_seconds() == y.paxos_grace_seconds()
        && x._raw._min_compaction_threshold == y._raw._min_compaction_threshold
        && x._raw._max_compaction_threshold == y._raw._max_compaction_threshold
        && x._raw._min_index_interval == y._raw._min_index_interval
        && x._raw._max_index_interval == y._raw._max_index_interval
        && x._raw._memtable_flush_period == y._raw._memtable_flush_period
        && x._raw._speculative_retry == y._raw._speculative_retry
        && x._raw._compaction_strategy == y._raw._compaction_strategy
        && x._raw._compaction_strategy_options == y._raw._compaction_strategy_options
        && x._raw._compaction_enabled == y._raw._compaction_enabled
        && x.cdc_options() == y.cdc_options()
        && x.tombstone_gc_options() == y.tombstone_gc_options()
        && x._raw._caching_options == y._raw._caching_options
        && x._raw._dropped_columns == y._raw._dropped_columns
        && x._raw._collections == y._raw._collections
        && indirect_equal_to<std::unique_ptr<::view_info>>()(x._view_info, y._view_info)
        && x._raw._indices_by_name == y._raw._indices_by_name
        && x._raw._is_counter == y._raw._is_counter
        ;
#if 0
        && Objects.equal(triggers, other.triggers)
#endif
}

index_metadata::index_metadata(const sstring& name,
                               const index_options_map& options,
                               index_metadata_kind kind,
                               is_local_index local)
    : _id{utils::UUID_gen::get_name_UUID(name)}
    , _name{name}
    , _kind{kind}
    , _options{options}
    , _local{bool(local)}
{}

bool index_metadata::operator==(const index_metadata& other) const {
    return _id == other._id
           && _name == other._name
           && _kind == other._kind
           && _options == other._options;
}

bool index_metadata::equals_noname(const index_metadata& other) const {
    return _kind == other._kind && _options == other._options;
}

const table_id& index_metadata::id() const {
    return _id;
}

const sstring& index_metadata::name() const {
    return _name;
}

index_metadata_kind index_metadata::kind() const {
    return _kind;
}

const index_options_map& index_metadata::options() const {
    return _options;
}

bool index_metadata::local() const {
    return _local;
}

sstring index_metadata::get_default_index_name(const sstring& cf_name,
                                               std::optional<sstring> root) {
    if (root) {
        // As noted in issue #3403, because table names in CQL only use word
        // characters [A-Za-z0-9_], the default index name should drop other
        // characters from the column name ("root").
        sstring name = root.value();
        name.erase(std::remove_if(name.begin(), name.end(), [](char c) {
            return !((c >= 'A' && c <= 'Z') ||
                     (c >= 'a' && c <= 'z') ||
                     (c >= '0' && c <= '9') ||
                     (c == '_')); }), name.end());
        return cf_name + "_" + name + "_idx";
    }
    return cf_name + "_idx";
}

column_definition::column_definition(bytes name, data_type type, column_kind kind, column_id component_index, column_view_virtual is_view_virtual, column_computation_ptr computation, api::timestamp_type dropped_at)
        : _name(std::move(name))
        , _dropped_at(dropped_at)
        , _is_atomic(type->is_atomic())
        , _is_counter(type->is_counter())
        , _is_view_virtual(is_view_virtual)
        , _computation(std::move(computation))
        , type(std::move(type))
        , id(component_index)
        , kind(kind)
{}

auto fmt::formatter<column_definition>::format(const column_definition& cd, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "ColumnDefinition{{name={}, type={}, kind={}",
                         cd.name_as_text(), cd.type->name(), to_sstring(cd.kind));
    if (cd.is_view_virtual()) {
        out = fmt::format_to(out, ", view_virtual");
    }
    if (cd.is_computed()) {
        out = fmt::format_to(out, ", computed:{}", cd.get_computation().serialize());
    }
    out = fmt::format_to(out, ", componentIndex={}", cd.has_component_index() ? std::to_string(cd.component_index()) : "null");
    out = fmt::format_to(out, ", droppedAt={}", cd._dropped_at);
    return fmt::format_to(out, "}}");
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
    return column_at(static_cast<ordinal_column_id>(column_offset(kind) + id));
}

const column_definition&
schema::column_at(ordinal_column_id ordinal_id) const {
    if (size_t(ordinal_id) >= _raw._columns.size()) [[unlikely]] {
        on_internal_error(dblog, format("{}.{}@{}: column id {:d} >= {:d}",
            ks_name(), cf_name(), version(), size_t(ordinal_id), _raw._columns.size()));
    }
    return _raw._columns.at(static_cast<column_count_type>(ordinal_id));
}

auto fmt::formatter<schema>::format(const schema& s, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "org.apache.cassandra.config.CFMetaData@{}[", fmt::ptr(&s));
    out = fmt::format_to(out, "cfId={}", s._raw._id);
    out = fmt::format_to(out, ",ksName=={}", s._raw._ks_name);
    out = fmt::format_to(out, ",cfName={}", s._raw._cf_name);
    out = fmt::format_to(out, ",cfType={}", cf_type_to_sstring(s._raw._type));
    out = fmt::format_to(out, ",comparator={}", cell_comparator::to_sstring(s));
    out = fmt::format_to(out, ",comment={}", s._raw._comment);
    out = fmt::format_to(out, ",tombstoneGcOptions={}", s.tombstone_gc_options().to_sstring());
    out = fmt::format_to(out, ",gcGraceSeconds={}", s._raw._gc_grace_seconds);
    out = fmt::format_to(out, ",minCompactionThreshold={}", s._raw._min_compaction_threshold);
    out = fmt::format_to(out, ",maxCompactionThreshold={}", s._raw._max_compaction_threshold);
    out = fmt::format_to(out, ",columnMetadata=[");
    int n = 0;
    for (auto& cdef : s._raw._columns) {
        if (n++ != 0) {
            out = fmt::format_to(out, ", ");
        }
        out = fmt::format_to(out, "{}", cdef);
    }
    out = fmt::format_to(out, "]");

    out = fmt::format_to(out, ",compactionStrategyClass=class org.apache.cassandra.db.compaction.{}",
                         sstables::compaction_strategy::name(s._raw._compaction_strategy));

    out = fmt::format_to(out, ",compactionStrategyOptions={{");
    n = 0;
    for (auto& p : s._raw._compaction_strategy_options) {
        out = fmt::format_to(out, "{}={}", p.first, p.second);
        out = fmt::format_to(out, ", ");
    }
    out = fmt::format_to(out, "enabled={}", s._raw._compaction_enabled);
    out = fmt::format_to(out, "}}");

    out = fmt::format_to(out, ",compressionParameters={{");
    n = 0;
    for (auto& p : s._raw._compressor_params.get_options() ) {
        if (n++ != 0) {
            out = fmt::format_to(out, ", ");
        }
        out = fmt::format_to(out, "{}={}", p.first, p.second);
    }
    out = fmt::format_to(out, "}}");

    out = fmt::format_to(out, ",bloomFilterFpChance={}", s._raw._bloom_filter_fp_chance);
    out = fmt::format_to(out, ",memtableFlushPeriod={}", s._raw._memtable_flush_period);
    out = fmt::format_to(out, ",caching={}", s._raw._caching_options.to_sstring());
    out = fmt::format_to(out, ",cdc={}", s.cdc_options().to_sstring());
    out = fmt::format_to(out, ",defaultTimeToLive={}", s._raw._default_time_to_live.count());
    out = fmt::format_to(out, ",minIndexInterval={}", s._raw._min_index_interval);
    out = fmt::format_to(out, ",maxIndexInterval={}", s._raw._max_index_interval);
    out = fmt::format_to(out, ",speculativeRetry={}", s._raw._speculative_retry.to_sstring());
    out = fmt::format_to(out, ",triggers=[]");
    out = fmt::format_to(out, ",isDense={}", s._raw._is_dense);
    out = fmt::format_to(out, ",version={}", s.version());

    out = fmt::format_to(out, ",droppedColumns={{");
    n = 0;
    for (auto& dc : s._raw._dropped_columns) {
        if (n++ != 0) {
            out = fmt::format_to(out, ", ");
        }
        out = fmt::format_to(out,"{} : {{ {}, {} }}",
                             dc.first, dc.second.type->name(), dc.second.timestamp);
    }
    out = fmt::format_to(out, "}}");

    out = fmt::format_to(out, ",collections={{");
    n = 0;
    for (auto& c : s._raw._collections) {
        if (n++ != 0) {
            out = fmt::format_to(out, ", ");
        }
        out = fmt::format_to(out,"{} : {}", c.first, c.second->name());
    }
    out = fmt::format_to(out, "}}");

    out = fmt::format_to(out, ",indices={{");
    n = 0;
    for (auto& c : s._raw._indices_by_name) {
        if (n++ != 0) {
            out = fmt::format_to(out, ", ");
        }
        out = fmt::format_to(out,"{} : {}", c.first, c.second.id());
    }
    out = fmt::format_to(out, "}}");

    if (s.is_view()) {
        out = fmt::format_to(out, ", viewInfo={}", *s.view_info());
    }
    return fmt::format_to(out, "]");
}

static std::ostream& map_as_cql_param(std::ostream& os, const std::map<sstring, sstring>& map, bool first = true) {
    for (auto i: map) {
        if (first) {
            first = false;
        } else {
            os << ", ";
        }
        os << "'" << i.first << "': '" << i.second << "'";
    }

    return os;
}

std::ostream& operator<<(std::ostream& os, const schema& s) {
    fmt::print(os, "{}", s);
    return os;
}

// default impl assumes options are in a map.
// implementations should override if not
std::string schema_extension::options_to_string() const {
    std::ostringstream ss;
    ss << '{';
    map_as_cql_param(ss, ser::deserialize_from_buffer(serialize(), boost::type<default_map_type>(), 0));
    ss << '}';
    return ss.str();
}

static std::ostream& column_definition_as_cql_key(std::ostream& os, const column_definition & cd) {
    os << cd.name_as_cql_string();
    os << " " << cd.type->cql3_type_name();

    if (cd.kind == column_kind::static_column) {
        os << " STATIC";
    }
    return os;
}

static bool is_global_index(replica::database& db, const table_id& id, const schema& s) {
    return  db.find_column_family(id).get_index_manager().is_global_index(s);
}

static bool is_index(replica::database& db, const table_id& id, const schema& s) {
    return  db.find_column_family(id).get_index_manager().is_index(s);
}

sstring schema::element_type(replica::database& db) const {
    if (is_view()) {
        if (is_index(db, view_info()->base_id(), *this)) {
            return "index";
        } else {
            return "view";
        }
    }
    return "table";
}

static void describe_index_columns(std::ostream& os, bool is_local, const schema& index_schema, schema_ptr base_schema) {
    auto index_name = secondary_index::index_name_from_table_name(index_schema.cf_name());
    if (!base_schema->all_indices().contains(index_name)) {
        on_internal_error(dblog, format("Couldn't find index {} on table {}", index_name, base_schema->cf_name()));
    }
    
    int n = 0;
    os << "(";

    if (is_local) {
        os << "(";
        for (auto& pk: index_schema.partition_key_columns()) {
            if (n++ != 0) {
                os << ", ";
            }
            os << pk.name_as_cql_string();
        }
        os << "), ";
    }

    // Global and local indexes may only contain one column.
    // Local indexes support multi column indexes via custom indexes, but at least
    // for now, Scylla supports no custom indexes.
    auto index_metadata = base_schema->all_indices().at(index_name);
    auto target_str = secondary_index::target_parser::get_target_column_name_from_string(
        index_metadata.options().at(cql3::statements::index_target::target_option_name)
    );
    auto base_column_name = cql3_parser::index_target::column_name_from_target_string(target_str);
    auto base_column = base_schema->get_column_definition(to_bytes(base_column_name));
    if (!base_column) {
        on_internal_error(dblog, format("Couldn't find base column {} in table {} for index {}", base_column_name, base_schema->cf_name(), index_name));
    }
    auto bk_type = base_column->type;

    if (bk_type->is_collection() && !bk_type->is_multi_cell()) {
        // Indexes on frozen collection require full() function but the function is dropped while saving the target.
        os << "full(" << cql3::util::maybe_quote(base_column_name) << ")";
    } else if (bk_type->is_set() && target_str.starts_with("keys(")) {
        // Indexes on set are saved with target keys() but only valid targets when creating the index are values() or just the column name.
        // Since indexes on list are always printed with values() target (it's always added even if the index is created only with column name),
        // always add values() target to index to set
        os << "values(" << cql3::util::maybe_quote(base_column_name) << ")";
    } else {
        // Target string is already quoted if needed
        os << target_str;
    }

    os << ")";
}

std::ostream& schema::describe(replica::database& db, std::ostream& os, bool with_internals) const {
    os << "CREATE ";
    int n = 0;

    if (is_view()) {
        if (is_index(db, view_info()->base_id(), *this)) {
            auto is_local = !is_global_index(db, view_info()->base_id(), *this);

            os << "INDEX " << cql3::util::maybe_quote(secondary_index::index_name_from_table_name(cf_name())) << " ON "
                    << cql3::util::maybe_quote(ks_name()) << "." << cql3::util::maybe_quote(view_info()->base_name());

            describe_index_columns(os, is_local, *this, db.find_schema(view_info()->base_id()));  
            os << ";\n";
            return os;
        } else {
            os << "MATERIALIZED VIEW " << cql3::util::maybe_quote(ks_name()) << "." << cql3::util::maybe_quote(cf_name()) << " AS\n";
            os << "    SELECT ";
            for (auto& cdef : all_columns()) {
                if (cdef.is_hidden_from_cql()) {
                    continue;
                }
                if (n++ != 0) {
                    os << ", ";
                }
                os << cdef.name_as_cql_string();
            }
            os << "\n    FROM " << cql3::util::maybe_quote(ks_name()) << "." <<  cql3::util::maybe_quote(view_info()->base_name());
            os << "\n    WHERE " << view_info()->where_clause();
        }
    } else {
        os << "TABLE " << cql3::util::maybe_quote(ks_name()) << "." << cql3::util::maybe_quote(cf_name()) << " (";
        for (auto& cdef : all_columns()) {
            if (with_internals && dropped_columns().contains(cdef.name_as_text())) {
                // If the column has been re-added after a drop, we don't include it right away. Instead, we'll add the
                // dropped one first below, then we'll issue the DROP and then the actual ADD for this column, thus
                // simulating the proper sequence of events.
                continue;
            }

            os << "\n    ";
            column_definition_as_cql_key(os, cdef);
            os << ",";
        }

        if (with_internals) {
            for (auto& cdef: dropped_columns()) {
                os << "\n    ";
                os << cql3::util::maybe_quote(cdef.first) << " " << cdef.second.type->cql3_type_name() << ",";
            }
        }
    }

    os << "\n    PRIMARY KEY (";
    if (partition_key_columns().size() > 1) {
        os << "(";
    }
    n = 0;
    for (auto& pk : partition_key_columns()) {
        if (n++ != 0) {
            os << ", ";
        }
        os << pk.name_as_cql_string();
    }
    if (partition_key_columns().size() > 1) {
        os << ")";
    }
    for (auto& pk : clustering_key_columns()) {
        os << ", ";
        os << pk.name_as_cql_string();
    }
    os << ")";
    if (is_view()) {
        os << "\n    ";
    } else {
        os << "\n) ";
    }
    os << "WITH ";
    if (with_internals) {
        os << "ID = " << id() << "\nAND ";
    }
    if (!clustering_key_columns().empty()) {
        // Adding clustering key order can be optional, but there's no harm in doing so.
        os << "CLUSTERING ORDER BY (";
        n = 0;
        for (auto& pk : clustering_key_columns()) {
            if (n++ != 0) {
                os << ", ";
            }
            os << pk.name_as_cql_string();
            if (pk.type->is_reversed()) {
                os << " DESC";
            } else {
                os << " ASC";
            }
        }
        os << ")\n    AND ";
    }
    if (is_compact_table()) {
        os << "COMPACT STORAGE\n    AND ";
    }
    schema_properties(db, os);
    os << ";\n";

    if (with_internals) {
        for (auto& cdef : dropped_columns()) {
            os << "\nALTER TABLE " << cql3::util::maybe_quote(ks_name()) << "." << cql3::util::maybe_quote(cf_name())
               << " DROP " << cql3::util::maybe_quote(cdef.first) << " USING TIMESTAMP " << cdef.second.timestamp << ";";

            auto column = get_column_definition(to_bytes(cdef.first));
            if (column) {
                os << "\nALTER TABLE " << cql3::util::maybe_quote(ks_name()) << "." << cql3::util::maybe_quote(cf_name())
                   << " ADD ";
                column_definition_as_cql_key(os, *column);
                os << ";";
            }
        }
    }

    return os;
}

std::ostream& schema::schema_properties(replica::database& db, std::ostream& os) const {
    os << "bloom_filter_fp_chance = " << bloom_filter_fp_chance();
    os << "\n    AND caching = {";
    map_as_cql_param(os, caching_options().to_map());
    os << "}";
    os << "\n    AND comment = " << cql3::util::single_quote(comment());
    os << "\n    AND compaction = {'class': '" <<  sstables::compaction_strategy::name(compaction_strategy()) << "'";
    map_as_cql_param(os, compaction_strategy_options(), false) << "}";
    os << "\n    AND compression = {";
    map_as_cql_param(os,  get_compressor_params().get_options());
    os << "}";

    os << "\n    AND crc_check_chance = " << crc_check_chance();
    os << "\n    AND default_time_to_live = " << default_time_to_live().count();
    os << "\n    AND gc_grace_seconds = " << gc_grace_seconds().count();
    os << "\n    AND max_index_interval = " << max_index_interval();
    os << "\n    AND memtable_flush_period_in_ms = " << memtable_flush_period();
    os << "\n    AND min_index_interval = " << min_index_interval();
    os << "\n    AND speculative_retry = '" << speculative_retry().to_sstring() << "'";
    
    for (auto& [type, ext] : extensions()) {
        os << "\n    AND " << type << " = " << ext->options_to_string();
    }
    if (is_view() && !is_index(db, view_info()->base_id(), *this)) {
        auto is_sync_update = db::find_tag(*this, db::SYNCHRONOUS_VIEW_UPDATES_TAG_KEY);
        if (is_sync_update.has_value()) {
            os << "\n    AND synchronous_updates = " << *is_sync_update;
        }
    }
    return os;
}

std::ostream& schema::describe_alter_with_properties(replica::database& db, std::ostream& os) const {
    os << "ALTER "; 
    if (is_view()) {
        if (is_index(db, view_info()->base_id(), *this)) {
            on_internal_error(dblog, "ALTER statement is not supported for index");
        }
        
        os << "MATERIALIZED VIEW ";
    } else {
        os << "TABLE ";
    }
    os << cql3::util::maybe_quote(ks_name()) << "." << cql3::util::maybe_quote(cf_name()) << " WITH ";
    schema_properties(db, os);
    os << ";\n";

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

sstring column_definition::name_as_cql_string() const {
    return cql3::util::maybe_quote(name_as_text());
}

bool column_definition::is_on_all_components() const {
    return _thrift_bits.is_on_all_components;
}

bool operator==(const column_definition& x, const column_definition& y)
{
    return x._name == y._name
        && x.type == y.type
        && x.id == y.id
        && x.kind == y.kind
        && x._dropped_at == y._dropped_at;
}

// Based on org.apache.cassandra.config.CFMetaData#generateLegacyCfId
table_id
generate_legacy_id(const sstring& ks_name, const sstring& cf_name) {
    return table_id(utils::UUID_gen::get_name_UUID(ks_name + cf_name));
}

schema_builder& schema_builder::set_compaction_strategy_options(std::map<sstring, sstring>&& options) {
    _raw._compaction_strategy_options = std::move(options);
    return *this;
}

schema_builder& schema_builder::with_partitioner(sstring name) {
    _raw._partitioner = get_partitioner(name);
    return *this;
}

schema_builder& schema_builder::with_sharder(unsigned shard_count, unsigned sharding_ignore_msb_bits) {
    _raw._sharder = get_sharder(shard_count, sharding_ignore_msb_bits);
    return *this;
}


schema_builder::schema_builder(std::string_view ks_name, std::string_view cf_name,
        std::optional<table_id> id, data_type rct)
        : _raw(id ? *id : table_id(utils::UUID_gen::get_time_UUID()))
{
    // Various schema-creation commands (creating tables, indexes, etc.)
    // usually place limits on which characters are allowed in keyspace or
    // table names. But in case we have a hole in those defences (see issue
    // #3403, for example), let's prevent at least the characters "/" and
    // null from being in the keyspace or table name, because those will
    // surely cause serious problems when materialized to directory names.
    // We throw a logic_error because we expect earlier defences to have
    // avoided this case in the first place.
    if (ks_name.find_first_of('/') != std::string_view::npos ||
        ks_name.find_first_of('\0') != std::string_view::npos) {
        throw std::logic_error(format("Tried to create a schema with illegal characters in keyspace name: {}", ks_name));
    }
    if (cf_name.find_first_of('/') != std::string_view::npos ||
        cf_name.find_first_of('\0') != std::string_view::npos) {
        throw std::logic_error(format("Tried to create a schema with illegal characters in table name: {}", cf_name));
    }
    _raw._ks_name = sstring(ks_name);
    _raw._cf_name = sstring(cf_name);
    _raw._regular_column_name_type = rct;
}

schema_builder::schema_builder(const schema_ptr s)
    : schema_builder(s->_raw)
{
    if (s->is_view()) {
        _view_info = s->view_info()->raw();
    }
}

schema_builder::schema_builder(const schema::raw_schema& raw)
    : _raw(raw)
{
    static_assert(schema::row_column_ids_are_ordered_by_name::value, "row columns don't need to be ordered by name");
    // Schema builder may add or remove columns and their ids need to be
    // recomputed in build().
    for (auto& def : _raw._columns | boost::adaptors::filtered([] (auto& def) { return !def.is_primary_key(); })) {
            def.id = 0;
            def.ordinal_id = static_cast<ordinal_column_id>(0);
    }
}

schema_builder::schema_builder(
        std::optional<table_id> id,
        std::string_view ks_name,
        std::string_view cf_name,
        std::vector<schema::column> partition_key,
        std::vector<schema::column> clustering_key,
        std::vector<schema::column> regular_columns,
        std::vector<schema::column> static_columns,
        data_type regular_column_name_type,
        sstring comment)
    : schema_builder(ks_name, cf_name, std::move(id), std::move(regular_column_name_type)) {
    for (auto&& column : partition_key) {
        with_column(std::move(column.name), std::move(column.type), column_kind::partition_key);
    }
    for (auto&& column : clustering_key) {
        with_column(std::move(column.name), std::move(column.type), column_kind::clustering_key);
    }
    for (auto&& column : regular_columns) {
        with_column(std::move(column.name), std::move(column.type));
    }
    for (auto&& column : static_columns) {
        with_column(std::move(column.name), std::move(column.type), column_kind::static_column);
    }
    set_comment(comment);
}

column_definition& schema_builder::find_column(const cql3::column_identifier& c) {
    auto i = std::find_if(_raw._columns.begin(), _raw._columns.end(), [c](auto& p) {
        return p.name() == c.name();
     });
    if (i != _raw._columns.end()) {
        return *i;
    }
    throw std::invalid_argument(format("No such column {}", c.name()));
}

bool schema_builder::has_column(const cql3::column_identifier& c) {
    auto i = std::find_if(_raw._columns.begin(), _raw._columns.end(), [c](auto& p) {
        return p.name() == c.name();
     });
    return i != _raw._columns.end();
}

schema_builder& schema_builder::with_column_ordered(const column_definition& c) {
    return with_column(bytes(c.name()), data_type(c.type), column_kind(c.kind), c.position(), c.view_virtual(), c.get_computation_ptr());
}

schema_builder& schema_builder::with_column(bytes name, data_type type, column_kind kind, column_view_virtual is_view_virtual) {
    // component_index will be determined by schema constructor
    return with_column(name, type, kind, 0, is_view_virtual);
}

schema_builder& schema_builder::with_column(bytes name, data_type type, column_kind kind, column_id component_index, column_view_virtual is_view_virtual, column_computation_ptr computation) {
    _raw._columns.emplace_back(name, type, kind, component_index, is_view_virtual, std::move(computation));
    if (type->is_multi_cell()) {
        with_collection(name, type);
    } else if (type->is_counter()) {
	    set_is_counter(true);
	}
    return *this;
}

schema_builder& schema_builder::with_computed_column(bytes name, data_type type, column_kind kind, column_computation_ptr computation) {
    return with_column(name, type, kind, 0, column_view_virtual::no, std::move(computation));
}

schema_builder& schema_builder::remove_column(bytes name, std::optional<api::timestamp_type> timestamp)
{
    auto it = boost::range::find_if(_raw._columns, [&] (auto& column) {
        return column.name() == name;
    });
    if(it == _raw._columns.end()) {
        throw std::out_of_range(format("Cannot remove: column {} not found.", name));
    }
    auto name_as_text = it->column_specification ? it->name_as_text() : schema::column_name_type(*it, _raw._regular_column_name_type)->get_string(it->name());
    without_column(name_as_text, it->type, timestamp ? *timestamp : api::new_timestamp());
    _raw._columns.erase(it);
    return *this;
}

schema_builder& schema_builder::without_column(sstring name, api::timestamp_type timestamp) {
    return without_column(std::move(name), bytes_type, timestamp);
}

schema_builder& schema_builder::without_column(sstring name, data_type type, api::timestamp_type timestamp)
{
    auto ret = _raw._dropped_columns.emplace(name, schema::dropped_column{type, timestamp});
    if (!ret.second && ret.first->second.timestamp < timestamp) {
        ret.first->second.type = type;
        ret.first->second.timestamp = timestamp;
    }
    return *this;
}

schema_builder& schema_builder::rename_column(bytes from, bytes to)
{
    auto it = std::find_if(_raw._columns.begin(), _raw._columns.end(), [&] (auto& col) {
        return col.name() == from;
    });
    SCYLLA_ASSERT(it != _raw._columns.end());
    auto& def = *it;
    column_definition new_def(to, def.type, def.kind, def.component_index());
    _raw._columns.erase(it);
    return with_column_ordered(new_def);
}

schema_builder& schema_builder::alter_column_type(bytes name, data_type new_type)
{
    auto it = boost::find_if(_raw._columns, [&name] (auto& c) { return c.name() == name; });
    SCYLLA_ASSERT(it != _raw._columns.end());
    it->type = new_type;

    if (new_type->is_multi_cell()) {
        auto c_it = _raw._collections.find(name);
        SCYLLA_ASSERT(c_it != _raw._collections.end());
        c_it->second = new_type;
    }
    return *this;
}

schema_builder& schema_builder::mark_column_computed(bytes name, column_computation_ptr computation) {
    auto it = boost::find_if(_raw._columns, [&name] (const column_definition& c) { return c.name() == name; });
    SCYLLA_ASSERT(it != _raw._columns.end());
    it->set_computed(std::move(computation));

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

static const sstring default_partition_key_name = "key";
static const sstring default_clustering_name = "column";
static const sstring default_compact_value_name = "value";

schema_builder::default_names::default_names(const schema_builder& builder)
    : default_names(builder._raw)
{}

schema_builder::default_names::default_names(const schema::raw_schema& raw)
    : _raw(raw)
    , _partition_index(0)
    , _clustering_index(1)
    , _compact_index(0)
{}

sstring schema_builder::default_names::unique_name(const sstring& base, size_t& idx, size_t off) const {
    for (;;) {
        auto candidate = idx == 0 ? base : base + std::to_string(idx + off);
        ++idx;
        auto i = std::find_if(_raw._columns.begin(), _raw._columns.end(), [b = to_bytes(candidate)](const column_definition& c) {
            return c.name() == b;
        });
        if (i == _raw._columns.end()) {
            return candidate;
        }
    }
}

sstring schema_builder::default_names::partition_key_name() {
    // For compatibility sake, we call the first alias 'key' rather than 'key1'. This
    // is inconsistent with column alias, but it's probably not worth risking breaking compatibility now.
    return unique_name(default_partition_key_name, _partition_index, 1);
}

sstring schema_builder::default_names::clustering_name() {
    return unique_name(default_clustering_name, _clustering_index, 0);
}

sstring schema_builder::default_names::compact_value_name() {
    return unique_name(default_compact_value_name, _compact_index, 0);
}

void schema_builder::prepare_dense_schema(schema::raw_schema& raw) {
    auto is_dense = raw._is_dense;
    auto is_compound = raw._is_compound;
    auto is_compact_table = is_dense || !is_compound;

    if (is_compact_table) {
        auto count_kind = [&raw](column_kind kind) {
            return std::count_if(raw._columns.begin(), raw._columns.end(), [kind](const column_definition& c) {
                return c.kind == kind;
            });
        };

        default_names names(raw);

        if (is_dense) {
            auto regular_cols = count_kind(column_kind::regular_column);
            // In Origin, dense CFs always have at least one regular column
            if (regular_cols == 0) {
                raw._columns.emplace_back(to_bytes(names.compact_value_name()),
                                empty_type,
                                column_kind::regular_column, 0);
            } else if (regular_cols > 1) {
                throw exceptions::configuration_exception(
                                format("Expecting exactly one regular column. Found {:d}",
                                                regular_cols));
            }
        }
    }
}

schema_builder& schema_builder::with_view_info(table_id base_id, sstring base_name, bool include_all_columns, sstring where_clause) {
    _view_info = raw_view_info(std::move(base_id), std::move(base_name), include_all_columns, std::move(where_clause));
    return *this;
}

schema_builder& schema_builder::with_index(const index_metadata& im) {
    _raw._indices_by_name.emplace(im.name(), im);
    return *this;
}

schema_builder& schema_builder::without_index(const sstring& name) {
    if (_raw._indices_by_name.contains(name)) {
        _raw._indices_by_name.erase(name);
    }
    return *this;
}

schema_builder& schema_builder::without_indexes() {
    _raw._indices_by_name.clear();
    return *this;
}

schema_ptr schema_builder::build() && {
    return build(_raw);
}

schema_ptr schema_builder::build() & {
    schema::raw_schema new_raw = _raw; // Copy so that build() remains idempotent.
    return build(new_raw);
}

schema_ptr schema_builder::build(schema::raw_schema& new_raw) {
    schema_static_props static_props{};
    for (const auto& c: static_configurators()) {
        c(new_raw._ks_name, new_raw._cf_name, static_props);
    }
    if (static_props.use_schema_commitlog) {
        if (!static_props.use_null_sharder) {
            on_internal_error(dblog,
                format("{}.{} uses schema commitlog, but not null sharder, "
                       "schema commitlog works only on shard 0", ks_name(), cf_name()));
        }
        if (static_props.wait_for_sync_to_commitlog) {
            on_internal_error(dblog,
                format("{}.{} uses schema commitlog, wait_for_sync_to_commitlog is redundant",
                        ks_name(), cf_name()));
        }
    }

    if (_version) {
        new_raw._version = *_version;
    } else {
        new_raw._version = table_schema_version(utils::UUID_gen::get_time_UUID());
    }

    if (new_raw._is_counter) {
        new_raw._default_validation_class = counter_type;
    }

    if (_compact_storage) {
        // Dense means that no part of the comparator stores a CQL column name. This means
        // COMPACT STORAGE with at least one columnAliases (otherwise it's a thrift "static" CF).
        auto clustering_key_size = std::count_if(new_raw._columns.begin(), new_raw._columns.end(), [](auto&& col) {
            return col.kind == column_kind::clustering_key;
        });
        new_raw._is_dense = (*_compact_storage == compact_storage::yes) && (clustering_key_size > 0);

        if (clustering_key_size == 0) {
            if (*_compact_storage == compact_storage::yes) {
                new_raw._is_compound = false;
            } else {
                new_raw._is_compound = true;
            }
        } else {
            if ((*_compact_storage == compact_storage::yes) && clustering_key_size == 1) {
                new_raw._is_compound = false;
            } else {
                new_raw._is_compound = true;
            }
        }
    }

    prepare_dense_schema(new_raw);

    // cache `paxos_grace_seconds` value for fast access through the schema object, which is immutable
    if (auto it = new_raw._extensions.find(db::paxos_grace_seconds_extension::NAME); it != new_raw._extensions.end()) {
        new_raw._paxos_grace_seconds =
            dynamic_pointer_cast<db::paxos_grace_seconds_extension>(it->second)->get_paxos_grace_seconds();
    }

    // cache the `per_partition_rate_limit` parameters for fast access through the schema object.
    if (auto it = new_raw._extensions.find(db::per_partition_rate_limit_extension::NAME); it != new_raw._extensions.end()) {
        new_raw._per_partition_rate_limit_options =
            dynamic_pointer_cast<db::per_partition_rate_limit_extension>(it->second)->get_options();
    }

    if (static_props.use_null_sharder) {
        new_raw._sharder = get_sharder(1, 0);
    }

    return make_lw_shared<schema>(schema::private_tag{}, new_raw, _view_info, static_props);
}

auto schema_builder::static_configurators() -> std::vector<static_configurator>& {
    static std::vector<static_configurator> result{};
    return result;
}

int schema_builder::register_static_configurator(static_configurator&& configurator) {
    static_configurators().push_back(std::move(configurator));
    return 0;
}

const cdc::options& schema::cdc_options() const {
    static const cdc::options default_cdc_options;
    const auto& schema_extensions = _raw._extensions;

    if (auto it = schema_extensions.find(cdc::cdc_extension::NAME); it != schema_extensions.end()) {
        return dynamic_pointer_cast<cdc::cdc_extension>(it->second)->get_options();
    }
    return default_cdc_options;
}

const ::tombstone_gc_options& schema::tombstone_gc_options() const {
    static const ::tombstone_gc_options default_tombstone_gc_options;
    const auto& schema_extensions = _raw._extensions;

    if (auto it = schema_extensions.find(tombstone_gc_extension::NAME); it != schema_extensions.end()) {
        return dynamic_pointer_cast<tombstone_gc_extension>(it->second)->get_options();
    }
    return default_tombstone_gc_options;
}

schema_builder& schema_builder::with_cdc_options(const cdc::options& opts) {
    add_extension(cdc::cdc_extension::NAME, ::make_shared<cdc::cdc_extension>(opts));
    return *this;
}

schema_builder& schema_builder::with_tombstone_gc_options(const tombstone_gc_options& opts) {
    add_extension(tombstone_gc_extension::NAME, ::make_shared<tombstone_gc_extension>(opts));
    return *this;
}

schema_builder& schema_builder::with_per_partition_rate_limit_options(const db::per_partition_rate_limit_options& opts) {
    add_extension(db::per_partition_rate_limit_extension::NAME, ::make_shared<db::per_partition_rate_limit_extension>(opts));
    return *this;
}

schema_builder& schema_builder::set_paxos_grace_seconds(int32_t seconds) {
    add_extension(db::paxos_grace_seconds_extension::NAME, ::make_shared<db::paxos_grace_seconds_extension>(seconds));
    return *this;
}

gc_clock::duration schema::paxos_grace_seconds() const {
    return std::chrono::duration_cast<gc_clock::duration>(
        std::chrono::seconds(
            _raw._paxos_grace_seconds ? *_raw._paxos_grace_seconds : DEFAULT_GC_GRACE_SECONDS
        )
    );
}

schema_ptr schema_builder::build(compact_storage cp) {
    return with(cp).build();
}

// Useful functions to manipulate the schema's comparator field
namespace cell_comparator {

static constexpr auto _composite_str = "org.apache.cassandra.db.marshal.CompositeType";
static constexpr auto _collection_str = "org.apache.cassandra.db.marshal.ColumnToCollectionType";

static sstring compound_name(const schema& s) {
    sstring compound(_composite_str);

    compound += "(";
    if (s.clustering_key_size()) {
        for (auto &t : s.clustering_key_columns()) {
            compound += t.type->name() + ",";
        }
    }

    if (!s.is_dense()) {
        compound += s.regular_column_name_type()->name() + ",";
    }

    if (!s.collections().empty()) {
        compound += _collection_str;
        compound += "(";
        for (auto& c : s.collections()) {
            compound += format("{}:{},", to_hex(c.first), c.second->name());
        }
        compound.back() = ')';
        compound += ",";
    }
    // last one will be a ',', just replace it.
    compound.back() = ')';
    return compound;
}

sstring to_sstring(const schema& s) {
    if (s.is_compound()) {
        return compound_name(s);
    } else if (s.clustering_key_size() == 1) {
        SCYLLA_ASSERT(s.is_dense() || s.is_static_compact_table());
        return s.clustering_key_columns().front().type->name();
    } else {
        return s.regular_column_name_type()->name();
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
                throw marshal_exception("read_collections - can't find any parentheses");
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
            throw marshal_exception("read_collections - open parenthesis not found");
        }
        end = find_closing_parenthesis(comparator, end) + 1;

        auto colon = comparator.find(':', pos);
        if (colon == sstring::npos || colon > end) {
            throw marshal_exception("read_collections - colon not found");
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
            throw marshal_exception("read_collections - invalid collection format");
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

struct column_less_comparator {
    bool operator()(const column_definition& def, const bytes& name) {
        return def.name() < name;
    }
    bool operator()(const bytes& name, const column_definition& def) {
        return name < def.name();
    }
};

schema::const_iterator
schema::regular_lower_bound(const bytes& name) const {
    return boost::lower_bound(regular_columns(), name, column_less_comparator());
}

schema::const_iterator
schema::regular_upper_bound(const bytes& name) const {
    return boost::upper_bound(regular_columns(), name, column_less_comparator());
}

schema::const_iterator
schema::static_begin() const {
    return static_columns().begin();
}

schema::const_iterator
schema::static_end() const {
    return static_columns().end();
}

schema::const_iterator
schema::static_lower_bound(const bytes& name) const {
    return boost::lower_bound(static_columns(), name, column_less_comparator());
}

schema::const_iterator
schema::static_upper_bound(const bytes& name) const {
    return boost::upper_bound(static_columns(), name, column_less_comparator());
}
data_type
schema::column_name_type(const column_definition& def, const data_type& regular_column_name_type) {
    if (def.kind == column_kind::regular_column) {
        return regular_column_name_type;
    }
    return utf8_type;
}

data_type
schema::column_name_type(const column_definition& def) const {
    return column_name_type(def, _raw._regular_column_name_type);
}

const column_definition&
schema::regular_column_at(column_id id) const {
    if (id >= regular_columns_count()) {
        on_internal_error(dblog, format("{}.{}@{}: regular column id {:d} >= {:d}",
            ks_name(), cf_name(), version(), id, regular_columns_count()));
    }
    return _raw._columns.at(column_offset(column_kind::regular_column) + id);
}

const column_definition&
schema::clustering_column_at(column_id id) const {
    if (id >= clustering_key_size()) {
        on_internal_error(dblog, format("{}.{}@{}: clustering column id {:d} >= {:d}",
            ks_name(), cf_name(), version(), id, clustering_key_size()));
    }
    return _raw._columns.at(column_offset(column_kind::clustering_key) + id);
}

const column_definition&
schema::static_column_at(column_id id) const {
    if (id >= static_columns_count()) {
        on_internal_error(dblog, format("{}.{}@{}: static column id {:d} >= {:d}",
            ks_name(), cf_name(), version(), id, static_columns_count()));
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
schema::columns_count(column_kind kind) const {
    switch (kind) {
    case column_kind::partition_key:
        return partition_key_size();
    case column_kind::clustering_key:
        return clustering_key_size();
    case column_kind::static_column:
        return static_columns_count();
    case column_kind::regular_column:
        return regular_columns_count();
    default:
        std::abort();
    }
}
column_count_type
schema::partition_key_size() const {
    return column_offset(column_kind::clustering_key);
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

schema::const_iterator_range_type
schema::columns(column_kind kind) const {
    switch (kind) {
    case column_kind::partition_key:
        return partition_key_columns();
    case column_kind::clustering_key:
        return clustering_key_columns();
    case column_kind::static_column:
        return static_columns();
    case column_kind::regular_column:
        return regular_columns();
    }
    throw std::invalid_argument(std::to_string(int(kind)));
}

schema::select_order_range schema::all_columns_in_select_order() const {
    auto is_static_compact_table = this->is_static_compact_table();
    auto no_non_pk_columns = is_compact_table()
                    // Origin: && CompactTables.hasEmptyCompactValue(this);
                    && regular_columns_count() == 1
                    && [](const column_definition& c) {
        // We use empty_type now to match origin, but earlier incarnations
        // set name empty instead. check either.
        return c.type == empty_type || c.name().empty();
    }(regular_column_at(0));
    auto pk_range = const_iterator_range_type(_raw._columns.begin(),
                    _raw._columns.begin() + (is_static_compact_table ?
                                    column_offset(column_kind::clustering_key) :
                                    column_offset(column_kind::static_column)));
    auto ck_v_range = no_non_pk_columns ? static_columns()
                                        : const_iterator_range_type(static_columns().begin(), all_columns().end());
    return boost::range::join(pk_range, ck_v_range);
}

uint32_t
schema::position(const column_definition& column) const {
    if (column.is_primary_key()) {
        return column.id;
    }
    return clustering_key_size();
}

std::optional<index_metadata> schema::find_index_noname(const index_metadata& target) const {
    const auto& it = boost::find_if(_raw._indices_by_name, [&] (auto&& e) {
        return e.second.equals_noname(target);
    });
    if (it != _raw._indices_by_name.end()) {
        return it->second;
    }
    return {};
}

std::vector<index_metadata> schema::indices() const {
    return boost::copy_range<std::vector<index_metadata>>(_raw._indices_by_name | boost::adaptors::map_values);
}

const std::unordered_map<sstring, index_metadata>& schema::all_indices() const {
    return _raw._indices_by_name;
}

bool schema::has_index(const sstring& index_name) const {
    return _raw._indices_by_name.contains(index_name);
}

std::vector<sstring> schema::index_names() const {
    return boost::copy_range<std::vector<sstring>>(_raw._indices_by_name | boost::adaptors::map_keys);
}

data_type schema::make_legacy_default_validator() const {
    return _raw._default_validation_class;
}

bool schema::is_synced() const {
    return _registry_entry && _registry_entry->is_synced();
}

bool schema::equal_columns(const schema& other) const {
    return boost::equal(all_columns(), other.all_columns());
}

schema_ptr schema::make_reversed() const {
    return make_lw_shared<schema>(schema::reversed_tag{}, *this);
}

schema_ptr schema::get_reversed() const {
    return local_schema_registry().get_or_load(reversed(_raw._version), [this] (table_schema_version) {
        return frozen_schema(make_reversed());
    });
}

raw_view_info::raw_view_info(table_id base_id, sstring base_name, bool include_all_columns, sstring where_clause)
        : _base_id(std::move(base_id))
        , _base_name(std::move(base_name))
        , _include_all_columns(include_all_columns)
        , _where_clause(where_clause)
{ }

column_computation_ptr column_computation::deserialize(bytes_view raw) {
    rjson::value parsed = rjson::parse(std::string_view(reinterpret_cast<const char*>(raw.begin()), reinterpret_cast<const char*>(raw.end())));
    if (!parsed.IsObject()) {
        throw std::runtime_error(format("Invalid column computation value: {}", parsed));
    }
    const rjson::value* type_json = rjson::find(parsed, "type");
    if (!type_json || !type_json->IsString()) {
        throw std::runtime_error(format("Type {} is not convertible to string", *type_json));
    }
    const std::string_view type = rjson::to_string_view(*type_json);
    if (type == "token") {
        return std::make_unique<legacy_token_column_computation>();
    }
    if (type == "token_v2") {
        return std::make_unique<token_column_computation>();
    }
    if (type.starts_with("collection_")) {
        const rjson::value* collection_name = rjson::find(parsed, "collection_name");

        if (collection_name && collection_name->IsString()) {
            auto collection = rjson::to_string_view(*collection_name);
            auto collection_as_bytes = bytes(collection.begin(), collection.end());
            if (auto collection = collection_column_computation::for_target_type(type, collection_as_bytes)) {
                return collection->clone();
            }
        }
    }
    throw std::runtime_error(format("Incorrect column computation type {} found when parsing {}", *type_json, parsed));
}

bytes legacy_token_column_computation::serialize() const {
    rjson::value serialized = rjson::empty_object();
    rjson::add(serialized, "type", rjson::from_string("token"));
    return to_bytes(rjson::print(serialized));
}

bytes legacy_token_column_computation::compute_value(const schema& schema, const partition_key& key) const {
    return {dht::get_token(schema, key).data()};
}

bytes token_column_computation::serialize() const {
    rjson::value serialized = rjson::empty_object();
    rjson::add(serialized, "type", rjson::from_string("token_v2"));
    return to_bytes(rjson::print(serialized));
}

bytes token_column_computation::compute_value(const schema& schema, const partition_key& key) const {
    auto long_value = dht::token::to_int64(dht::get_token(schema, key));
    return long_type->decompose(long_value);
}

bytes collection_column_computation::serialize() const {
    rjson::value serialized = rjson::empty_object();
    const char* type = nullptr;
    switch (_kind) {
        case kind::keys:
            type = "collection_keys";
            break;
        case kind::values:
            type = "collection_values";
            break;
        case kind::entries:
            type = "collection_entries";
            break;
    }
    rjson::add(serialized, "type", rjson::from_string(type));
    rjson::add(serialized, "collection_name", rjson::from_string(to_sstring_view(_collection_name)));
    return to_bytes(rjson::print(serialized));
}

column_computation_ptr collection_column_computation::for_target_type(std::string_view type, const bytes& collection_name) {
    if (type == "collection_keys") {
        return collection_column_computation::for_keys(collection_name).clone();
    }
    if (type == "collection_values") {
        return collection_column_computation::for_values(collection_name).clone();
    }
    if (type == "collection_entries") {
        return collection_column_computation::for_entries(collection_name).clone();
    }
    return {};
}

void collection_column_computation::operate_on_collection_entries(
        std::invocable<collection_kv*, collection_kv*, tombstone> auto&& old_and_new_row_func, const schema& schema,
        const partition_key& key, const db::view::clustering_or_static_row& update, const std::optional<db::view::clustering_or_static_row>& existing) const {

    const column_definition* cdef = schema.get_column_definition(_collection_name);

    decltype(collection_mutation_view_description::cells) update_cells, existing_cells;

    const auto* update_cell = update.cells().find_cell(cdef->id);
    tombstone update_tombstone = update.tomb().tomb();
    if (update_cell) {
        collection_mutation_view update_col_view = update_cell->as_collection_mutation();
        update_col_view.with_deserialized(*(cdef->type), [&update_cells, &update_tombstone] (collection_mutation_view_description descr) {
            update_tombstone.apply(descr.tomb);
            update_cells = descr.cells;
        });
    }
    if (existing) {
        const auto* existing_cell = existing->cells().find_cell(cdef->id);
        if (existing_cell) {
            collection_mutation_view existing_col_view = existing_cell->as_collection_mutation();
            existing_col_view.with_deserialized(*(cdef->type), [&existing_cells] (collection_mutation_view_description descr) {
                existing_cells = descr.cells;
            });
        }
    }

    auto compare = [](const collection_kv& p1, const collection_kv& p2) {
        return p1.first <=> p2.first;
    };

    // Both collections are assumed to be sorted by the keys.
    auto existing_it = existing_cells.begin();
    auto update_it = update_cells.begin();

    auto is_existing_end = [&] {
        return existing_it == existing_cells.end();
    };
    auto is_update_end = [&] {
        return update_it == update_cells.end();
    };
    while (!(is_existing_end() && is_update_end())) {
        std::strong_ordering cmp = [&] {
            if (is_existing_end()) {
                return std::strong_ordering::greater;
            } else if (is_update_end()) {
                return std::strong_ordering::less;
            }
            return compare(*existing_it, *update_it);
        }();

        auto existing_ptr = [&] () -> collection_kv* {
            return (!is_existing_end() && cmp <= 0) ? &*existing_it : nullptr;
        };
        auto update_ptr = [&] () -> collection_kv* {
            return (!is_update_end() && cmp >= 0) ? &*update_it : nullptr;
        };

        old_and_new_row_func(existing_ptr(), update_ptr(), update_tombstone);
        if (cmp <= 0) {
            ++existing_it;
        }
        if (cmp >= 0) {
            ++update_it;
        }
    }
}

bytes collection_column_computation::compute_value(const schema&, const partition_key&) const {
    throw std::runtime_error(fmt::format("{}: not supported", __PRETTY_FUNCTION__));
}

std::vector<db::view::view_key_and_action> collection_column_computation::compute_values_with_action(const schema& schema, const partition_key& key,
        const db::view::clustering_or_static_row& update, const std::optional<db::view::clustering_or_static_row>& existing) const {
    using collection_kv = std::pair<bytes_view, atomic_cell_view>;
    auto serialize_cell = [_kind = _kind](const collection_kv& kv) -> bytes {
        using kind = collection_column_computation::kind;
        auto& [key, value] = kv;
        switch (_kind) {
            case kind::keys:
                return bytes(key);
            case kind::values:
                return value.value().linearize();
            case kind::entries:
                bytes_opt elements[] = {bytes(key), value.value().linearize()};
                return tuple_type_impl::build_value(elements);
        }
        std::abort(); // compiler will error
    };

    std::vector<db::view::view_key_and_action> ret;

    auto compute_row_marker = [] (auto&& cell) -> row_marker {
        return cell.is_live_and_has_ttl() ? row_marker(cell.timestamp(), cell.ttl(), cell.expiry()) : row_marker(cell.timestamp());
    };

    auto fn = [&ret, &compute_row_marker, &serialize_cell] (collection_kv* existing, collection_kv* update, tombstone tomb) {
        api::timestamp_type operation_ts = tomb.timestamp;
        if (existing && update && compare_atomic_cell_for_merge(existing->second, update->second) == 0) {
            return;
        }
        if (update) {
            operation_ts = update->second.timestamp();
            if (update->second.is_live()) {
                row_marker rm = compute_row_marker(update->second);
                ret.push_back({serialize_cell(*update), {rm}});
            }
        }
        operation_ts -= 1;
        if (existing && existing->second.is_live()) {
            db::view::view_key_and_action::shadowable_tombstone_tag tag{operation_ts};
            ret.push_back({serialize_cell(*existing), {tag}});
        }
    };
    operate_on_collection_entries(fn, schema, key, update, existing);
    return ret;
}

bool operator==(const raw_view_info& x, const raw_view_info& y) {
    return x._base_id == y._base_id
        && x._base_name == y._base_name
        && x._include_all_columns == y._include_all_columns
        && x._where_clause == y._where_clause;
}

auto fmt::formatter<raw_view_info>::format(const raw_view_info& view, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(),
                          "ViewInfo{{baseTableId={}, baseTableName={}, includeAllColumns={}, whereClause={}}}",
                          view._base_id, view._base_name, view._include_all_columns, view._where_clause);
}

auto fmt::formatter<view_ptr>::format(const view_ptr& view, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    if (view) {
        return fmt::format_to(ctx.out(), "{}", *view);
    } else {
        return fmt::format_to(ctx.out(), "null");
    }
}

namespace std {

std::ostream& operator<<(std::ostream& os, const table_info& ti) {
    fmt::print(os, "{}", ti);
    return os;
}

} // namespace std

auto fmt::formatter<table_info>::format(const table_info& ti,
                                             fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "table{{name={}, id={}}}", ti.name, ti.id);
}

schema_mismatch_error::schema_mismatch_error(table_schema_version expected, const schema& access)
    : std::runtime_error(fmt::format("Attempted to deserialize schema-dependent object of version {} using {}.{} {}",
        expected, access.ks_name(), access.cf_name(), access.version()))
{ }

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

#pragma once

#include <functional>
#include <optional>
#include <unordered_map>
#include <boost/range/iterator_range.hpp>
#include <boost/range/join.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/dynamic_bitset.hpp>

#include "cql3/column_specification.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/backtrace.hh>
#include "types.hh"
#include "compound.hh"
#include "gc_clock.hh"
#include "utils/UUID.hh"
#include "compress.hh"
#include "compaction/compaction_strategy_type.hh"
#include "caching_options.hh"
#include "column_computation.hh"
#include "timestamp.hh"

namespace dht {

class i_partitioner;
class sharder;

}

namespace cdc {
class options;
}

class database;

using column_count_type = uint32_t;

// Column ID, unique within column_kind
using column_id = column_count_type;

// Column ID unique within a schema. Enum class to avoid
// mixing wtih column id.
enum class ordinal_column_id: column_count_type {};

std::ostream& operator<<(std::ostream& os, ordinal_column_id id);

// Maintains a set of columns used in a query. The columns are
// identified by ordinal_id.
//
// @sa column_definition::ordinal_id.
class column_set {
public:
    using bitset = boost::dynamic_bitset<uint64_t>;
    using size_type = bitset::size_type;

    // column_count_type is more narrow than size_type, but truncating a size_type max value does
    // give column_count_type max value. This is used to avoid extra branching in
    // find_first()/find_next().
    static_assert(static_cast<column_count_type>(boost::dynamic_bitset<uint64_t>::npos) == ~static_cast<column_count_type>(0));
    static constexpr ordinal_column_id npos = static_cast<ordinal_column_id>(bitset::npos);

    explicit column_set(column_count_type num_bits = 0)
        : _mask(num_bits)
    {
    }

    void resize(column_count_type num_bits) {
        _mask.resize(num_bits);
    }

    // Set the appropriate bit for column id.
    void set(ordinal_column_id id) {
        column_count_type bit = static_cast<column_count_type>(id);
        _mask.set(bit);
    }
    // Test the mask for use of a given column id.
    bool test(ordinal_column_id id) const {
        column_count_type bit = static_cast<column_count_type>(id);
        return _mask.test(bit);
    }
    // @sa boost::dynamic_bistet docs
    size_type count() const { return _mask.count(); }
    ordinal_column_id find_first() const {
        return static_cast<ordinal_column_id>(_mask.find_first());
    }
    ordinal_column_id find_next(ordinal_column_id pos) const {
        return static_cast<ordinal_column_id>(_mask.find_next(static_cast<column_count_type>(pos)));
    }
    // Logical or
    void union_with(const column_set& with) {
        _mask |= with._mask;
    }

private:
    bitset _mask;
};

// Cluster-wide identifier of schema version of particular table.
//
// The version changes the value not only on structural changes but also
// temporal. For example, schemas with the same set of columns but created at
// different times should have different versions. This allows nodes to detect
// if the version they see was already synchronized with or not even if it has
// the same structure as the past versions.
//
// Schema changes merged in any order should result in the same final version.
//
// When table_schema_version changes, schema_tables::calculate_schema_digest() should
// also change when schema mutations are applied.
using table_schema_version = utils::UUID;

class schema;
class schema_registry;
class schema_registry_entry;
class schema_builder;

// Useful functions to manipulate the schema's comparator field
namespace cell_comparator {
sstring to_sstring(const schema& s);
bool check_compound(sstring comparator);
void read_collections(schema_builder& builder, sstring comparator);
}

namespace db {
class extensions;
}
// make sure these match the order we like columns back from schema
enum class column_kind { partition_key, clustering_key, static_column, regular_column };

enum class column_view_virtual { no, yes };

sstring to_sstring(column_kind k);
bool is_compatible(column_kind k1, column_kind k2);

enum class cf_type : uint8_t {
    standard,
    super,
};

inline sstring cf_type_to_sstring(cf_type t) {
    if (t == cf_type::standard) {
        return "Standard";
    } else if (t == cf_type::super) {
        return "Super";
    }
    throw std::invalid_argument(format("unknown type: {:d}\n", uint8_t(t)));
}

inline cf_type sstring_to_cf_type(sstring name) {
    if (name == "Standard") {
        return cf_type::standard;
    } else if (name == "Super") {
        return cf_type::super;
    }
    throw std::invalid_argument(format("unknown type: {}\n", name));
}

struct speculative_retry {
    enum class type {
        NONE, CUSTOM, PERCENTILE, ALWAYS
    };
private:
    type _t;
    double _v;
public:
    speculative_retry(type t, double v) : _t(t), _v(v) {}

    sstring to_sstring() const {
        if (_t == type::NONE) {
            return "NONE";
        } else if (_t == type::ALWAYS) {
            return "ALWAYS";
        } else if (_t == type::CUSTOM) {
            return format("{:.2f}ms", _v);
        } else if (_t == type::PERCENTILE) {
            return format("{:.1f}PERCENTILE", 100 * _v);
        } else {
            throw std::invalid_argument(format("unknown type: {:d}\n", uint8_t(_t)));
        }
    }
    static speculative_retry from_sstring(sstring str) {
        std::transform(str.begin(), str.end(), str.begin(), ::toupper);

        sstring ms("MS");
        sstring percentile("PERCENTILE");

        auto convert = [&str] (sstring& t) {
            try {
                return boost::lexical_cast<double>(str.substr(0, str.size() - t.size()));
            } catch (boost::bad_lexical_cast& e) {
                throw std::invalid_argument(format("cannot convert {} to speculative_retry\n", str));
            }
        };

        type t;
        double v = 0;
        if (str == "NONE") {
            t = type::NONE;
        } else if (str == "ALWAYS") {
            t = type::ALWAYS;
        } else if (str.compare(str.size() - ms.size(), ms.size(), ms) == 0) {
            t = type::CUSTOM;
            v = convert(ms);
        } else if (str.compare(str.size() - percentile.size(), percentile.size(), percentile) == 0) {
            t = type::PERCENTILE;
            v = convert(percentile) / 100;
        } else {
            throw std::invalid_argument(format("cannot convert {} to speculative_retry\n", str));
        }
        return speculative_retry(t, v);
    }
    type get_type() const {
        return _t;
    }
    double get_value() const {
        return _v;
    }
    bool operator==(const speculative_retry& other) const {
        return _t == other._t && _v == other._v;
    }
    bool operator!=(const speculative_retry& other) const {
        return !(*this == other);
    }
};

typedef std::unordered_map<sstring, sstring> index_options_map;

enum class index_metadata_kind {
    keys,
    custom,
    composites,
};

class index_metadata final {
public:
    struct is_local_index_tag {};
    using is_local_index = bool_class<is_local_index_tag>;
private:
    utils::UUID _id;
    sstring _name;
    index_metadata_kind _kind;
    index_options_map _options;
    bool _local;
public:
    index_metadata(const sstring& name, const index_options_map& options, index_metadata_kind kind, is_local_index local);
    bool operator==(const index_metadata& other) const;
    bool equals_noname(const index_metadata& other) const;
    const utils::UUID& id() const;
    const sstring& name() const;
    const index_metadata_kind kind() const;
    const index_options_map& options() const;
    bool local() const;
    static sstring get_default_index_name(const sstring& cf_name, std::optional<sstring> root);
};

class column_definition final {
public:
    struct name_comparator {
        data_type type;
        name_comparator(data_type type) : type(type) {}
        bool operator()(const column_definition& cd1, const column_definition& cd2) const {
            return type->less(cd1.name(), cd2.name());
        }
    };
private:
    bytes _name;
    api::timestamp_type _dropped_at;
    bool _is_atomic;
    bool _is_counter;
    column_view_virtual _is_view_virtual;
    column_computation_ptr _computation;

    struct thrift_bits {
        thrift_bits()
            : is_on_all_components(0)
        {}
        uint8_t is_on_all_components : 1;
        // more...?
    };

    thrift_bits _thrift_bits;
    friend class schema;
public:
    column_definition(bytes name, data_type type, column_kind kind,
        column_id component_index = 0,
        column_view_virtual view_virtual = column_view_virtual::no,
        column_computation_ptr = nullptr,
        api::timestamp_type dropped_at = api::missing_timestamp);

    data_type type;

    // Unique within (kind, schema instance).
    // schema::position() and component_index() depend on the fact that for PK columns this is
    // equivalent to component index.
    column_id id;

    // Unique within schema instance
    ordinal_column_id ordinal_id;

    column_kind kind;
    lw_shared_ptr<cql3::column_specification> column_specification;

    // NOTICE(sarna): This copy constructor is hand-written instead of default,
    // because it involves deep copying of the computation object.
    // Computation has a strict ownership policy provided by
    // unique_ptr, and as such cannot rely on default copying.
    column_definition(const column_definition& other)
            : _name(other._name)
            , _dropped_at(other._dropped_at)
            , _is_atomic(other._is_atomic)
            , _is_counter(other._is_counter)
            , _is_view_virtual(other._is_view_virtual)
            , _computation(other.get_computation_ptr())
            , _thrift_bits(other._thrift_bits)
            , type(other.type)
            , id(other.id)
            , ordinal_id(other.ordinal_id)
            , kind(other.kind)
            , column_specification(other.column_specification)
        {}

    column_definition& operator=(const column_definition& other) {
        if (this == &other) {
            return *this;
        }
        column_definition tmp(other);
        *this = std::move(tmp);
        return *this;
    }

    column_definition& operator=(column_definition&& other) = default;

    bool is_static() const { return kind == column_kind::static_column; }
    bool is_regular() const { return kind == column_kind::regular_column; }
    bool is_partition_key() const { return kind == column_kind::partition_key; }
    bool is_clustering_key() const { return kind == column_kind::clustering_key; }
    bool is_primary_key() const { return kind == column_kind::partition_key || kind == column_kind::clustering_key; }
    bool is_atomic() const { return _is_atomic; }
    bool is_multi_cell() const { return !_is_atomic; }
    bool is_counter() const { return _is_counter; }
    // "virtual columns" appear in a materialized view as placeholders for
    // unselected columns, with liveness information but without data, and
    // allow view rows to remain alive despite having no data (issue #3362).
    // These columns should be hidden from the user's SELECT queries.
    bool is_view_virtual() const { return _is_view_virtual == column_view_virtual::yes; }
    column_view_virtual view_virtual() const { return _is_view_virtual; }
    // Computed column values are generated from other columns (and possibly other sources) during updates.
    // Their values are still stored on disk, same as a regular columns.
    bool is_computed() const { return bool(_computation); }
    const column_computation& get_computation() const { return *_computation; }
    column_computation_ptr get_computation_ptr() const {
        return _computation ? _computation->clone() : nullptr;
    }
    void set_computed(column_computation_ptr computation) { _computation = std::move(computation); }
    // Columns hidden from CQL cannot be in any way retrieved by the user,
    // either explicitly or via the '*' operator, or functions, aggregates, etc.
    bool is_hidden_from_cql() const { return is_view_virtual(); }
    const sstring& name_as_text() const;
    const bytes& name() const;
    sstring name_as_cql_string() const;
    friend std::ostream& operator<<(std::ostream& os, const column_definition& cd);
    friend std::ostream& operator<<(std::ostream& os, const column_definition* cd) {
        return cd != nullptr ? os << *cd : os << "(null)";
    }
    bool has_component_index() const {
        return is_primary_key();
    }
    uint32_t component_index() const {
        assert(has_component_index());
        return id;
    }
    uint32_t position() const {
        if (has_component_index()) {
            return component_index();
        }
        return 0;
    }
    bool is_on_all_components() const;
    bool is_part_of_cell_name() const {
        return is_regular() || is_static();
    }
    api::timestamp_type dropped_at() const { return _dropped_at; }
    friend bool operator==(const column_definition&, const column_definition&);
};

class schema_builder;

/*
 * Sub-schema for thrift aspects. Should be kept isolated (and starved)
 */
class thrift_schema {
    bool _compound = true;
    bool _is_dynamic = false;
public:
    bool has_compound_comparator() const;
    bool is_dynamic() const;
    friend class schema;
};

bool operator==(const column_definition&, const column_definition&);
inline bool operator!=(const column_definition& a, const column_definition& b) { return !(a == b); }

static constexpr int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
static constexpr int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;
static constexpr int DEFAULT_MIN_INDEX_INTERVAL = 128;
static constexpr int DEFAULT_GC_GRACE_SECONDS = 864000;

// Unsafe to access across shards.
// Safe to copy across shards.
class column_mapping_entry {
    bytes _name;
    data_type _type;
    bool _is_atomic;
public:
    column_mapping_entry(bytes name, data_type type)
        : _name(std::move(name)), _type(std::move(type)), _is_atomic(_type->is_atomic()) { }
    column_mapping_entry(bytes name, sstring type_name);
    column_mapping_entry(const column_mapping_entry&);
    column_mapping_entry& operator=(const column_mapping_entry&);
    column_mapping_entry(column_mapping_entry&&) = default;
    column_mapping_entry& operator=(column_mapping_entry&&) = default;
    const bytes& name() const { return _name; }
    const data_type& type() const { return _type; }
    const sstring& type_name() const { return _type->name(); }
    bool is_atomic() const { return _is_atomic; }
};

bool operator==(const column_mapping_entry& lhs, const column_mapping_entry& rhs);
bool operator!=(const column_mapping_entry& lhs, const column_mapping_entry& rhs);

// Encapsulates information needed for converting mutations between different schema versions.
//
// Unsafe to access across shards.
// Safe to copy across shards.
class column_mapping {
private:
    // Contains _n_static definitions for static columns followed by definitions for regular columns,
    // both ordered by consecutive column_ids.
    // Primary key column sets are not mutable so we don't need to map them.
    std::vector<column_mapping_entry> _columns;
    column_count_type _n_static = 0;
public:
    column_mapping() {}
    column_mapping(std::vector<column_mapping_entry> columns, column_count_type n_static)
            : _columns(std::move(columns))
            , _n_static(n_static)
    { }
    const std::vector<column_mapping_entry>& columns() const { return _columns; }
    column_count_type n_static() const { return _n_static; }
    const column_mapping_entry& column_at(column_kind kind, column_id id) const {
        assert(kind == column_kind::regular_column || kind == column_kind::static_column);
        return kind == column_kind::regular_column ? regular_column_at(id) : static_column_at(id);
    }
    const column_mapping_entry& static_column_at(column_id id) const {
        if (id >= _n_static) {
            throw std::out_of_range(format("static column id {:d} >= {:d}", id, _n_static));
        }
        return _columns[id];
    }
    const column_mapping_entry& regular_column_at(column_id id) const {
        auto n_regular = _columns.size() - _n_static;
        if (id >= n_regular) {
            throw std::out_of_range(format("regular column id {:d} >= {:d}", id, n_regular));
        }
        return _columns[id + _n_static];
    }
    friend std::ostream& operator<<(std::ostream& out, const column_mapping& cm);
};

bool operator==(const column_mapping& lhs, const column_mapping& rhs);

/**
 * Augments a schema with fields related to materialized views.
 * Effectively immutable.
 */
class raw_view_info final {
    utils::UUID _base_id;
    sstring _base_name;
    bool _include_all_columns;
    sstring _where_clause;
public:
    raw_view_info(utils::UUID base_id, sstring base_name, bool include_all_columns, sstring where_clause);

    const utils::UUID& base_id() const {
        return _base_id;
    }

    const sstring& base_name() const {
        return _base_name;
    }

    bool include_all_columns() const {
        return _include_all_columns;
    }

    const sstring& where_clause() const {
        return _where_clause;
    }

    friend bool operator==(const raw_view_info&, const raw_view_info&);
    friend std::ostream& operator<<(std::ostream& os, const raw_view_info& view);
};

bool operator==(const raw_view_info&, const raw_view_info&);
std::ostream& operator<<(std::ostream& os, const raw_view_info& view);

class view_info;

// Represents a column set which is compactible with Cassandra 3.x.
//
// This layout differs from the layout Scylla uses in schema/schema_builder for static compact tables.
// For such tables, Scylla expects all columns to be of regular type and no clustering columns,
// whereas in v3 those columns are static and there is a clustering column with type matching the
// cell name comparator and a regular column with type matching the default validator.
// See issues #2555 and #1474.
class v3_columns {
    bool _is_dense = false;
    bool _is_compound = false;
    std::vector<column_definition> _columns;
    std::unordered_map<bytes, const column_definition*> _columns_by_name;
public:
    v3_columns(std::vector<column_definition> columns, bool is_dense, bool is_compound);
    v3_columns() = default;
    v3_columns(v3_columns&&) = default;
    v3_columns& operator=(v3_columns&&) = default;
    v3_columns(const v3_columns&) = delete;
    static v3_columns from_v2_schema(const schema&);
public:
    const std::vector<column_definition>& all_columns() const;
    const std::unordered_map<bytes, const column_definition*>& columns_by_name() const;
    bool is_static_compact() const;
    bool is_compact() const;
    void apply_to(schema_builder&) const;
};

namespace query {
class partition_slice;
}

/**
 * Schema extension. An opaque type representing
 * entries in the "extensions" part of a table/view (see schema_tables).
 *
 * An extension has a name (the mapping key), and it can re-serialize
 * itself to bytes again, when we write back into schema tables.
 *
 * Code using a particular extension can locate it by name in the schema map,
 * and barring the "is_placeholder" says true, cast it to whatever might
 * be the expeceted implementation.
 *
 * We allow placeholder object since an extension written to schema tables
 * might be unavailable on next boot/other node. To avoid loosing the config data,
 * a placeholder object is put into schema map, which at least can
 * re-serialize the data back.
 *
 */
class schema_extension {
public:
    virtual ~schema_extension() {};
    virtual bytes serialize() const = 0;
    virtual bool is_placeholder() const {
        return false;
    }
};

class schema;

using schema_ptr = lw_shared_ptr<const schema>;

/*
 * Effectively immutable.
 * Not safe to access across cores because of shared_ptr's.
 * Use global_schema_ptr for safe across-shard access.
 */
class schema final : public enable_lw_shared_from_this<schema> {
    friend class v3_columns;
public:
    struct dropped_column {
        data_type type;
        api::timestamp_type timestamp;
        bool operator==(const dropped_column& rhs) const {
            return type == rhs.type && timestamp == rhs.timestamp;
        }
    };
    using extensions_map = std::map<sstring, ::shared_ptr<schema_extension>>;
private:
    // More complex fields are derived from these inside rebuild().
    // Contains only fields which can be safely default-copied.
    struct raw_schema {
        raw_schema(utils::UUID id);
        utils::UUID _id;
        sstring _ks_name;
        sstring _cf_name;
        // regular columns are sorted by name
        // static columns are sorted by name, but present only when there's any clustering column
        std::vector<column_definition> _columns;
        sstring _comment;
        gc_clock::duration _default_time_to_live = gc_clock::duration::zero();
        data_type _regular_column_name_type;
        data_type _default_validation_class = bytes_type;
        double _bloom_filter_fp_chance = 0.01;
        compression_parameters _compressor_params;
        extensions_map _extensions;
        bool _is_dense = false;
        bool _is_compound = true;
        bool _is_counter = false;
        cf_type _type = cf_type::standard;
        int32_t _gc_grace_seconds = DEFAULT_GC_GRACE_SECONDS;
        std::optional<int32_t> _paxos_grace_seconds;
        double _dc_local_read_repair_chance = 0.0;
        double _read_repair_chance = 0.0;
        double _crc_check_chance = 1;
        int32_t _min_compaction_threshold = DEFAULT_MIN_COMPACTION_THRESHOLD;
        int32_t _max_compaction_threshold = DEFAULT_MAX_COMPACTION_THRESHOLD;
        int32_t _min_index_interval = DEFAULT_MIN_INDEX_INTERVAL;
        int32_t _max_index_interval = 2048;
        int32_t _memtable_flush_period = 0;
        speculative_retry _speculative_retry = ::speculative_retry(speculative_retry::type::PERCENTILE, 0.99);
        // This is the compaction strategy that will be used by default on tables which don't have one explicitly specified.
        sstables::compaction_strategy_type _compaction_strategy = sstables::compaction_strategy_type::size_tiered;
        std::map<sstring, sstring> _compaction_strategy_options;
        bool _compaction_enabled = true;
        caching_options _caching_options;
        table_schema_version _version;
        std::unordered_map<sstring, dropped_column> _dropped_columns;
        std::map<bytes, data_type> _collections;
        std::unordered_map<sstring, index_metadata> _indices_by_name;
        // The flag is not stored in the schema mutation and does not affects schema digest.
        // It is set locally on a system tables that should be extra durable
        bool _wait_for_sync = false; // true if all writes using this schema have to be synced immediately by commitlog
        std::reference_wrapper<const dht::i_partitioner> _partitioner;
        // Sharding info is not stored in the schema mutation and does not affect
        // schema digest. It is also not set locally on a schema tables.
        std::reference_wrapper<const dht::sharder> _sharder;
    };
    raw_schema _raw;
    thrift_schema _thrift;
    v3_columns _v3_columns;
    mutable schema_registry_entry* _registry_entry = nullptr;
    std::unique_ptr<::view_info> _view_info;

    const std::array<column_count_type, 3> _offsets;

    inline column_count_type column_offset(column_kind k) const {
        return k == column_kind::partition_key ? 0 : _offsets[column_count_type(k) - 1];
    }

    std::unordered_map<bytes, const column_definition*> _columns_by_name;
    lw_shared_ptr<compound_type<allow_prefixes::no>> _partition_key_type;
    lw_shared_ptr<compound_type<allow_prefixes::yes>> _clustering_key_type;
    column_mapping _column_mapping;
    shared_ptr<query::partition_slice> _full_slice;
    column_count_type _clustering_key_size;
    column_count_type _regular_column_count;
    column_count_type _static_column_count;

    extensions_map& extensions() {
        return _raw._extensions;
    }

    friend class db::extensions;
    friend class schema_builder;
public:
    using row_column_ids_are_ordered_by_name = std::true_type;

    typedef std::vector<column_definition> columns_type;
    typedef typename columns_type::iterator iterator;
    typedef typename columns_type::const_iterator const_iterator;
    typedef boost::iterator_range<iterator> iterator_range_type;
    typedef boost::iterator_range<const_iterator> const_iterator_range_type;

    static constexpr int32_t NAME_LENGTH = 48;


    struct column {
        bytes name;
        data_type type;
    };
private:
    struct reversed_tag { };

    lw_shared_ptr<cql3::column_specification> make_column_specification(const column_definition& def);
    void rebuild();
    schema(const schema&, const std::function<void(schema&)>&);
    class private_tag{};
public:
    schema(private_tag, schema_registry* registry, const raw_schema&, std::optional<raw_view_info>);
    schema(const schema&);
    // See \ref make_reversed().
    schema(reversed_tag, const schema&);
    ~schema();
    table_schema_version version() const {
        return _raw._version;
    }
    double bloom_filter_fp_chance() const {
        return _raw._bloom_filter_fp_chance;
    }
    sstring thrift_key_validator() const;
    const compression_parameters& get_compressor_params() const {
        return _raw._compressor_params;
    }
    const extensions_map& extensions() const {
        return _raw._extensions;
    }
    bool is_dense() const {
        return _raw._is_dense;
    }

    bool is_compound() const {
        return _raw._is_compound;
    }

    bool is_cql3_table() const {
        return !is_super() && !is_dense() && is_compound();
    }
    bool is_compact_table() const {
        return !is_cql3_table();
    }
    bool is_static_compact_table() const {
        return !is_super() && !is_dense() && !is_compound();
    }

    thrift_schema& thrift() {
        return _thrift;
    }
    const thrift_schema& thrift() const {
        return _thrift;
    }
    const utils::UUID& id() const {
        return _raw._id;
    }
    const sstring& comment() const {
        return _raw._comment;
    }
    bool is_counter() const {
        return _raw._is_counter;
    }

    const cf_type type() const {
        return _raw._type;
    }

    bool is_super() const {
        return _raw._type == cf_type::super;
    }

    gc_clock::duration gc_grace_seconds() const {
        auto seconds = std::chrono::seconds(_raw._gc_grace_seconds);
        return std::chrono::duration_cast<gc_clock::duration>(seconds);
    }

    gc_clock::duration paxos_grace_seconds() const;

    double dc_local_read_repair_chance() const {
        return _raw._dc_local_read_repair_chance;
    }

    double read_repair_chance() const {
        return _raw._read_repair_chance;
    }
    double crc_check_chance() const {
        return _raw._crc_check_chance;
    }

    int32_t min_compaction_threshold() const {
        return _raw._min_compaction_threshold;
    }

    int32_t max_compaction_threshold() const {
        return _raw._max_compaction_threshold;
    }

    int32_t min_index_interval() const {
        return _raw._min_index_interval;
    }

    int32_t max_index_interval() const {
        return _raw._max_index_interval;
    }

    int32_t memtable_flush_period() const {
        return _raw._memtable_flush_period;
    }

    sstables::compaction_strategy_type configured_compaction_strategy() const {
        return _raw._compaction_strategy;
    }

    sstables::compaction_strategy_type compaction_strategy() const {
        return _raw._compaction_enabled ? _raw._compaction_strategy : sstables::compaction_strategy_type::null;
    }

    const std::map<sstring, sstring>& compaction_strategy_options() const {
        return _raw._compaction_strategy_options;
    }

    bool compaction_enabled() const {
        return _raw._compaction_enabled;
    }

    const cdc::options& cdc_options() const;

    const ::speculative_retry& speculative_retry() const {
        return _raw._speculative_retry;
    }

    const ::caching_options& caching_options() const {
        return _raw._caching_options;
    }

    static void set_default_partitioner(const sstring& class_name, unsigned ignore_msb = 0);
    const dht::i_partitioner& get_partitioner() const;
    const dht::sharder& get_sharder() const;
    bool has_custom_partitioner() const;

    const column_definition* get_column_definition(const bytes& name) const;
    const column_definition& column_at(column_kind, column_id) const;
    // Find a column definition given column ordinal id in the schema
    const column_definition& column_at(ordinal_column_id ordinal_id) const;
    const_iterator regular_begin() const;
    const_iterator regular_end() const;
    const_iterator regular_lower_bound(const bytes& name) const;
    const_iterator regular_upper_bound(const bytes& name) const;
    const_iterator static_begin() const;
    const_iterator static_end() const;
    const_iterator static_lower_bound(const bytes& name) const;
    const_iterator static_upper_bound(const bytes& name) const;
    static data_type column_name_type(const column_definition& def, const data_type& regular_column_name_type);
    data_type column_name_type(const column_definition& def) const;
    const column_definition& clustering_column_at(column_id id) const;
    const column_definition& regular_column_at(column_id id) const;
    const column_definition& static_column_at(column_id id) const;
    bool is_last_partition_key(const column_definition& def) const;
    bool has_multi_cell_collections() const;
    bool has_static_columns() const;
    column_count_type columns_count(column_kind kind) const;
    column_count_type partition_key_size() const;
    column_count_type clustering_key_size() const { return _clustering_key_size; }
    column_count_type static_columns_count() const { return _static_column_count; }
    column_count_type regular_columns_count() const { return _regular_column_count; }
    column_count_type all_columns_count() const { return _raw._columns.size(); }
    // Returns a range of column definitions
    const_iterator_range_type partition_key_columns() const;
    // Returns a range of column definitions
    const_iterator_range_type clustering_key_columns() const;
    // Returns a range of column definitions
    const_iterator_range_type static_columns() const;
    // Returns a range of column definitions
    const_iterator_range_type regular_columns() const;
    // Returns a range of column definitions
    const_iterator_range_type columns(column_kind) const;
    // Returns a range of column definitions

    typedef boost::range::joined_range<const_iterator_range_type, const_iterator_range_type>
        select_order_range;

    select_order_range all_columns_in_select_order() const;
    uint32_t position(const column_definition& column) const;

    const columns_type& all_columns() const {
        return _raw._columns;
    }

    const std::unordered_map<bytes, const column_definition*>& columns_by_name() const {
        return _columns_by_name;
    }

    const auto& dropped_columns() const {
        return _raw._dropped_columns;
    }

    const auto& collections() const {
        return _raw._collections;
    }

    gc_clock::duration default_time_to_live() const {
        return _raw._default_time_to_live;
    }

    data_type make_legacy_default_validator() const;

    const sstring& ks_name() const {
        return _raw._ks_name;
    }
    const sstring& cf_name() const {
        return _raw._cf_name;
    }
    const lw_shared_ptr<compound_type<allow_prefixes::no>>& partition_key_type() const {
        return _partition_key_type;
    }
    const lw_shared_ptr<compound_type<allow_prefixes::yes>>& clustering_key_type() const {
        return _clustering_key_type;
    }
    const lw_shared_ptr<compound_type<allow_prefixes::yes>>& clustering_key_prefix_type() const {
        return _clustering_key_type;
    }
    const data_type& regular_column_name_type() const {
        return _raw._regular_column_name_type;
    }
    const data_type& static_column_name_type() const {
        return utf8_type;
    }
    const std::unique_ptr<::view_info>& view_info() const {
        return _view_info;
    }
    bool is_view() const {
        return bool(_view_info);
    }
    const query::partition_slice& full_slice() const {
        return *_full_slice;
    }
    // Returns all index names of this schema.
    std::vector<sstring> index_names() const;
    // Returns all indices of this schema.
    std::vector<index_metadata> indices() const;
    const std::unordered_map<sstring, index_metadata>& all_indices() const;
    // Search for an index with a given name.
    bool has_index(const sstring& index_name) const;
    // Search for an existing index with same kind and options.
    std::optional<index_metadata> find_index_noname(const index_metadata& target) const;
    friend std::ostream& operator<<(std::ostream& os, const schema& s);
    /*!
     * \brief stream the CQL DESCRIBE output.
     *
     * CQL DESCRIBE is implemented at the driver level. This method mimic that functionality
     * inside Scylla.
     *
     * The output of DESCRIBE is the CQL command to create the described table with its indexes and views.
     *
     * For tables with Indexes or Materialized Views, the CQL DESCRIBE is split between the base and view tables.
     * Calling the describe method on the base table schema would result with the CQL "CREATE TABLE"
     * command for creating that table only.
     *
     * Calling the describe method on a view schema would result with the appropriate "CREATE MATERIALIZED VIEW"
     * or "CREATE INDEX" depends on the type of index that schema describes (ie. Materialized View, Global
     * Index or Local Index).
     *
     */
    std::ostream& describe(database& db, std::ostream& os) const;
    friend bool operator==(const schema&, const schema&);
    const column_mapping& get_column_mapping() const;
    friend class schema_registry_entry;
    // May be called from different shard
    schema_registry_entry* registry_entry() const noexcept;
    // Null when registry_entry() is null
    schema_registry* registry() const noexcept;
    // Returns true iff this schema version was synced with on current node.
    // Schema version is said to be synced with when its mutations were merged
    // into current node's schema, so that current node's schema is at least as
    // recent as this version.
    bool is_synced() const;
    bool equal_columns(const schema&) const;
    bool wait_for_sync_to_commitlog() const {
        return _raw._wait_for_sync;
    }
public:
    const v3_columns& v3() const {
        return _v3_columns;
    }

    // Make a copy of the schema with reversed clustering order.
    //
    // The reversing is revertible, so that:
    //
    //      s->make_reversed()->make_reversed()->version() == s->version()
    //
    // But note that: `s != s->make_reversed()->make_reversed()` (they are two
    // different C++ objects).
    // The schema's version is also reversed using UUID_gen::negate().
    schema_ptr make_reversed() const;
};

lw_shared_ptr<const schema> make_shared_schema(std::optional<utils::UUID> id, std::string_view ks_name, std::string_view cf_name,
    std::vector<schema::column> partition_key, std::vector<schema::column> clustering_key, std::vector<schema::column> regular_columns,
    std::vector<schema::column> static_columns, data_type regular_column_name_type, sstring comment = "");

bool operator==(const schema&, const schema&);

/**
 * Wrapper for schema_ptr used by functions that expect an engaged view_info field.
 */
class view_ptr final {
    schema_ptr _schema;
public:
    explicit view_ptr(schema_ptr schema) noexcept : _schema(schema) {
        if (schema) {
            assert(_schema->is_view());
        }
    }

    const schema& operator*() const noexcept { return *_schema; }
    const schema* operator->() const noexcept { return _schema.operator->(); }
    const schema* get() const noexcept { return _schema.get(); }

    operator schema_ptr() const noexcept {
        return _schema;
    }

    explicit operator bool() const noexcept {
        return bool(_schema);
    }

    friend std::ostream& operator<<(std::ostream& os, const view_ptr& s);
};

std::ostream& operator<<(std::ostream& os, const view_ptr& view);

utils::UUID generate_legacy_id(const sstring& ks_name, const sstring& cf_name);


// Thrown when attempted to access a schema-dependent object using
// an incompatible version of the schema object.
class schema_mismatch_error : public std::runtime_error {
public:
    schema_mismatch_error(table_schema_version expected, const schema& access);
};

// Throws schema_mismatch_error when a schema-dependent object of "expected" version
// cannot be accessed using "access" schema.
inline void check_schema_version(table_schema_version expected, const schema& access) {
    if (expected != access.version()) {
        throw_with_backtrace<schema_mismatch_error>(expected, access);
    }
}

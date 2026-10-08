/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>
#include <cctype>
#include <unordered_map>

#include <seastar/core/coroutine.hh>

#include <fmt/ranges.h>
#include "keys/compound.hh"
#include "types/tuple.hh"
#include "types/json_utils.hh"
#include "cql3/cql3_type.hh"
#include "db/marshal/type_parser.hh"
#include "schema/schema_builder.hh"
#include "tools/utils.hh"
#include "tools/schema_loader.hh"
#include "db/config.hh"
#include "dht/i_partitioner.hh"
#include "sstables/key.hh"
#include "utils/managed_bytes.hh"
#include "utils/chunked_string.hh"

using namespace seastar;
using namespace tools::utils;

namespace bpo = boost::program_options;

namespace std {
// required by boost::lexical_cast<std::string>(vector<string>), which is in turn used
// by boost::program_option for printing out the default value of an option
static std::ostream& operator<<(std::ostream& os, const std::vector<std::string>& v) {
    return os << fmt::format("{}", v);
}
}

namespace {

const auto app_name = "types";

// The compound options have more human-friendly aliases, either name is accepted.
const std::map<std::string, std::string> compound_option_aliases{
    {"prefix-compound", "clustering-key"},
    {"full-compound", "partition-key"},
    {"legacy-composite", "legacy-partition-key"},
};

// Is the compound option (or its alias) set?
bool has_compound_option(const bpo::variables_map& vm, const std::string& name) {
    return vm.contains(name) || vm.contains(compound_option_aliases.at(name));
}

// A full compound, that is, a partition key.
// Values are serialized either in scylla's in-memory format (see
// keys/compound.hh), or in the legacy composite format, used in sstables (see
// keys/compound_compat.hh).
struct partition_key_type {
    schema_ptr schema;
    bool legacy_composite;

    const compound_type<allow_prefixes::no>& type() const {
        return *schema->partition_key_type();
    }

    partition_key to_partition_key(bytes_view value) const {
        if (!legacy_composite) {
            return partition_key::from_bytes(value);
        }
        // The composite iterator invokes on_internal_error() on malformed
        // input, so validate first. Note that non-compound keys are stored
        // as-is in the legacy format, there is nothing to validate there.
        if (schema->partition_key_size() > 1 && !composite_view(value, true).is_valid()) {
            throw marshal_exception("invalid legacy compound: the sizes of the components don't add up to the size of the value");
        }
        return sstables::key_view(value).to_partition_key(*schema);
    }

    bytes serialize(const partition_key& pk) const {
        if (!legacy_composite) {
            return to_bytes(pk.representation());
        }
        return sstables::key::from_partition_key(*schema, pk).get_bytes();
    }
};

using type_variant = std::variant<
        data_type,
        compound_type<allow_prefixes::yes>,
        partition_key_type>;

using bytes_func = void(*)(type_variant, std::vector<bytes>, const bpo::variables_map& vm);
using string_func = void(*)(type_variant, std::vector<sstring>, const bpo::variables_map& vm);
using operation_func_variant = std::variant<bytes_func, string_func>;

// abstract_type::from_string() is not implemented for collections and
// vectors, it aborts. Tuples (and UDTs) are supported, as long as their
// fields are.
bool can_parse_from_string(const abstract_type& type) {
    const auto& t = type.without_reversed();
    if (t.is_collection() || t.is_vector()) {
        return false;
    }
    if (t.is_tuple()) {
        return std::ranges::all_of(static_cast<const tuple_type_impl&>(t).all_types(), [] (const data_type& field_type) {
            return can_parse_from_string(*field_type);
        });
    }
    return true;
}

managed_bytes from_string(const data_type& type, const sstring& value) {
    if (!can_parse_from_string(*type)) {
        throw std::invalid_argument(fmt::format("error: serializing values of type {} is not supported: collections and vectors (including those nested"
                " in tuples and UDTs) cannot be parsed from their string representation, use --input-format=json", type->cql3_type_name()));
    }
    return type->from_string(value);
}

// Parses the value from its JSON representation (the one used by INSERT JSON,
// see types/json_utils.hh) if json is true, from its string representation
// otherwise.
managed_bytes parse_value(const data_type& type, const sstring& value, bool json) {
    if (json) {
        return managed_bytes(from_json_object(*type, rjson::parse(value)));
    }
    return from_string(type, value);
}

// Converts a type name in CQL syntax (e.g. map<int, text>) to the equivalent
// Cassandra type class name (e.g. MapType(Int32Type, UTF8Type)), which can be
// parsed by db::marshal::type_parser.
// Names which are not CQL type names (e.g. Int32Type) are passed through as-is,
// so Cassandra type class names work too, they can even be mixed with CQL names,
// e.g. ReversedType(timeuuid).
class cql_type_name_converter {
    std::string_view _str; // the whole type name, for error messages
    std::string_view _remaining; // the yet unparsed suffix of _str

private:
    static const std::unordered_map<sstring, sstring>& native_types() {
        static thread_local const auto types = [] {
            std::unordered_map<sstring, sstring> types;
            for (const auto& t : cql3::cql3_type::values()) {
                types.emplace(t.to_string(), t.get_type()->name());
            }
            types.emplace("varchar", utf8_type->name());
            return types;
        }();
        return types;
    }

    static const std::unordered_map<sstring, sstring>& parametric_types() {
        static thread_local const std::unordered_map<sstring, sstring> types{
            {"frozen", "FrozenType"},
            {"list", "ListType"},
            {"set", "SetType"},
            {"map", "MapType"},
            {"tuple", "TupleType"},
            {"vector", "VectorType"},
        };
        return types;
    }

    [[noreturn]] void error(std::string_view msg) const {
        throw std::invalid_argument(fmt::format("error: failed to parse type '{}' at position {}: {}", _str, _str.size() - _remaining.size(), msg));
    }

    void skip_blank() {
        while (!_remaining.empty() && std::isspace(static_cast<unsigned char>(_remaining.front()))) {
            _remaining.remove_prefix(1);
        }
    }

    bool consume(char c) {
        skip_blank();
        if (!_remaining.empty() && _remaining.front() == c) {
            _remaining.remove_prefix(1);
            return true;
        }
        return false;
    }

    std::string_view read_identifier() {
        skip_blank();
        const auto it = std::ranges::find_if_not(_remaining, [] (unsigned char c) {
            return std::isalnum(c) || c == '_' || c == '.' || c == ':';
        });
        const auto identifier = _remaining.substr(0, it - _remaining.begin());
        _remaining.remove_prefix(identifier.size());
        return identifier;
    }

    std::vector<sstring> convert_parameters(char closing_bracket) {
        std::vector<sstring> params;
        do {
            params.push_back(convert_type());
        } while (consume(','));
        if (!consume(closing_bracket)) {
            error(fmt::format("expected '{}'", closing_bracket));
        }
        return params;
    }

    sstring convert_type() {
        const auto name = read_identifier();
        if (name.empty()) {
            error("expected type name");
        }
        if (consume('(')) { // Cassandra type with parameters
            auto params = convert_parameters(')');
            return seastar::format("{}({})", name, fmt::join(params, ", "));
        }
        sstring lower_name(name);
        std::ranges::transform(lower_name, lower_name.begin(), [] (unsigned char c) { return std::tolower(c); });
        if (consume('<')) {
            const auto it = parametric_types().find(lower_name);
            if (it == parametric_types().end()) {
                error(fmt::format("unknown parametric type {}", name));
            }
            auto params = convert_parameters('>');
            return seastar::format("{}({})", it->second, fmt::join(params, ", "));
        }
        if (const auto it = native_types().find(lower_name); it != native_types().end()) {
            return it->second;
        }
        return sstring(name);
    }

public:
    explicit cql_type_name_converter(std::string_view str) : _str(str), _remaining(str) { }

    sstring convert() {
        auto type_name = convert_type();
        skip_blank();
        if (!_remaining.empty()) {
            error("unexpected trailing characters");
        }
        return type_name;
    }
};

struct serializing_visitor {
    const std::vector<sstring>& values;
    bool json; // values are in JSON format

    managed_bytes operator()(const data_type& type) {
        if (values.size() != 1) {
            throw std::runtime_error(fmt::format("serialize_handler(): expected 1 value for non-compound type, got {}", values.size()));
        }
        return parse_value(type, values.front(), json);
    }
    template <allow_prefixes AllowPrefixes>
    managed_bytes operator()(const compound_type<AllowPrefixes>& type) {
        if constexpr (AllowPrefixes == allow_prefixes::yes) {
            if (values.size() > type.types().size()) {
                throw std::runtime_error(fmt::format("serialize_handler(): expected at most {} (number of subtypes) values for prefix compound type, got {}", type.types().size(), values.size()));
            }
        } else {
            if (values.size() != type.types().size()) {
                throw std::runtime_error(fmt::format("serialize_handler(): expected {} (number of subtypes) values for non-prefix compound type, got {}", type.types().size(), values.size()));
            }
        }
        std::vector<bytes> serialized_values;
        serialized_values.reserve(values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            serialized_values.push_back(to_bytes(parse_value(type.types().at(i), values.at(i), json)));
        }
        return type.serialize_value(serialized_values);
    }
    managed_bytes operator()(const partition_key_type& type) {
        const auto pk = partition_key::from_bytes((*this)(type.type()));
        return managed_bytes(type.serialize(pk));
    }

    managed_bytes operator()(const type_variant& type) {
        return std::visit(*this, type);
    }
};

// The format the values are provided in, on the command line.
enum class input_format {
    hex, // serialized, hex encoded
    text, // unserialized, the string representation of the values
    json, // unserialized, the JSON representation of the values
};

const std::map<input_format, std::string_view> input_format_names{
    {input_format::hex, "hex"},
    {input_format::text, "text"},
    {input_format::json, "json"},
};

// Returns the format the values are provided in: the one selected with
// --input-format, or default_format if none was selected.
input_format get_input_format(const bpo::variables_map& vm, std::string_view action, input_format default_format,
        std::initializer_list<input_format> supported_formats) {
    if (!vm.contains("input-format")) {
        return default_format;
    }
    const std::string_view name = vm["input-format"].as<sstring>();
    const auto it = std::ranges::find(input_format_names, name, [] (const auto& format_and_name) { return format_and_name.second; });
    if (it == input_format_names.end()) {
        // Boost program options doesn't support '=' after short options, -f=text is parsed as "=text".
        const auto hint = name.starts_with("=") ? ", note that -f=<format> is not supported, use -f <format> or --input-format=<format>" : "";
        throw std::invalid_argument(fmt::format("error: invalid input format '{}', expected one of: {}{}", name,
                fmt::join(input_format_names | std::views::values, ", "), hint));
    }
    if (!std::ranges::contains(supported_formats, it->first)) {
        throw std::invalid_argument(fmt::format("error: the {} action doesn't support the {} input format, supported input formats: {}", action, name,
                fmt::join(supported_formats | std::views::transform([] (input_format format) { return input_format_names.at(format); }), ", ")));
    }
    return it->first;
}

void serialize_handler(type_variant type, std::vector<sstring> values, const bpo::variables_map& vm) {
    const auto format = get_input_format(vm, "serialize", input_format::text, {input_format::text, input_format::json});
    fmt::print("{}\n", managed_bytes_view(serializing_visitor{values, format == input_format::json}(type)));
}

// Returns the serialized values to operate on.
// The values are either serialized (hex encoded), or unserialized (text or
// json), which are serialized here. Unserialized values are split into
// unserialized_count groups of equal size, each group making up one value
// (compound values are made up of multiple components).
std::vector<bytes> get_serialized_values(const type_variant& type, const std::vector<sstring>& values, const bpo::variables_map& vm,
        std::string_view action, size_t unserialized_count) {
    const auto format = get_input_format(vm, action, input_format::hex, {input_format::hex, input_format::text, input_format::json});
    if (format == input_format::hex) {
        return values | std::views::transform([] (const sstring& hex_str) { return from_hex(hex_str); }) | std::ranges::to<std::vector>();
    }
    if (values.size() % unserialized_count) {
        throw std::invalid_argument(fmt::format("error: expected the number of unserialized values ({}) to be divisible by {}, the number of values to operate on",
                values.size(), unserialized_count));
    }
    std::vector<bytes> serialized_values;
    for (auto&& group : values | std::views::chunk(values.size() / unserialized_count)) {
        const auto group_values = group | std::ranges::to<std::vector<sstring>>();
        serialized_values.push_back(to_bytes(serializing_visitor{group_values, format == input_format::json}(type)));
    }
    return serialized_values;
}

sstring to_printable_string(const data_type& type, bytes_view value) {
    return type->to_string(value);
}

template <allow_prefixes AllowPrefixes>
sstring to_printable_string(const compound_type<AllowPrefixes>& type, bytes_view value) {
    std::vector<sstring> printable_values;
    printable_values.reserve(type.types().size());

    const auto types = type.types();
    const auto values = type.deserialize_value(value);

    for (size_t i = 0; i != values.size(); ++i) {
        printable_values.emplace_back(types.at(i)->to_string(values.at(i)));
    }
    return seastar::format("({})", fmt::join(printable_values, ", "));
}

sstring to_printable_string(const partition_key_type& type, bytes_view value) {
    const auto pk = type.to_partition_key(value);
    return to_printable_string(type.type(), to_bytes(pk.representation()));
}

struct printing_visitor {
    bytes_view value;

    sstring operator()(const data_type& type) {
        return to_printable_string(type, value);
    }
    template <allow_prefixes AllowPrefixes>
    sstring operator()(const compound_type<AllowPrefixes>& type) {
        return to_printable_string(type, value);
    }
    sstring operator()(const partition_key_type& type) {
        return to_printable_string(type, value);
    }
};

sstring to_printable_string(const type_variant& type, bytes_view value) {
    return std::visit(printing_visitor{value}, type);
}

void deserialize_handler(type_variant type, std::vector<bytes> values, const bpo::variables_map& vm) {
    for (const auto& value : values) {
        fmt::print("{}\n", to_printable_string(type, value));
    }
}

void print_compare_result(std::string_view lhs, std::string_view rhs, std::strong_ordering res) {
    std::string_view res_str;

    if (res == 0) {
        res_str = "==";
    } else if (res < 0) {
        res_str = "<";
    } else {
        res_str = ">";
    }
    fmt::print("{} {} {}\n", lhs, res_str, rhs);
}

void compare_handler(type_variant type, std::vector<sstring> unparsed_values, const bpo::variables_map& vm) {
    const auto values = get_serialized_values(type, unparsed_values, vm, "compare", 2);
    if (values.size() != 2) {
        throw std::runtime_error(fmt::format("compare_handler(): expected 2 values, got {}", values.size()));
    }

    struct {
        bytes_view lhs, rhs;

        std::strong_ordering operator()(const data_type& type) {
            return type->compare(lhs, rhs);
        }
        std::strong_ordering operator()(const compound_type<allow_prefixes::yes>& type) {
            return type.compare(lhs, rhs);
        }
        std::strong_ordering operator()(const partition_key_type& type) {
            return type.type().compare(type.to_partition_key(lhs).representation(), type.to_partition_key(rhs).representation());
        }
    } compare_visitor{values[0], values[1]};

    print_compare_result(to_printable_string(type, values[0]), to_printable_string(type, values[1]), std::visit(compare_visitor, type));
}

void validate_handler(type_variant type, std::vector<bytes> values, const bpo::variables_map& vm) {
    struct validate_visitor {
        bytes_view value;

        void operator()(const data_type& type) {
            type->validate(value);
        }
        void operator()(const compound_type<allow_prefixes::yes>& type) {
            type.validate(value);
        }
        void operator()(const partition_key_type& type) {
            type.type().validate(type.to_partition_key(value).representation());
        }
    };

    for (const auto& value : values) {
        std::exception_ptr ex;
        try {
            std::visit(validate_visitor{value}, type);
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            fmt::print("{}: INVALID - {}\n", to_hex(value), ex);
        } else {
            fmt::print("{}: VALID - {}\n", to_hex(value), to_printable_string(type, value));
        }
    }
}

schema_ptr build_dummy_partition_key_schema(const std::vector<data_type>& types) {
    schema_builder builder(this_smp_shard_count(), "ks", "dummy");
    unsigned i = 0;
    for (const auto& t : types) {
        const auto col_name = format("pk{}", i++);
        builder.with_column(bytes(to_bytes_view(col_name)), t, column_kind::partition_key);
    }
    builder.with_column("v", utf8_type, column_kind::regular_column);

    return builder.build();
}

const partition_key_type& get_partition_key_type(const type_variant& type, std::string_view action) {
    if (const auto* pk_type = std::get_if<partition_key_type>(&type)) {
        return *pk_type;
    }
    throw std::invalid_argument(fmt::format("{} action requires --full-compound (--partition-key) or --legacy-composite (--legacy-partition-key) input", action));
}

void ring_order_compare_handler(type_variant type, std::vector<sstring> unparsed_values, const bpo::variables_map& vm) {
    const auto& pk_type = get_partition_key_type(type, "ring-order-compare");
    const auto values = get_serialized_values(type, unparsed_values, vm, "ring-order-compare", 2);
    if (values.size() != 2) {
        throw std::runtime_error(fmt::format("ring_order_compare_handler(): expected 2 values, got {}", values.size()));
    }

    const auto& s = *pk_type.schema;
    const auto lhs_dk = dht::decorate_key(s, pk_type.to_partition_key(values[0]));
    const auto rhs_dk = dht::decorate_key(s, pk_type.to_partition_key(values[1]));

    // Print the tokens too, they determine the order (in most cases).
    const auto to_printable_ring_position = [&type] (const dht::decorated_key& dk, bytes_view value) {
        return seastar::format("{{token: {}, key: {}}}", dk.token(), to_printable_string(type, value));
    };
    print_compare_result(to_printable_ring_position(lhs_dk, values[0]), to_printable_ring_position(rhs_dk, values[1]), lhs_dk.tri_compare(s, rhs_dk));
}

void tokenof_handler(type_variant type, std::vector<sstring> values, const bpo::variables_map& vm) {
    const auto& pk_type = get_partition_key_type(type, "tokenof");

    for (const auto& value : get_serialized_values(type, values, vm, "tokenof", 1)) {
        const auto dk = dht::decorate_key(*pk_type.schema, pk_type.to_partition_key(value));
        fmt::print("{}: {}\n", to_printable_string(pk_type, value), dk.token());
    }
}

void shardof_handler(type_variant type, std::vector<sstring> values, const bpo::variables_map& vm) {
    const auto& pk_type = get_partition_key_type(type, "shardof");

    if (!vm.count("shards")) {
        throw std::invalid_argument("error: missing mandatory argument --shards");
    }

    for (const auto& value : get_serialized_values(type, values, vm, "shardof", 1)) {
        const auto dk = dht::decorate_key(*pk_type.schema, pk_type.to_partition_key(value));
        const auto shard = dht::shard_of(vm["shards"].as<unsigned>(), vm["ignore-msb-bits"].as<unsigned>(), dk.token());
        fmt::print("{}: token: {}, shard: {}\n", to_printable_string(pk_type, value), dk.token(), shard);
    }
}

type_variant type_from_schema(const db::config& dbcfg, const bpo::variables_map& app_config) {
    if (app_config.contains("type")) {
        throw std::invalid_argument("error: --type and --schema-file are mutually exclusive");
    }
    const auto schema = tools::load_one_schema_from_file(dbcfg, app_config["schema-file"].as<std::string>()).get();

    if (app_config.contains("column")) {
        if (has_compound_option(app_config, "prefix-compound") || has_compound_option(app_config, "full-compound") || has_compound_option(app_config, "legacy-composite")) {
            throw std::invalid_argument("error: --column cannot be used together with --prefix-compound (--clustering-key), --full-compound (--partition-key)"
                    " or --legacy-composite (--legacy-partition-key)");
        }
        const auto& column_name = app_config["column"].as<std::string>();
        const auto* cdef = schema->get_column_definition(to_bytes(column_name));
        if (!cdef) {
            throw std::invalid_argument(fmt::format("error: column {} not found in table {}.{}", column_name, schema->ks_name(), schema->cf_name()));
        }
        return cdef->type;
    }

    if (has_compound_option(app_config, "prefix-compound")) {
        return compound_type<allow_prefixes::yes>(std::vector<data_type>(schema->clustering_key_prefix_type()->types()));
    } else if (has_compound_option(app_config, "full-compound")) {
        return partition_key_type{schema, false};
    } else if (has_compound_option(app_config, "legacy-composite")) {
        return partition_key_type{schema, true};
    }
    throw std::invalid_argument("error: --schema-file requires one of: --column, --prefix-compound (--clustering-key), --full-compound (--partition-key)"
            " or --legacy-composite (--legacy-partition-key)");
}

const std::vector<operation_option> global_options{
    typed_option<std::vector<std::string>>("type,t", "the type of the values, all values must be of the same type;"
            " types can be specified either with their CQL name (e.g. map<int, text>) or with their cassandra type class name (e.g. MapType(Int32Type, UTF8Type));"
            " when values are compounds, multiple types can be specified, one for each type making up the compound, "
            "note that the order of the types on the command line will be their order in the compound too"),
    typed_option<std::string>("schema-file", "path to a file containing the schema of the table, which the values belong to (CREATE TABLE statement, possibly preceded"
            " by CREATE KEYSPACE and CREATE TYPE statements); alternative to --type, use --column to select the column the values belong to,"
            " or --prefix-compound (--clustering-key), --full-compound (--partition-key) or --legacy-composite (--legacy-partition-key)"
            " for the clustering key or partition key respectively"),
    typed_option<std::string>("column", "the name of the column the values belong to, the column is looked up in the schema loaded with --schema-file"),
    typed_option<>("prefix-compound", "values are prefixable compounds (e.g. clustering key), composed of multiple values of possibly different types;"
            " alias: --clustering-key"),
    typed_option<>("full-compound", "values are full compounds (e.g. partition key), composed of multiple values of possibly different types;"
            " alias: --partition-key"),
    typed_option<>("legacy-composite", "values are full compounds (e.g. partition key), serialized in the legacy composite format, used in sstables,"
            " instead of scylla's in-memory format; alias: --legacy-partition-key"),
    typed_option<>("clustering-key", "alias for --prefix-compound"),
    typed_option<>("partition-key", "alias for --full-compound"),
    typed_option<>("legacy-partition-key", "alias for --legacy-composite"),
    typed_option<sstring>("input-format,f", "the format the values are provided in: hex - serialized, hex encoded (the default, except for the serialize action),"
            " text - unserialized, the human-readable string representation of the values (the default for the serialize action),"
            " json - unserialized, the JSON representation of the values, the same as accepted by INSERT JSON, allows serializing"
            " collections and vectors, which have no string representation;"
            " the supported input formats depend on the action, see the help of the action; for compare and ring-order-compare,"
            " the first half of the unserialized values make up the first compared value, the second half the second one;"
            " for tokenof and shardof, all unserialized values make up a single partition key"),
    typed_option<unsigned>("shards", "number of shards (only relevant for shardof action)"),
    typed_option<unsigned>("ignore-msb-bits", 12u, "number of the most significant bits of the token to ignore when calculating the shard"
            " (only relevant for shardof action)"),
};

const std::vector<operation_option> global_positional_options{
    typed_option<std::vector<std::string>>("value", "value(s) to process, can also be provided as positional arguments", -1),
};

const std::map<operation, operation_func_variant> operations_with_func = {
    {{"serialize", "serialize the value and print it in hex encoded form",
R"(
Serialize the value and print it in a hex encoded form.

Arguments:
* 1 value for regular types
* N values for non-prefix compound types (one value for each component)
* <N values for prefix compound types (one value for each present component)

To avoid boost::program_options trying to interpret values with special
characters like '-' as options, separate values from the rest of the arguments
with '--'.

Values of collection and vector types (including tuples and UDTs, which have
fields of such types) have no string representation, they can only be
serialized from their JSON representation, see --input-format=json.

Input formats: text (default), json.

Examples:

$ scylla types serialize -t Int32Type -- -1286905132
b34b62d4

$ scylla types serialize --prefix-compound -t TimeUUIDType -t Int32Type -- d0081989-6f6b-11ea-0000-0000001c571b 16
0010d00819896f6b11ea00000000001c571b000400000010

$ scylla types serialize --prefix-compound -t TimeUUIDType -t Int32Type -- d0081989-6f6b-11ea-0000-0000001c571b
0010d00819896f6b11ea00000000001c571b

$ scylla types serialize -f json -t 'map<int, text>' -- '{"1": "a"}'
0000000100000004000000010000000161
)"}, serialize_handler},
    {{"deserialize", "deserialize the value(s) and print them in a human readable form",
R"(
Deserialize the value(s) and print them in a human-readable form.

Arguments: 1 or more serialized values.

Input formats: hex (default).

Examples:

$ scylla types deserialize -t Int32Type b34b62d4
-1286905132

$ scylla types deserialize --prefix-compound -t TimeUUIDType -t Int32Type 0010d00819896f6b11ea00000000001c571b000400000010
(d0081989-6f6b-11ea-0000-0000001c571b, 16)
)"}, deserialize_handler},
    {{"compare", "compare two values",
R"(
Compare two values and print the result.

Arguments: 2 values. With unserialized input (text or json), the first half of
the values make up the first compared value, the second half the second one.

Input formats: hex (default), text, json.

Examples:

$ scylla types compare -t 'ReversedType(TimeUUIDType)' b34b62d46a8d11ea0000005000237906 d00819896f6b11ea00000000001c571b
b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b
)"}, compare_handler},
    {{"ring-order-compare", "compare two partition keys in ring order",
R"(
Compare two partition keys in ring order and print the result.
Partition keys are ordered by their token first and only by the keys themselves
on token collision. Same as in scylla, keys with colliding tokens are compared
byte-wise, in their legacy (sstable) format.
Only supports --full-compound (or its alias --partition-key) and
--legacy-composite (or its alias --legacy-partition-key).

Arguments: 2 values. With unserialized input (text or json), the first half of
the values make up the first compared value, the second half the second one.

Input formats: hex (default), text, json.

Examples:

$ scylla types ring-order-compare --full-compound -t int -t text 0004000000010003616263 0004000000020003616263
{token: 8771735466527499816, key: (1, abc)} > {token: -3504390351319460166, key: (2, abc)}
)"}, ring_order_compare_handler},
    {{"validate", "validate the value(s)",
R"(
Check that the value(s) are valid for the type according to the requirements of
the type.

Arguments: 1 or more serialized values.

Input formats: hex (default).

Examples:

$  scylla types validate -t Int32Type b34b62d4
b34b62d4: VALID - -1286905132
)"}, validate_handler},
    {{"tokenof", "tokenof (calculate the token of) the partition-key",
R"(
Decorate the key, that is calculate its token.
Only supports --full-compound (or its alias --partition-key) and
--legacy-composite (or its alias --legacy-partition-key).

Arguments: 1 or more values. With unserialized input (text or json), all values
make up a single partition key.

Input formats: hex (default), text, json.

Examples:

$ scylla types tokenof --full-compound -t UTF8Type -t SimpleDateType -t UUIDType 000d66696c655f696e7374616e63650004800049190010c61a3321045941c38e5675255feb0196
(file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): -5043005771368701888

$ scylla types tokenof --partition-key -t text -t date -t uuid -f text -- file_instance 2021-03-27 c61a3321-0459-41c3-8e56-75255feb0196
(file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): -5043005771368701888
)"}, tokenof_handler},
    {{"shardof", "calculate which shard the partition-key belongs to",
R"(
Decorate the key and calculate which shard its token belongs to.
Only supports --full-compound (or its alias --partition-key) and
--legacy-composite (or its alias --legacy-partition-key).
Use --shards and --ignore-msb-bits to specify sharding parameters.

Arguments: 1 or more values. With unserialized input (text or json), all values
make up a single partition key.

Input formats: hex (default), text, json.

Examples:

$ scylla types shardof --full-compound -t UTF8Type -t SimpleDateType -t UUIDType --shards=7 000d66696c655f696e7374616e63650004800049190010c61a3321045941c38e5675255feb0196
(file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): token: -5043005771368701888, shard: 1
)"}, shardof_handler},
};

}

namespace tools {

int scylla_types_main(int argc, char** argv) {
    constexpr auto description_template =
R"(scylla-{} - a command-line tool to examine values belonging to scylla types.

Usage: scylla {} {{action}} [--option1] [--option2] ... {{hex_value1}} [{{hex_value2}}] ...

Allows examining raw values obtained from e.g. sstables, logs or coredumps and
executing various actions on them. Values should be provided in hex form,
without a leading 0x prefix, e.g. 00783562. For scylla-types to be able to
examine the values, their type has to be provided. Types can be provided by
their CQL names, e.g. int or map<int, text>, or by their cassandra class
names, e.g. org.apache.cassandra.db.marshal.Int32Type for the int32_type. The
org.apache.cassandra.db.marshal. prefix can be omitted.
See https://github.com/scylladb/scylla/blob/master/docs/dev/cql3-type-mapping.md
for a mapping of cql3 types to Cassandra type class names.
Compound cassandra types specify their subtypes inside () separated by comma,
e.g.: MapType(Int32Type, BytesType). CQL and cassandra names can be mixed, e.g.
ReversedType(timeuuid). All provided values have to share the same type.
scylla-types executes so called actions on the provided values. Each action has
a required number of arguments. The supported actions are:
{}

For more information about individual actions, see their specific help:

$ scylla types {{action}} --help
)";

    const auto operations = operations_with_func | std::views::keys | std::ranges::to<std::vector>();
    tool_app_template::config app_cfg{
        .name = app_name,
        .description = seastar::format(description_template, app_name, app_name, fmt::join(operations | std::views::transform(
                [] (const operation& op) { return fmt::format("* {} - {}", op.name(), op.summary()); } ), "\n")),
        .operations = std::move(operations),
        .global_options = &global_options,
        .global_positional_options = &global_positional_options,
    };
    tool_app_template app(std::move(app_cfg));

    return app.run_async(argc, argv, [] (const operation& op, const boost::program_options::variables_map& app_config) {
        // Kept alive alongside the schema loaded with it.
        std::optional<db::config> dbcfg;

        type_variant type = [&app_config, &dbcfg] () -> type_variant {
            if (app_config.contains("schema-file")) {
                return type_from_schema(dbcfg.emplace(), app_config);
            }
            if (app_config.contains("column")) {
                throw std::invalid_argument("error: --column requires --schema-file");
            }
            if (!app_config.contains("type")) {
                throw std::invalid_argument("error: missing required option '--type' (or '--schema-file')");
            }
            auto types = app_config["type"].as<std::vector<std::string>>()
                    | std::views::transform([] (const std::string_view type_name) { return cql_type_name_converter(type_name).convert(); })
                    | std::views::transform([] (const sstring& type_name) { return db::marshal::type_parser::parse(type_name); })
                    | std::ranges::to<std::vector<data_type>>();
            if (has_compound_option(app_config, "prefix-compound")) {
                return compound_type<allow_prefixes::yes>(std::move(types));
            } else if (has_compound_option(app_config, "full-compound")) {
                return partition_key_type{build_dummy_partition_key_schema(types), false};
            } else if (has_compound_option(app_config, "legacy-composite")) {
                return partition_key_type{build_dummy_partition_key_schema(types), true};
            } else { // non-compound type
                if (types.size() != 1) {
                    throw std::invalid_argument(fmt::format("error: expected a single '--type' argument, got  {}", types.size()));
                }
                return std::move(types.front());
            }
        }();

        if (!app_config.contains("value")) {
            throw std::invalid_argument("error: no values specified");
        }

        const auto& handler = operations_with_func.at(op);
        switch (handler.index()) {
            case 0:
                {
                    get_input_format(app_config, op.name(), input_format::hex, {input_format::hex});
                    auto from_hex_func = [] (const std::string& hex_str) { return from_hex(hex_str); };
                    auto values = app_config["value"].as<std::vector<std::string>>() | std::views::transform(from_hex_func) | std::ranges::to<std::vector>();
                    std::get<bytes_func>(handler)(std::move(type), std::move(values), app_config);
                }
                break;
            case 1:
                {
                    auto values = app_config["value"].as<std::vector<std::string>>() | std::ranges::to<std::vector<sstring>>();
                    std::get<string_func>(handler)(std::move(type), std::move(values), app_config);
                }
                break;
        }

        return 0;
    });
}

} // namespace tools

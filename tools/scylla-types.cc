/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/map.hpp>
#include <seastar/core/coroutine.hh>

#include <fmt/ranges.h>
#include "compound.hh"
#include "db/marshal/type_parser.hh"
#include "schema/schema_builder.hh"
#include "tools/utils.hh"
#include "dht/i_partitioner.hh"
#include "utils/managed_bytes.hh"

using namespace seastar;
using namespace tools::utils;

namespace bpo = boost::program_options;

namespace std {
// required by boost::lexical_cast<std::string>(vector<string>), which is in turn used
// by boost::program_option for printing out the default value of an option
static std::ostream& operator<<(std::ostream& os, const std::vector<sstring>& v) {
    return os << fmt::format("{}", v);
}
}

namespace {

const auto app_name = "types";

using type_variant = std::variant<
        data_type,
        compound_type<allow_prefixes::yes>,
        compound_type<allow_prefixes::no>>;

using bytes_func = void(*)(type_variant, std::vector<bytes>, const bpo::variables_map& vm);
using string_func = void(*)(type_variant, std::vector<sstring>, const bpo::variables_map& vm);
using operation_func_variant = std::variant<bytes_func, string_func>;

struct serializing_visitor {
    const std::vector<sstring>& values;

    managed_bytes operator()(const data_type& type) {
        if (values.size() != 1) {
            throw std::runtime_error(fmt::format("serialize_handler(): expected 1 value for non-compound type, got {}", values.size()));
        }
        return managed_bytes(type->from_string(values.front()));
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
            serialized_values.push_back(type.types().at(i)->from_string(values.at(i)));
        }
        return type.serialize_value(serialized_values);
    }

    managed_bytes operator()(const type_variant& type) {
        return std::visit(*this, type);
    }
};

void serialize_handler(type_variant type, std::vector<sstring> values, const bpo::variables_map& vm) {
    fmt::print("{}\n", managed_bytes_view(serializing_visitor{values}(type)));
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
    return format("({})", boost::algorithm::join(printable_values, ", "));
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
};

sstring to_printable_string(const type_variant& type, bytes_view value) {
    return std::visit(printing_visitor{value}, type);
}

void deserialize_handler(type_variant type, std::vector<bytes> values, const bpo::variables_map& vm) {
    for (const auto& value : values) {
        fmt::print("{}\n", to_printable_string(type, value));
    }
}

void compare_handler(type_variant type, std::vector<bytes> values, const bpo::variables_map& vm) {
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
        std::strong_ordering operator()(const compound_type<allow_prefixes::no>& type) {
            return type.compare(lhs, rhs);
        }
    } compare_visitor{values[0], values[1]};

    const auto res = std::visit(compare_visitor, type);
    sstring_view res_str;

    if (res == 0) {
        res_str = "==";
    } else if (res < 0) {
        res_str = "<";
    } else {
        res_str = ">";
    }
    fmt::print("{} {} {}\n", to_printable_string(type, values[0]), res_str, to_printable_string(type, values[1]));
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
        void operator()(const compound_type<allow_prefixes::no>& type) {
            type.validate(value);
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

schema_ptr build_dummy_partition_key_schema(const compound_type<allow_prefixes::no>& type) {
    schema_builder builder("ks", "dummy");
    unsigned i = 0;
    for (const auto& t : type.types()) {
        const auto col_name = format("pk{}", i);
        builder.with_column(bytes(to_bytes_view(col_name)), t, column_kind::partition_key);
    }
    builder.with_column("v", utf8_type, column_kind::regular_column);

    return builder.build();
}

void tokenof_handler(type_variant type, std::vector<bytes> values, const bpo::variables_map& vm) {
    struct tokenof_visitor {
        bytes_view value;

        void operator()(const data_type& type) {
            throw std::invalid_argument("tokenof action requires full-compound input");
        }
        void operator()(const compound_type<allow_prefixes::yes>& type) {
            throw std::invalid_argument("tokenof action requires full-compound input");
        }
        void operator()(const compound_type<allow_prefixes::no>& type) {
            auto s = build_dummy_partition_key_schema(type);
            auto pk = partition_key::from_bytes(value);
            auto dk = dht::decorate_key(*s, pk);
            fmt::print("{}: {}\n", to_printable_string(type, value), dk.token());
        }
    };

    for (const auto& value : values) {
        std::visit(tokenof_visitor{value}, type);
    }
}

void shardof_handler(type_variant type, std::vector<bytes> values, const bpo::variables_map& vm) {
    struct shardof_visitor {
        bytes_view value;
        const bpo::variables_map& vm;

        void operator()(const data_type& type) {
            throw std::invalid_argument("shardof action requires full-compound input");
        }
        void operator()(const compound_type<allow_prefixes::yes>& type) {
            throw std::invalid_argument("shardof action requires full-compound input");
        }
        void operator()(const compound_type<allow_prefixes::no>& type) {
            auto s = build_dummy_partition_key_schema(type);
            auto pk = partition_key::from_bytes(value);
            auto dk = dht::decorate_key(*s, pk);
            auto shard = dht::shard_of(vm["shards"].as<unsigned>(), vm["ignore-msb-bits"].as<unsigned>(), dk.token());
            fmt::print("{}: token: {}, shard: {}\n", to_printable_string(type, value), dk.token(), shard);
        }
    };

    if (!vm.count("shards")) {
        throw std::invalid_argument("error: missing mandatory argument --shards");
    }

    for (const auto& value : values) {
        std::visit(shardof_visitor{value, vm}, type);
    }
}

const std::vector<operation_option> global_options{
    typed_option<std::vector<sstring>>("type,t", "the type of the values, all values must be of the same type;"
            " when values are compounds, multiple types can be specified, one for each type making up the compound, "
            "note that the order of the types on the command line will be their order in the compound too"),
    typed_option<>("prefix-compound", "values are prefixable compounds (e.g. clustering key), composed of multiple values of possibly different types"),
    typed_option<>("full-compound", "values are full compounds (e.g. partition key), composed of multiple values of possibly different types"),
    typed_option<unsigned>("shards", "number of shards (only relevant for shardof action)"),
    typed_option<unsigned>("ignore-msb-bits", 12u, "number of shards (only relevant for shardof action)"),
};

const std::vector<operation_option> global_positional_options{
    typed_option<std::vector<sstring>>("value", "value(s) to process, can also be provided as positional arguments", -1),
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

Only atomic, regular types are supported for now, collections, UDT and tuples are
not supported, not even in frozen form.

Examples:

$ scylla types serialize -t Int32Type -- -1286905132
b34b62d4

$ scylla types serialize --prefix-compound -t TimeUUIDType -t Int32Type -- d0081989-6f6b-11ea-0000-0000001c571b 16
0010d00819896f6b11ea00000000001c571b000400000010

$ scylla types serialize --prefix-compound -t TimeUUIDType -t Int32Type -- d0081989-6f6b-11ea-0000-0000001c571b
0010d00819896f6b11ea00000000001c571b
)"}, serialize_handler},
    {{"deserialize", "deserialize the value(s) and print them in a human readable form",
R"(
Deserialize the value(s) and print them in a human-readable form.

Arguments: 1 or more serialized values.

Examples:

$ scylla types deserialize -t Int32Type b34b62d4
-1286905132

$ scylla types deserialize --prefix-compound -t TimeUUIDType -t Int32Type 0010d00819896f6b11ea00000000001c571b000400000010
(d0081989-6f6b-11ea-0000-0000001c571b, 16)
)"}, deserialize_handler},
    {{"compare", "compare two values",
R"(
Compare two values and print the result.

Arguments: 2 serialized values.

Examples:

$ scylla types compare -t 'ReversedType(TimeUUIDType)' b34b62d46a8d11ea0000005000237906 d00819896f6b11ea00000000001c571b
b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b
)"}, compare_handler},
    {{"validate", "validate the value(s)",
R"(
Check that the value(s) are valid for the type according to the requirements of
the type.

Arguments: 1 or more serialized values.

Examples:

$  scylla types validate -t Int32Type b34b62d4
b34b62d4: VALID - -1286905132
)"}, validate_handler},
    {{"tokenof", "tokenof (calculate the token of) the partition-key",
R"(
Decorate the key, that is calculate its token.
Only supports --full-compound.

Arguments: 1 or more serialized values.

Examples:

$ scylla types tokenof --full-compound -t UTF8Type -t SimpleDateType -t UUIDType 000d66696c655f696e7374616e63650004800049190010c61a3321045941c38e5675255feb0196
(file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): -5043005771368701888
)"}, tokenof_handler},
    {{"shardof", "calculate which shard the partition-key belongs to",
R"(
Decorate the key and calculate which shard its token belongs to.
Only supports --full-compound. Use --shards and --ignore-msb-bits to specify
sharding parameters.

Arguments: 1 or more serialized values.

Examples:

$ scylla types shardof --full-compound -t UTF8Type -t SimpleDateType -t UUIDType --shards=7 000d66696c655f696e7374616e63650004800049190010c61a3321045941c38e5675255feb0196
(file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): token: -5043005771368701888, shard: 1
)"}, shardof_handler},
};

}

namespace tools {

int scylla_types_main(int argc, char** argv) {
    auto description_template =
R"(scylla-{} - a command-line tool to examine values belonging to scylla types.

Usage: scylla {} {{action}} [--option1] [--option2] ... {{hex_value1}} [{{hex_value2}}] ...

Allows examining raw values obtained from e.g. sstables, logs or coredumps and
executing various actions on them. Values should be provided in hex form,
without a leading 0x prefix, e.g. 00783562. For scylla-types to be able to
examine the values, their type has to be provided. Types should be provided by
their cassandra class names, e.g. org.apache.cassandra.db.marshal.Int32Type for
the int32_type. The org.apache.cassandra.db.marshal. prefix can be omitted.
See https://github.com/scylladb/scylla/blob/master/docs/dev/cql3-type-mapping.md
for a mapping of cql3 types to Cassandra type class names.
Compound types specify their subtypes inside () separated by comma, e.g.:
MapType(Int32Type, BytesType). All provided values have to share the same type.
scylla-types executes so called actions on the provided values. Each action has
a required number of arguments. The supported actions are:
{}

For more information about individual actions, see their specific help:

$ scylla types {{action}} --help
)";

    const auto operations = boost::copy_range<std::vector<operation>>(operations_with_func | boost::adaptors::map_keys);
    tool_app_template::config app_cfg{
        .name = app_name,
        .description = format(description_template, app_name, app_name, boost::algorithm::join(operations | boost::adaptors::transformed(
                [] (const operation& op) { return format("* {} - {}", op.name(), op.summary()); } ), "\n")),
        .operations = std::move(operations),
        .global_options = &global_options,
        .global_positional_options = &global_positional_options,
    };
    tool_app_template app(std::move(app_cfg));

    return app.run_async(argc, argv, [] (const operation& op, const boost::program_options::variables_map& app_config) {
        if (!app_config.contains("type")) {
            throw std::invalid_argument("error: missing required option '--type'");
        }
        type_variant type = [&app_config] () -> type_variant {
            auto types = boost::copy_range<std::vector<data_type>>(app_config["type"].as<std::vector<sstring>>()
                    | boost::adaptors::transformed([] (const sstring_view type_name) { return db::marshal::type_parser::parse(type_name); }));
            if (app_config.contains("prefix-compound")) {
                return compound_type<allow_prefixes::yes>(std::move(types));
            } else if (app_config.contains("full-compound")) {
                return compound_type<allow_prefixes::no>(std::move(types));
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
                    auto values = boost::copy_range<std::vector<bytes>>(
                            app_config["value"].as<std::vector<sstring>>() | boost::adaptors::transformed(from_hex));

                    std::get<bytes_func>(handler)(std::move(type), std::move(values), app_config);
                }
                break;
            case 1:
                std::get<string_func>(handler)(std::move(type), app_config["value"].as<std::vector<sstring>>(), app_config);
                break;
        }

        return 0;
    });
}

} // namespace tools

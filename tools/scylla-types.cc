/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/algorithm/string/join.hpp>
#include <seastar/core/app-template.hh>

#include "compound.hh"
#include "db/marshal/type_parser.hh"
#include "log.hh"
#include "schema_builder.hh"
#include "tools/utils.hh"

using namespace seastar;

namespace bpo = boost::program_options;

namespace {

using type_variant = std::variant<
        data_type,
        compound_type<allow_prefixes::yes>,
        compound_type<allow_prefixes::no>>;

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

void print_handler(type_variant type, std::vector<bytes> values, const bpo::variables_map& vm) {
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
            type->validate(value, cql_serialization_format::internal());
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

using action_handler_func = void(*)(type_variant, std::vector<bytes>, const bpo::variables_map& vm);

class action_handler {
    std::string _name;
    std::string _summary;
    std::string _description;
    action_handler_func _func;

public:
    action_handler(std::string name, std::string summary, action_handler_func func, std::string description)
        : _name(std::move(name)), _summary(std::move(summary)), _description(std::move(description)), _func(func) {
    }

    const std::string& name() const { return _name; }
    const std::string& summary() const { return _summary; }
    const std::string& description() const { return _description; }

    void operator()(type_variant type, std::vector<bytes> values, const bpo::variables_map& vm) const {
        _func(std::move(type), std::move(values), vm);
    }
};

const std::vector<action_handler> action_handlers = {
    {"print", "print the value(s) in a human readable form", print_handler,
R"(
Deserialize and print the value(s) in a human-readable form.

Arguments: 1 or more serialized values.

Examples:

$ scylla types print -t Int32Type b34b62d4
-1286905132

$ scylla types print --prefix-compound -t TimeUUIDType -t Int32Type 0010d00819896f6b11ea00000000001c571b000400000010
(d0081989-6f6b-11ea-0000-0000001c571b, 16)
)"},
    {"compare", "compare two values", compare_handler,
R"(
Compare two values and print the result.

Arguments: 2 serialized values.

Examples:

$ scylla types compare -t 'ReversedType(TimeUUIDType)' b34b62d46a8d11ea0000005000237906 d00819896f6b11ea00000000001c571b
b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b
)"},
    {"validate", "validate the value(s)", validate_handler,
R"(
Check that the value(s) are valid for the type according to the requirements of
the type.

Arguments: 1 or more serialized values.

Examples:

$  scylla types validate -t Int32Type b34b62d4
b34b62d4: VALID - -1286905132
)"},
    {"tokenof", "tokenof (calculate the token of) the partition-key", tokenof_handler,
R"(
Decorate the key, that is calculate its token.
Only supports --full-compound.

Arguments: 1 or more serialized values.

Examples:

$ scylla types tokenof --full-compound -t UTF8Type -t SimpleDateType -t UUIDType 000d66696c655f696e7374616e63650004800049190010c61a3321045941c38e5675255feb0196
(file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): -5043005771368701888
)"},
};

}

namespace tools {

int scylla_types_main(int argc, char** argv) {
    const action_handler* found_ah = nullptr;
    if (std::strcmp(argv[1], "--help") != 0 && std::strcmp(argv[1], "-h") != 0) {
        found_ah = &tools::utils::get_selected_operation(argc, argv, action_handlers, "action");
    }

    app_template::seastar_options app_cfg;
    app_cfg.name = "scylla-types";

    auto description_template =
R"(scylla-types - a command-line tool to examine values belonging to scylla types.

Usage: scylla types {{action}} [--option1] [--option2] ... {{hex_value1}} [{{hex_value2}}] ...

Allows examining raw values obtained from e.g. sstables, logs or coredumps and
executing various actions on them. Values should be provided in hex form,
without a leading 0x prefix, e.g. 00783562. For scylla-types to be able to
examine the values, their type has to be provided. Types should be provided by
their cassandra class names, e.g. org.apache.cassandra.db.marshal.Int32Type for
the int32_type. The org.apache.cassandra.db.marshal. prefix can be ommited.
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

    if (found_ah) {
        app_cfg.description = found_ah->description();
    } else {
        app_cfg.description = format(description_template, boost::algorithm::join(action_handlers | boost::adaptors::transformed(
                        [] (const action_handler& ah) { return format("* {} - {}", ah.name(), ah.summary()); } ), "\n"));
    }

    tools::utils::configure_tool_mode(app_cfg);

    app_template app(std::move(app_cfg));

    app.add_options()
        ("type,t", bpo::value<std::vector<sstring>>(), "the type of the values, all values must be of the same type;"
                " when values are compounds, multiple types can be specified, one for each type making up the compound, "
                "note that the order of the types on the command line will be their order in the compound too")
        ("prefix-compound", "values are prefixable compounds (e.g. clustering key), composed of multiple values of possibly different types")
        ("full-compound", "values are full compounds (e.g. partition key), composed of multiple values of possibly different types")
        ;

    app.add_positional_options({
        {"value", bpo::value<std::vector<sstring>>(), "value(s) to process, can also be provided as positional arguments", -1}
    });

    return app.run(argc, argv, [&app, found_ah] {
        const action_handler& handler = *found_ah;

        if (!app.configuration().contains("type")) {
            throw std::invalid_argument("error: missing required option '--type'");
        }
        type_variant type = [&app] () -> type_variant {
            auto types = boost::copy_range<std::vector<data_type>>(app.configuration()["type"].as<std::vector<sstring>>()
                    | boost::adaptors::transformed([] (const sstring_view type_name) { return db::marshal::type_parser::parse(type_name); }));
            if (app.configuration().contains("prefix-compound")) {
                return compound_type<allow_prefixes::yes>(std::move(types));
            } else if (app.configuration().contains("full-compound")) {
                return compound_type<allow_prefixes::no>(std::move(types));
            } else { // non-compound type
                if (types.size() != 1) {
                    throw std::invalid_argument(fmt::format("error: expected a single '--type' argument, got  {}", types.size()));
                }
                return std::move(types.front());
            }
        }();

        if (!app.configuration().contains("value")) {
            throw std::invalid_argument("error: no values specified");
        }
        auto values = boost::copy_range<std::vector<bytes>>(
                app.configuration()["value"].as<std::vector<sstring>>() | boost::adaptors::transformed(from_hex));

        handler(std::move(type), std::move(values), app.configuration());

        return make_ready_future<>();
    });
}

} // namespace tools

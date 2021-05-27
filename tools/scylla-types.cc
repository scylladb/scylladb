/*
 * Copyright (C) 2020 ScyllaDB
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

#include <boost/algorithm/string/join.hpp>
#include <seastar/core/app-template.hh>

#include "compound.hh"
#include "db/marshal/type_parser.hh"
#include "log.hh"

using namespace seastar;

namespace {

using type_variant = std::variant<
        data_type,
        compound_type<allow_prefixes::yes>,
        compound_type<allow_prefixes::no>>;

struct printing_visitor {
    bytes_view value;

    sstring operator()(const data_type& type) {
        return type->to_string(value);
    }
    template <allow_prefixes AllowPrefixes>
    sstring operator()(const compound_type<AllowPrefixes>& type) {
        std::vector<sstring> printable_values;
        printable_values.reserve(type.types().size());

        const auto types = type.types();
        const auto values = type.deserialize_value(value);

        for (size_t i = 0; i != values.size(); ++i) {
            printable_values.emplace_back(types.at(i)->to_string(values.at(i)));
        }
        return format("({})", boost::algorithm::join(printable_values, ", "));
    }
};

sstring to_printable_string(const type_variant& type, bytes_view value) {
    return std::visit(printing_visitor{value}, type);
}

void print_handler(type_variant type, std::vector<bytes> values) {
    for (const auto& value : values) {
        std::cout << to_printable_string(type, value) << std::endl;
    }
}

void compare_handler(type_variant type, std::vector<bytes> values) {
    if (values.size() != 2) {
        throw std::runtime_error(fmt::format("compare_handler(): expected 2 values, got {}", values.size()));
    }

    struct {
        bytes_view lhs, rhs;

        int operator()(const data_type& type) {
            return type->compare(lhs, rhs);
        }
        int operator()(const compound_type<allow_prefixes::yes>& type) {
            return type.compare(lhs, rhs);
        }
        int operator()(const compound_type<allow_prefixes::no>& type) {
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
    std::cout
        << to_printable_string(type, values[0])
        << " "
        << res_str
        << " "
        << to_printable_string(type, values[1])
        << std::endl;
}

void validate_handler(type_variant type, std::vector<bytes> values) {
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
        // Cannot convert to printable string, as it can fail for invalid values.
        std::cout << to_hex(value) << ": ";

        std::exception_ptr ex;
        try {
            std::visit(validate_visitor{value}, type);
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            std::cout << " INVALID - " << ex;
        } else {
            std::cout << " VALID - " << to_printable_string(type, value);
        }
        std::cout << std::endl;
    }
}

using action_handler_func = void(*)(type_variant, std::vector<bytes>);

class action_handler {
    std::string _name;
    std::string _description;
    action_handler_func _func;

public:
    action_handler(std::string name, std::string description, action_handler_func func)
        : _name(std::move(name)), _description(std::move(description)), _func(func) {
    }

    const std::string& name() const { return _name; }
    const std::string& description() const { return _description; }

    void operator()(type_variant type, std::vector<bytes> values) const {
        _func(std::move(type), std::move(values));
    }
};

const std::vector<action_handler> action_handlers = {
    {"print", "print the value in a human readable form, takes 1+ values", print_handler},
    {"compare", "compare two values and print the result, takes 2 values", compare_handler},
    {"validate", "validate the values, takes 1+ values", validate_handler},
};

}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;

    app_template::config app_cfg;
    app_cfg.name = "scylla-types";

    auto description_template =
R"(scylla-types - a command-line tool to examine values belonging to scylla types.

Allows examining raw values obtained from e.g. sstables, logs or coredumps and
executing various actions on them. Values should be provided in hex form,
without a leading 0x prefix, e.g. 00783562. For scylla-types to be able to
examine the values, their type has to be provided. Types should be provided by
their cassandra class names, e.g. org.apache.cassandra.db.marshal.Int32Type for
the int32_type. The org.apache.cassandra.db.marshal. prefix can be ommited.
See https://github.com/scylladb/scylla/blob/master/docs/design-notes/cql3-type-mapping.md
for a mapping of cql3 types to Cassandra type class names.
Compound types specify their subtypes inside () separated by comma, e.g.:
MapType(Int32Type, BytesType). All provided values have to share the same type.
scylla-types executes so called actions on the provided values. Each action has
a required number of arguments. The supported actions are:
{}

Examples:
$ scylla-types --print -t Int32Type b34b62d4
-1286905132

$ scylla-types --compare -t 'ReversedType(TimeUUIDType)' b34b62d46a8d11ea0000005000237906 d00819896f6b11ea00000000001c571b
b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b

$ scylla-types --print --prefix-compound -t TimeUUIDType -t Int32Type 0010d00819896f6b11ea00000000001c571b000400000010
(d0081989-6f6b-11ea-0000-0000001c571b, 16)
)";
    app_cfg.description = format(description_template, boost::algorithm::join(action_handlers | boost::adaptors::transformed(
                    [] (const action_handler& ah) { return format("* --{} - {}", ah.name(), ah.description()); } ), "\n"));

    app_template app(std::move(app_cfg));

    for (const auto& ah : action_handlers) {
        app.add_options()(ah.name().c_str(), ah.description().c_str());
    }

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

    //FIXME: this exposes all core options, which we are not interested in.
    return app.run(argc, argv, [&app] {
        const action_handler& handler = [&app] () -> const action_handler& {
            std::vector<const action_handler*> found_handlers;
            for (const auto& ah : action_handlers) {
                if (app.configuration().contains(ah.name())) {
                    found_handlers.push_back(&ah);
                }
            }
            if (found_handlers.size() == 1) {
                return *found_handlers.front();
            }

            const auto all_handler_names = boost::algorithm::join(action_handlers | boost::adaptors::transformed(
                    [] (const action_handler& ah) { return format("--{}", ah.name()); } ), ", ");

            if (found_handlers.empty()) {
                throw std::invalid_argument(fmt::format("error: missing action, exactly one of {} should be specified", all_handler_names));
            }
            throw std::invalid_argument(fmt::format("error: need exactly one action, cannot specify more then one of {}", all_handler_names));
        }();

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

        handler(std::move(type), std::move(values));

        return make_ready_future<>();
    });
}

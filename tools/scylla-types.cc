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

#include <seastar/core/app-template.hh>

#include "db/marshal/type_parser.hh"

using namespace seastar;

namespace {

void print_handler(data_type type, std::vector<bytes> values) {
    for (const auto& value : values) {
        std::cout << type->to_string(value) << std::endl;
    }
}

void compare_handler(data_type type, std::vector<bytes> values) {
    if (values.size() != 2) {
        throw std::runtime_error(fmt::format("compare_handler(): expected 2 values, got {}", values.size()));
    }
    const auto res = type->compare(values[0], values[1]);
    sstring_view res_str;
    if (res == 0) {
        res_str = "==";
    } else if (res < 0) {
        res_str = "<";
    } else {
        res_str = ">";
    }
    std::cout
        << type->to_string(values[0])
        << " "
        << res_str
        << " "
        << type->to_string(values[1])
        << std::endl;
}

using action_handler = void(*)(data_type, std::vector<bytes>);

const std::unordered_map<sstring, action_handler> action_handlers = {
    {"print", print_handler},
    {"compare", compare_handler},
};

}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;

    app_template::config app_cfg;
    app_cfg.name = "scylla_types";
    app_cfg.description =
R"(scylla_types - a command-line tool to examine values belonging to scylla types.

Allows examining raw values obtained from e.g. sstables, logs or coredumps and
executing various actions on them. Values should be provided in hex form,
without a leading 0x prefix, e.g. 00783562. For scylla_types to be able to
examine the values, their type has to be provided. Types should be provided by
their cassandra class names, e.g. org.apache.cassandra.db.marshal.Int32Type for
the int32_type. The org.apache.cassandra.db.marshal. prefix can be ommited.
Compound types specify their subtypes inside () separated by comma, e.g.:
MapType(Int32Type, BytesType). All provided values have to share the same type.
scylla_types executes so called actions on the provided values. Each action has
a required number of arguments. The supported actions, with the number of
required values are:
* print - print the value in a human readable form; 1+ values
* compare - compare two values and print the result; 2 values

Examples:
$ scylla_types -a print -t Int32Type b34b62d4
-1286905132

$ scylla_types -a compare -t 'ReversedType(TimeUUIDType)' b34b62d46a8d11ea0000005000237906 d00819896f6b11ea00000000001c571b
b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b
)";

    app_template app(std::move(app_cfg));
    app.add_options()
        ("action,a", bpo::value<sstring>()->default_value("print"), "the action to execute on the values, "
                " valid actions are (with values required): print (1+), compare (2); default: print")
        ("type,t", bpo::value<sstring>(), "the type of the values, all values must be of the same type")
        ;

    app.add_positional_options({
        {"value", bpo::value<std::vector<sstring>>(), "value(s) to process, can also be provided as positional arguments", -1}
    });

    //FIXME: this exposes all core options, which we are not interested in.
    return app.run(argc, argv, [&app] {
        action_handler handler;
        {
            const auto action = app.configuration()["action"].as<sstring>();
            if (const auto it = action_handlers.find(action); it != action_handlers.end()) {
                handler = it->second;
            } else {
                throw std::invalid_argument(fmt::format("error: invalid action '{}', valid actions are: print, compare", action));
            }
        }

        if (!app.configuration().count("type")) {
            throw std::invalid_argument("error: missing required option '--type'");
        }
        auto type = db::marshal::type_parser::parse(app.configuration()["type"].as<sstring>());

        if (!app.configuration().count("value")) {
            throw std::invalid_argument("error: no values specified");
        }
        auto values = boost::copy_range<std::vector<bytes>>(
                app.configuration()["value"].as<std::vector<sstring>>() | boost::adaptors::transformed(from_hex));

        handler(std::move(type), std::move(values));

        return make_ready_future<>();
    });
}

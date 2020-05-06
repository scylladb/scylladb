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

using action_handler_func = void(*)(data_type, std::vector<bytes>);

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

    void operator()(data_type type, std::vector<bytes> values) const {
        _func(std::move(type), std::move(values));
    }
};

const std::vector<action_handler> action_handlers = {
    {"print", "print the value in a human readable form, takes 1+ values", print_handler},
    {"compare", "compare two values and print the result, takes 2 values", compare_handler},
};

}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;

    app_template::config app_cfg;
    app_cfg.name = "scylla_types";

    auto description_template =
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
a required number of arguments. The supported actions are:
{}

Examples:
$ scylla_types --print -t Int32Type b34b62d4
-1286905132

$ scylla_types --compare -t 'ReversedType(TimeUUIDType)' b34b62d46a8d11ea0000005000237906 d00819896f6b11ea00000000001c571b
b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b
)";
    app_cfg.description = format(description_template, boost::algorithm::join(action_handlers | boost::adaptors::transformed(
                    [] (const action_handler& ah) { return format("* --{} - {}", ah.name(), ah.description()); } ), "\n"));

    app_template app(std::move(app_cfg));

    for (const auto& ah : action_handlers) {
        app.add_options()(ah.name().c_str(), ah.description().c_str());
    }

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
        const action_handler& handler = [&app] () -> const action_handler& {
            std::vector<const action_handler*> found_handlers;
            for (const auto& ah : action_handlers) {
                if (app.configuration().count(ah.name())) {
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

/*
 * Copyright (C) 2021-present ScyllaDB
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
#include <boost/range/adaptor/transformed.hpp>
#include <boost/program_options.hpp>
#include <seastar/core/app-template.hh>
#include <string>
#include <vector>
#include <fmt/format.h>
#include "seastarx.hh"

namespace tools::utils {

template<typename Op>
concept Operation = requires(Op op) {
    { op.name() }; // returns some string
};

// Extract the operation from the argv.
//
// The operation is expected to be at argv[1].If found, it is shifted out to the
// end (effectively removed) and the corresponding Op* is returned.
// If not found or unrecognized an error is logged and exit() is called.
template <Operation Op>
const Op& get_selected_operation(int& ac, char**& av, const std::vector<Op>& operations, std::string_view alias) {
    if (ac < 2) {
        fmt::print(std::cerr, "error: missing mandatory {} argument\n", alias);
        exit(1);
    }

    const Op* found_operation = nullptr;
    for (const auto& op : operations) {
        if (av[1] == op.name()) {
            found_operation = &op;
            break;
        }
    }
    if (found_operation) {
        --ac;
        for (int i = 1; i < ac; ++i) {
            std::swap(av[i], av[i + 1]);
        }
        return *found_operation;
    }

    const auto all_operation_names = boost::algorithm::join(operations | boost::adaptors::transformed([] (const Op& op) { return op.name(); } ), ", ");

    fmt::print(std::cerr, "error: unrecognized {} argument: expected one of ({}), got {}\n", alias, all_operation_names, av[1]);
    exit(1);
}

template <Operation Op>
const Op& get_selected_operation(boost::program_options::variables_map& app_config, const std::vector<Op>& operations, std::string_view alias) {
    std::vector<const Op*> found_operations;
    for (const auto& op : operations) {
        if (app_config.contains(op.name())) {
            found_operations.push_back(&op);
        }
    }
    if (found_operations.size() == 1) {
        return *found_operations.front();
    }

    const auto all_operation_names = boost::algorithm::join(operations | boost::adaptors::transformed(
            [] (const Op& op) { return format("--{}", op.name()); } ), ", ");

    if (found_operations.empty()) {
        throw std::invalid_argument(fmt::format("error: missing {}, exactly one of {} should be specified", alias, all_operation_names));
    }
    throw std::invalid_argument(fmt::format("error: need exactly one {}, cannot specify more then one of {}", alias, all_operation_names));
}

// Configure seastar with defaults more appropriate for a tool.
// Make seastar not act as if it owns the place, taking over all system resources.
// Set ERROR as the default log level, except for the logger \p logger_name, which
// is configured with INFO level.
void configure_tool_mode(app_template::seastar_options& opts, const sstring& logger_name = {});

} // namespace tools::utils

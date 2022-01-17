/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

// Configure seastar with defaults more appropriate for a tool.
// Make seastar not act as if it owns the place, taking over all system resources.
// Set ERROR as the default log level, except for the logger \p logger_name, which
// is configured with INFO level.
void configure_tool_mode(app_template::seastar_options& opts, const sstring& logger_name = {});

} // namespace tools::utils

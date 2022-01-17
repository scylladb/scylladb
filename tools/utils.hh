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

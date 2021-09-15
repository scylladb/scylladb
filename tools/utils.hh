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
#include <string>
#include <vector>
#include <fmt/format.h>

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

} // namespace tools::utils

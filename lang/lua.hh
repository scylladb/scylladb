/*
 * Copyright (C) 2019-present ScyllaDB
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

#pragma once

#include "types.hh"
#include "utils/updateable_value.hh"
#include <seastar/core/future.hh>

namespace db {
class config;
}

namespace lua {
// type safe alias
struct bitcode_view {
    std::string_view bitcode;
};

struct runtime_config {
    utils::updateable_value<unsigned> timeout_in_ms;
    utils::updateable_value<unsigned> max_bytes;
    utils::updateable_value<unsigned> max_contiguous;
};

runtime_config make_runtime_config(const db::config& config);

sstring compile(const runtime_config& cfg, const std::vector<sstring>& arg_names, sstring script);
seastar::future<bytes_opt> run_script(bitcode_view bitcode, const std::vector<data_value>& values,
                                      data_type return_type, const runtime_config& cfg);
}

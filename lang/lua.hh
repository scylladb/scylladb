/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "types/types.hh"
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

sstring compile(const runtime_config& cfg, const std::vector<sstring>& arg_names, sstring script);
seastar::future<bytes_opt> run_script(bitcode_view bitcode, const std::vector<data_value>& values,
                                      data_type return_type, const runtime_config& cfg);
}

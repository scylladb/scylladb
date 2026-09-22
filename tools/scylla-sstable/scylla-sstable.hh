/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <filesystem>
#include <boost/program_options.hpp>

#include "schema/schema_fwd.hh"
#include "seastarx.hh"
#include "utils/log.hh"

namespace db {
class config;
}

// The internals of the tool its operations share.
namespace tools {

extern logging::logger sst_log;

/// What the path of an sstable, or of the directory holding them, says about it
struct sstable_path_info {
    std::filesystem::path sstable_path;
    std::filesystem::path data_dir_path;
    sstring keyspace;
    sstring table;
    table_id id;
};

/// Deduce the above from the first sstable argument, throwing if it says nothing
sstable_path_info extract_from_sstable_path(const boost::program_options::variables_map& app_config);

/// The data dir the sstables being examined live in, empty when it is not known
std::filesystem::path find_data_dir(const boost::program_options::variables_map& app_config, const db::config& dbcfg);

enum class output_format {
    text, json
};

/// The format --output-format asks for, or \p default_format when it is not given
output_format get_output_format_from_options(const boost::program_options::variables_map& opts, output_format default_format);

}

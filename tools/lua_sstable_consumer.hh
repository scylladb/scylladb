/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/program-options.hh>

#include "schema/schema_fwd.hh"
#include "tools/sstable_consumer.hh"

class reader_permit;

/// Sstable consumer consuming the content via a lua script
///
/// Loads the script from /p script_path and feeds the consumed content to the
/// script.
/// See the help section for the script operation in ./scylla-sstable.cc for more
/// details on the Lua API.
future<std::unique_ptr<sstable_consumer>> make_lua_sstable_consumer(schema_ptr s, reader_permit p, std::string_view script_path,
        program_options::string_map script_args);

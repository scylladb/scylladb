/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <filesystem>
#include <functional>
#include <string_view>
#include <optional>

#include "tools/sstable_manager_service.hh"
#include "schema/schema_fwd.hh"
#include "types/types.hh"
#include "reader_permit.hh"

mutation_opt read_mutation_from_table_offline(sharded<sstable_manager_service>& sst_man,
                                              reader_permit permit,
                                              std::filesystem::path table_path,
                                              std::string_view keyspace,
                                              std::function<schema_ptr()> table_schema,
                                              data_value primary_key,
                                              std::optional<data_value> clustering_key);

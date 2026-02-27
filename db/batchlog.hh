/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "mutation/mutation.hh"
#include "utils/UUID.hh"

namespace db {

mutation get_batchlog_mutation_for(schema_ptr schema, const utils::chunked_vector<mutation>& mutations, int32_t version, db_clock::time_point now, const utils::UUID& id);

mutation get_batchlog_delete_mutation(schema_ptr schema, int32_t version, db_clock::time_point now, const utils::UUID& id);

}

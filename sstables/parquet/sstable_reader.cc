/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/sstable_reader.hh"

#include "schema/schema.hh"

namespace sstables::parquet {

lw_shared_ptr<const read_plan> sstable_read_state::plan_for(const ::schema& s, const column_translation& ct, const file_summary& summary) {
    if (!_plan || _plan->version() != s.version()) [[unlikely]] {
        _plan = make_lw_shared<const read_plan>(_mapping, summary, s, ct);
    }
    return _plan;
}

} // namespace sstables::parquet

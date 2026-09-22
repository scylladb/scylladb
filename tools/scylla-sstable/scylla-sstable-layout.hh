/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <vector>
#include <boost/program_options.hpp>

#include "db/config.hh"
#include "reader_permit.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"
#include "sstables/shared_sstable.hh"

namespace sstables {
class sstables_manager;
}

namespace tools {

/// Describe how the sstables of a table are organized by its compaction strategy
///
/// Incremental and size-tiered compaction organize them into runs, leveled
/// compaction into levels and time-window compaction into time windows. The
/// sstables are grouped accordingly, per compaction group, and each group is
/// annotated with the aggregate of the sstables in it.
void layout_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const db::config& dbcfg, const boost::program_options::variables_map& vm);

/// The sstable properties the layout can report, as a list for --help
sstring layout_columns_help();

/// The properties --columns reports when it is not given
sstring default_layout_columns();

}

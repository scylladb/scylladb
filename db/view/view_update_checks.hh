/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include "streaming/stream_reason.hh"
#include "locator/token_metadata_fwd.hh"
#include "seastarx.hh"

namespace replica {
class table;
}

namespace db::view {
class view_builder;

enum class sstable_destination_decision {
    normal_directory,               // use normal sstable directory
    staging_directly_to_generator,  // use staging directory and create view building task for the sstable
    staging_managed_by_vbc          // use staging directory and register the sstable to view update generator
};

future<sstable_destination_decision> check_needs_view_update_path(view_builder& vb, locator::token_metadata_ptr tmptr, const replica::table& t,
        streaming::stream_reason reason);

}

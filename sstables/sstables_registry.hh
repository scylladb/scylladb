// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#pragma once

#include "open_info.hh"

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include "locator/host_id.hh"
#include "schema/schema_fwd.hh"
#include "db/consistency_level_type.hh"
#include "seastarx.hh"

namespace sstables {

// sstables_manager needs to store the names of its sstables somewhere, when
// using object storage. This is system_keyspace, but for modularity we hide
// it behind this interface.

class sstables_registry {
public:
    virtual ~sstables_registry();
    virtual future<> create_entry(table_id tid, locator::host_id node_owner, sstring status, sstable_state state, sstables::entry_descriptor desc) = 0;
    virtual future<> update_entry_status(table_id tid, locator::host_id node_owner, sstables::generation_type gen, sstring status) = 0;
    virtual future<> update_entry_state(table_id tid, locator::host_id node_owner, sstables::generation_type gen, sstables::sstable_state state) = 0;
    virtual future<> delete_entry(table_id tid, locator::host_id node_owner, sstables::generation_type gen) = 0;
    using entry_consumer = noncopyable_function<future<>(sstring status, sstables::sstable_state state, sstables::entry_descriptor desc)>;
    virtual future<> sstables_registry_list(table_id tid, locator::host_id node_owner, entry_consumer consumer, db::consistency_level cl = db::consistency_level::LOCAL_QUORUM) = 0;
};

} // namespace sstables

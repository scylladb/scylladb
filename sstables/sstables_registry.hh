// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include "open_info.hh"

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace sstables {

// sstables_manager needs to store the names of its sstables somewhere, when
// using object storage. This is system_keyspace, but for modularity we hide
// it behind this interface.

class sstables_registry {
public:
    virtual ~sstables_registry();
    virtual future<> create_entry(table_id owner, sstring status, sstable_state state, sstables::entry_descriptor desc) = 0;
    virtual future<> update_entry_status(table_id owner, sstables::generation_type gen, sstring status) = 0;
    virtual future<> update_entry_state(table_id owner, sstables::generation_type gen, sstables::sstable_state state) = 0;
    virtual future<> delete_entry(table_id owner, sstables::generation_type gen) = 0;
    using entry_consumer = noncopyable_function<future<>(sstring status, sstables::sstable_state state, sstables::entry_descriptor desc)>;
    virtual future<> sstables_registry_list(table_id owner, entry_consumer consumer) = 0;
};

} // namespace sstables

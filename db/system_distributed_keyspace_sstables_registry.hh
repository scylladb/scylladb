// Copyright (C) 2026-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#pragma once

#include "db/system_distributed_keyspace.hh"
#include "sstables/sstables_registry.hh"

namespace db {

class system_distributed_keyspace_sstables_registry : public sstables::sstables_registry {
    system_distributed_keyspace& _sys_dist_ks;
public:
    explicit system_distributed_keyspace_sstables_registry(system_distributed_keyspace& sys_dist_ks)
        : _sys_dist_ks(sys_dist_ks) {}
    future<> create_entry(table_id tid, locator::host_id node_owner, sstring status, sstables::sstable_state state, sstables::entry_descriptor desc) override {
        return _sys_dist_ks.sstables_registry_create_entry(tid, node_owner, std::move(status), state, std::move(desc));
    }
    future<> update_entry_status(table_id tid, locator::host_id node_owner, sstables::generation_type gen, sstring status) override {
        return _sys_dist_ks.sstables_registry_update_entry_status(tid, node_owner, gen, std::move(status));
    }
    future<> update_entry_state(table_id tid, locator::host_id node_owner, sstables::generation_type gen, sstables::sstable_state state) override {
        return _sys_dist_ks.sstables_registry_update_entry_state(tid, node_owner, gen, state);
    }
    future<> delete_entry(table_id tid, locator::host_id node_owner, sstables::generation_type gen) override {
        return _sys_dist_ks.sstables_registry_delete_entry(tid, node_owner, gen);
    }
    future<> sstables_registry_list(table_id tid, locator::host_id node_owner, entry_consumer consumer) override {
        return _sys_dist_ks.sstables_registry_list(tid, node_owner, std::move(consumer));
    }
};

} // namespace db

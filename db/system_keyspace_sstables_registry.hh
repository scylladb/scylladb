// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)

#pragma once

#include "system_keyspace.hh"
#include "sstables/sstables_registry.hh"

// Implement the sstables_registry interface using system_keyspace.

namespace db {

class system_keyspace_sstables_registry : public sstables::sstables_registry {
    shared_ptr<system_keyspace> _keyspace;
public:
    system_keyspace_sstables_registry(system_keyspace& keyspace) : _keyspace(keyspace.shared_from_this()) {}

    virtual seastar::future<> create_entry(table_id tid, locator::host_id node_owner, sstring status, sstables::sstable_state state, sstables::entry_descriptor desc) override {
        return _keyspace->sstables_registry_create_entry(tid, node_owner, status, state, desc);
    }

    virtual seastar::future<> update_entry_status(table_id tid, locator::host_id node_owner, sstables::generation_type gen, sstring status) override {
        return _keyspace->sstables_registry_update_entry_status(tid, node_owner, gen, status);
    }

    virtual seastar::future<> batch_update_entry_status(table_id tid, locator::host_id node_owner, const std::vector<sstables::generation_type>& gens, sstring status) override {
        return _keyspace->sstables_registry_batch_update_entry_status(tid, node_owner, gens, status);
    }

    virtual seastar::future<> update_entry_state(table_id tid, locator::host_id node_owner, sstables::generation_type gen, sstables::sstable_state state) override {
        return _keyspace->sstables_registry_update_entry_state(tid, node_owner, gen, state);
    }

    virtual seastar::future<> delete_entry(table_id tid, locator::host_id node_owner, sstables::generation_type gen) override {
        return _keyspace->sstables_registry_delete_entry(tid, node_owner, gen);
    }

    virtual seastar::future<> sstables_registry_list(table_id tid, locator::host_id node_owner, entry_consumer consumer) override {
        return _keyspace->sstables_registry_list(tid, node_owner, std::move(consumer));
    }
};

}

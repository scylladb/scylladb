// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)

#pragma once

#include "system_keyspace.hh"
#include "sstables/sstables_registry.hh"

// Implement the sstables_registry interface using system_keyspace.

namespace db {

class system_keyspace_sstables_registry : public sstables::sstables_registry {
    shared_ptr<system_keyspace> _keyspace;
public:
    system_keyspace_sstables_registry(system_keyspace& keyspace) : _keyspace(keyspace.shared_from_this()) {}

    virtual seastar::future<> create_entry(table_id owner, sstring status, sstables::sstable_state state, sstables::entry_descriptor desc) override {
        return _keyspace->sstables_registry_create_entry(owner, status, state, desc);
    }

    virtual seastar::future<> update_entry_status(table_id owner, sstables::generation_type gen, sstring status) override {
        return _keyspace->sstables_registry_update_entry_status(owner, gen, status);
    }

    virtual seastar::future<> update_entry_state(table_id owner, sstables::generation_type gen, sstables::sstable_state state) override {
        return _keyspace->sstables_registry_update_entry_state(owner, gen, state);
    }

    virtual seastar::future<> delete_entry(table_id owner, sstables::generation_type gen) override {
        return _keyspace->sstables_registry_delete_entry(owner, gen);
    }

    virtual seastar::future<> sstables_registry_list(table_id owner, entry_consumer consumer) override {
        return _keyspace->sstables_registry_list(owner, std::move(consumer));
    }
};

}

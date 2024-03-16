/*
 * Copyright (C) 2020-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include "sstables/version.hh"
#include "gms/feature.hh"

using namespace seastar;

namespace replica {
class database;
}

namespace gms {
class gossiper;
class feature_service;
}

namespace db {

class system_keyspace;
class sstables_format_listener;

class feature_enabled_listener : public gms::feature::listener {
    sstables_format_listener& _listener;
    sstables::sstable_version_types _format;

public:
    feature_enabled_listener(sstables_format_listener& l, sstables::sstable_version_types format)
        : _listener(l)
        , _format(format)
    { }
    void on_enabled() override;
};

class sstables_format_selector {
    sharded<replica::database>& _db;
    db::system_keyspace* _sys_ks = nullptr;
    sstables::sstable_version_types _selected_format = sstables::sstable_version_types::me;
    future<> select_format(sstables::sstable_version_types new_format);
    future<> read_sstables_format();
public:
    explicit sstables_format_selector(sharded<replica::database>& db);

    future<> on_system_tables_loaded(db::system_keyspace& sys_ks);

    inline sstables::sstable_version_types selected_format() const noexcept {
        return _selected_format;
    }
    future<> update_format(sstables::sstable_version_types new_format);
};

class sstables_format_listener {
    gms::gossiper& _gossiper;
    sharded<gms::feature_service>& _features;
    sstables_format_selector& _selector;
    seastar::named_semaphore _sem = {1, named_semaphore_exception_factory{"feature listeners"}};
    seastar::gate _sel;

    feature_enabled_listener _me_feature_listener;
public:
    sstables_format_listener(gms::gossiper& g, sharded<gms::feature_service>& f, sstables_format_selector& selector);

    future<> start();
    future<> stop();

    future<> maybe_select_format(sstables::sstable_version_types new_format);
};

} // namespace sstables

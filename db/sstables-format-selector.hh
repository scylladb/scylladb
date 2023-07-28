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
class sstables_format_selector;

class feature_enabled_listener : public gms::feature::listener {
    sstables_format_selector& _selector;
    sstables::sstable_version_types _format;

public:
    feature_enabled_listener(sstables_format_selector& s, sstables::sstable_version_types format)
        : _selector(s)
        , _format(format)
    { }
    void on_enabled() override;
};

class sstables_format_selector {
    gms::gossiper& _gossiper;
    sharded<gms::feature_service>& _features;
    sharded<replica::database>& _db;
    db::system_keyspace& _sys_ks;
    seastar::named_semaphore _sem = {1, named_semaphore_exception_factory{"feature listeners"}};
    seastar::gate _sel;

    feature_enabled_listener _md_feature_listener;
    feature_enabled_listener _me_feature_listener;

    sstables::sstable_version_types _selected_format = sstables::sstable_version_types::mc;
    future<> select_format(sstables::sstable_version_types new_format);
    future<> read_sstables_format();

public:
    sstables_format_selector(gms::gossiper& g, sharded<gms::feature_service>& f, sharded<replica::database>& db, db::system_keyspace& sys_ks);

    future<> start();
    future<> stop();

    future<> maybe_select_format(sstables::sstable_version_types new_format);
};

} // namespace sstables

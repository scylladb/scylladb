/*
 * Copyright (C) 2020-present ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include "sstables/version.hh"
#include "gms/feature.hh"

using namespace seastar;

class database;

namespace gms {
class gossiper;
class feature_service;
}

namespace db {

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
    sharded<database>& _db;
    seastar::named_semaphore _sem = {1, named_semaphore_exception_factory{"feature listeners"}};
    seastar::gate _sel;

    feature_enabled_listener _mc_feature_listener;
    feature_enabled_listener _md_feature_listener;

    sstables::sstable_version_types _selected_format = sstables::sstable_version_types::la;
    future<> select_format(sstables::sstable_version_types new_format);
    future<> read_sstables_format();

    future<> do_maybe_select_format(sstables::sstable_version_types new_format);

public:
    sstables_format_selector(gms::gossiper& g, sharded<gms::feature_service>& f, sharded<database>& db);

    future<> start();
    future<> stop();

    future<> maybe_select_format(sstables::sstable_version_types new_format);

    void sync() {
        get_units(_sem, 1).get0();
    }
};

} // namespace sstables

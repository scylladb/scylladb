/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/tablets.hh"
#include "replica/tablets.hh"
#include "locator/tablet_replication_strategy.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"
#include "service/tablet_allocator.hh"

using namespace locator;
using namespace replica;

namespace service {

class tablet_allocator_impl : public tablet_allocator::impl
                            , public service::migration_listener::empty_listener {
    service::migration_notifier& _migration_notifier;
    replica::database& _db;
    bool _stopped = false;
public:
    tablet_allocator_impl(service::migration_notifier& mn, replica::database& db)
            : _migration_notifier(mn)
            , _db(db) {
        _migration_notifier.register_listener(this);
    }

    tablet_allocator_impl(tablet_allocator_impl&&) = delete; // "this" captured.

    ~tablet_allocator_impl() {
        assert(_stopped);
    }

    future<> stop() {
        co_await _migration_notifier.unregister_listener(this);
        _stopped = true;
    }

    void on_before_create_column_family(const schema& s, std::vector<mutation>& muts, api::timestamp_type ts) override {
        keyspace& ks = _db.find_keyspace(s.ks_name());
        auto&& rs = ks.get_replication_strategy();
        if (auto&& tablet_rs = rs.maybe_as_tablet_aware()) {
            auto tm = _db.get_shared_token_metadata().get();
            auto map = tablet_rs->allocate_tablets_for_new_table(s.shared_from_this(), tm).get0();
            muts.emplace_back(tablet_map_to_mutation(map, s.id(), s.keypace_name(), s.cf_name(), ts).get0());
        }
    }

    void on_before_drop_column_family(const schema& s, std::vector<mutation>& muts, api::timestamp_type ts) override {
        keyspace& ks = _db.find_keyspace(s.ks_name());
        auto&& rs = ks.get_replication_strategy();
        std::vector<mutation> result;
        if (rs.uses_tablets()) {
            auto tm = _db.get_shared_token_metadata().get();
            muts.emplace_back(make_drop_tablet_map_mutation(s.keypace_name(), s.id(), ts));
        }
    }

    void on_before_drop_keyspace(const sstring& keyspace_name, std::vector<mutation>& muts, api::timestamp_type ts) override {
        keyspace& ks = _db.find_keyspace(keyspace_name);
        auto&& rs = ks.get_replication_strategy();
        if (rs.uses_tablets()) {
            auto tm = _db.get_shared_token_metadata().get();
            for (auto&& [name, s] : ks.metadata()->cf_meta_data()) {
                muts.emplace_back(make_drop_tablet_map_mutation(keyspace_name, s->id(), ts));
            }
        }
    }

    // FIXME: Handle materialized views.
};

tablet_allocator::tablet_allocator(service::migration_notifier& mn, replica::database& db)
    : _impl(std::make_unique<tablet_allocator_impl>(mn, db)) {
}

future<> tablet_allocator::stop() {
    return impl().stop();
}

tablet_allocator_impl& tablet_allocator::impl() {
    return static_cast<tablet_allocator_impl&>(*_impl);
}

}

/*
 * Copyright (C) 2020-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <seastar/core/coroutine.hh>
#include "sstables-format-selector.hh"
#include "log.hh"
#include "replica/database.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "gms/versioned_value.hh"
#include "db/system_keyspace.hh"

namespace db {

static logging::logger logger("format_selector");
static const sstring SSTABLE_FORMAT_PARAM_NAME = "sstable_format";

void feature_enabled_listener::on_enabled() {
    if (!_started) {
        _started = true;
        _listener.maybe_select_format(_format).get();
    }
}

sstables_format_selector::sstables_format_selector(sharded<replica::database>& db)
    : _db(db)
{
}

future<> sstables_format_selector::on_system_tables_loaded(db::system_keyspace& sys_ks) {
    _sys_ks = &sys_ks;
    return read_sstables_format();
}

future<> sstables_format_selector::read_sstables_format() {
    std::optional<sstring> format_opt = co_await _sys_ks->get_scylla_local_param(SSTABLE_FORMAT_PARAM_NAME);
    if (format_opt) {
        sstables::sstable_version_types format = sstables::version_from_string(*format_opt);
        co_await select_format(format);
    }
}

future<> sstables_format_selector::update_format(sstables::sstable_version_types new_format) {
    if (!_sys_ks) {
        on_internal_error(logger, format("system keyspace is not loaded"));
    }
    co_await _sys_ks->set_scylla_local_param(SSTABLE_FORMAT_PARAM_NAME, fmt::to_string(new_format), true);
    co_await select_format(new_format);
}

future<> sstables_format_selector::select_format(sstables::sstable_version_types format) {
    logger.info("Selected {} sstables format", format);
    _selected_format = format;
    co_await _db.invoke_on_all([this] (replica::database& db) {
        db.set_format(_selected_format);
    });
}

sstables_format_listener::sstables_format_listener(gms::gossiper& g, sharded<gms::feature_service>& f, sstables_format_selector& selector)
    : _gossiper(g)
    , _features(f)
    , _selector(selector)
    , _me_feature_listener(*this, sstables::sstable_version_types::me)
{ }

future<> sstables_format_listener::maybe_select_format(sstables::sstable_version_types new_format) {
    auto hg = _sel.hold();
    auto units = co_await get_units(_sem, 1);

    if (new_format > _selector.selected_format()) {
        co_await _selector.update_format(new_format);
        // FIXME discarded future
        (void)_gossiper.add_local_application_state(gms::application_state::SUPPORTED_FEATURES,
                 gms::versioned_value::supported_features(_features.local().supported_feature_set())).finally([h = std::move(hg)] {});
    }
}

future<> sstables_format_listener::start() {
    SCYLLA_ASSERT(this_shard_id() == 0);
    // The listener may fire immediately, create a thread for that case.
    co_await seastar::async([this] {
        _me_feature_listener.on_enabled();
    });
}

future<> sstables_format_listener::stop() {
    co_await _sel.close();
}

} // namespace sstables

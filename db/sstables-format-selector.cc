/*
 * Copyright (C) 2020 ScyllaDB
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

#include "sstables-format-selector.hh"
#include "service/storage_service.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "gms/versioned_value.hh"
#include "db/system_keyspace.hh"

namespace db {

static const sstring SSTABLE_FORMAT_PARAM_NAME = "sstable_format";

void feature_enabled_listener::on_enabled() {
    if (!_started) {
        _started = true;
        // FIXME -- discarded future
        (void)_selector.maybe_select_format(_format);
    }
}

sstables_format_selector::sstables_format_selector(gms::gossiper& g, gms::feature_service& f, bool for_testing)
    : _gossiper(g)
    , _features(f)
    , _mc_feature_listener(*this, sstables::sstable_version_types::mc) {

    if (!for_testing) {
        if (this_shard_id() == 0) {
            _features.cluster_supports_mc_sstable().when_enabled(_mc_feature_listener);
        }
    }
}

future<> sstables_format_selector::maybe_select_format(sstables::sstable_version_types new_format) {
    return with_semaphore(_sem, 1, [this, new_format] {
        if (!sstables::is_later(new_format, _selected_format)) {
            return make_ready_future<bool>(false);
        }
        return db::system_keyspace::set_scylla_local_param(SSTABLE_FORMAT_PARAM_NAME, to_string(new_format)).then([this, new_format] {
            return select_format(new_format);
        }).then([] { return true; });
    }).then([this] (bool update_features) {
        if (!update_features) {
            return make_ready_future<>();
        }
        return _gossiper.add_local_application_state(gms::application_state::SUPPORTED_FEATURES,
                         gms::versioned_value::supported_features(join(",", _features.supported_feature_set())));
    });
}

future<> sstables_format_selector::read_sstables_format() {
    return db::system_keyspace::get_scylla_local_param(SSTABLE_FORMAT_PARAM_NAME).then([this] (std::optional<sstring> format_opt) {
        if (format_opt) {
            sstables::sstable_version_types format = sstables::from_string(*format_opt);
            return select_format(format);
        }
        return make_ready_future<>();
    });
}

future<> sstables_format_selector::select_format(sstables::sstable_version_types format) {
    _selected_format = format;
    return service::get_storage_service().invoke_on_all([format] (service::storage_service& s) {
        s._sstables_format = format;
        if (sstables::is_later(format, sstables::sstable_version_types::la)) {
            s._feature_service.support(gms::features::UNBOUNDED_RANGE_TOMBSTONES);
        }
    });
}

} // namespace sstables

/*
 * Copyright (C) 2018-present ScyllaDB
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

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <unordered_map>
#include <functional>
#include <set>
#include <any>
#include "seastarx.hh"
#include "db/schema_features.hh"
#include "gms/feature.hh"

namespace db { class config; }
namespace service { class storage_service; }

namespace gms {

class feature_service;

struct feature_config {
private:
    std::set<sstring> _disabled_features;
    std::set<sstring> _masked_features;
    feature_config();

    friend class feature_service;
    friend feature_config feature_config_from_db_config(db::config& cfg, std::set<sstring> disabled);
};

feature_config feature_config_from_db_config(db::config& cfg, std::set<sstring> disabled = {});

/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 */
class feature_service final {
    void register_feature(feature& f);
    void unregister_feature(feature& f);
    friend class feature;
    std::unordered_map<sstring, std::reference_wrapper<feature>> _registered_features;

    feature_config _config;
public:
    explicit feature_service(feature_config cfg);
    ~feature_service() = default;
    future<> stop();
    // Has to run inside seastar::async context
    void enable(const sstring& name);
    void support(const std::string_view& name);
    void enable(const std::set<std::string_view>& list);
    db::schema_features cluster_schema_features() const;
    std::set<std::string_view> known_feature_set();
    std::set<std::string_view> supported_feature_set();

private:
    gms::feature _udf_feature;
    gms::feature _mc_sstable_feature;
    gms::feature _md_sstable_feature;
    gms::feature _unbounded_range_tombstones_feature;
    gms::feature _view_virtual_columns;
    gms::feature _digest_insensitive_to_expiry;
    gms::feature _computed_columns;
    gms::feature _cdc_feature;
    gms::feature _nonfrozen_udts;
    gms::feature _hinted_handoff_separate_connection;
    gms::feature _lwt_feature;
    gms::feature _per_table_partitioners_feature;
    gms::feature _per_table_caching_feature;
    gms::feature _digest_for_null_values_feature;
    gms::feature _correct_idx_token_in_secondary_index_feature;
    gms::feature _alternator_streams_feature;
    gms::feature _range_scan_data_variant;
    gms::feature _cdc_generations_v2;

public:
    bool cluster_supports_user_defined_functions() const {
        return bool(_udf_feature);
    }

    const feature& cluster_supports_mc_sstable() const {
        return _mc_sstable_feature;
    }

    const feature& cluster_supports_md_sstable() const {
        return _md_sstable_feature;
    }

    const feature& cluster_supports_cdc() const {
        return _cdc_feature;
    }

    const feature& cluster_supports_per_table_partitioners() const {
        return _per_table_partitioners_feature;
    }

    const feature& cluster_supports_per_table_caching() const {
        return _per_table_caching_feature;
    }

    const feature& cluster_supports_digest_for_null_values() const {
        return _digest_for_null_values_feature;
    }

    const feature& cluster_supports_unbounded_range_tombstones() const {
        return _unbounded_range_tombstones_feature;
    }

    const feature& cluster_supports_view_virtual_columns() const {
        return _view_virtual_columns;
    }
    const feature& cluster_supports_digest_insensitive_to_expiry() const {
        return _digest_insensitive_to_expiry;
    }

    const feature& cluster_supports_computed_columns() const {
        return _computed_columns;
    }

    bool cluster_supports_nonfrozen_udts() const {
        return bool(_nonfrozen_udts);
    }

    bool cluster_supports_hinted_handoff_separate_connection() {
        return bool(_hinted_handoff_separate_connection);
    }

    bool cluster_supports_lwt() const {
        return bool(_lwt_feature);
    }

    bool cluster_supports_correct_idx_token_in_secondary_index() const {
        return bool(_correct_idx_token_in_secondary_index_feature);
    }

    bool cluster_supports_alternator_streams() const {
        return bool(_alternator_streams_feature);
    }

    // Range scans have a data variant, which produces query::result directly,
    // instead of through the intermediate reconcilable_result format.
    bool cluster_supports_range_scan_data_variant() const {
        return bool(_range_scan_data_variant);
    }

    bool cluster_supports_cdc_generations_v2() const {
        return bool(_cdc_generations_v2);
    }
};

} // namespace gms

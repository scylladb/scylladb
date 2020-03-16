/*
 * Copyright (C) 2018 ScyllaDB
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
#include "seastarx.hh"
#include "db/schema_features.hh"

namespace db { class config; }
namespace service { class storage_service; }

namespace gms {

class feature;

struct feature_config {
    bool enable_sstables_mc_format = false;
    bool enable_user_defined_functions = false;
    bool enable_cdc = false;
    bool enable_lwt = false;
    std::set<sstring> disabled_features;
    feature_config();
};

feature_config feature_config_from_db_config(db::config& cfg);

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
    void enable(const std::set<std::string_view>& list);
    db::schema_features cluster_schema_features() const;
    std::set<std::string_view> known_feature_set();

private:
    gms::feature _range_tombstones_feature;
    gms::feature _large_partitions_feature;
    gms::feature _materialized_views_feature;
    gms::feature _counters_feature;
    gms::feature _indexes_feature;
    gms::feature _digest_multipartition_read_feature;
    gms::feature _correct_counter_order_feature;
    gms::feature _schema_tables_v3;
    gms::feature _correct_non_compound_range_tombstones;
    gms::feature _write_failure_reply_feature;
    gms::feature _xxhash_feature;
    gms::feature _udf_feature;
    gms::feature _roles_feature;
    gms::feature _la_sstable_feature;
    gms::feature _stream_with_rpc_stream_feature;
    gms::feature _mc_sstable_feature;
    gms::feature _row_level_repair_feature;
    gms::feature _truncation_table;
    gms::feature _correct_static_compact_in_mc;
    gms::feature _unbounded_range_tombstones_feature;
    gms::feature _view_virtual_columns;
    gms::feature _digest_insensitive_to_expiry;
    gms::feature _computed_columns;
    gms::feature _cdc_feature;
    gms::feature _nonfrozen_udts;
    gms::feature _hinted_handoff_separate_connection;
    gms::feature _lwt_feature;
    gms::feature _per_table_partitioners_feature;

public:
    bool cluster_supports_range_tombstones() const {
        return bool(_range_tombstones_feature);
    }

    bool cluster_supports_large_partitions() const {
        return bool(_large_partitions_feature);
    }

    bool cluster_supports_materialized_views() const {
        return bool(_materialized_views_feature);
    }

    bool cluster_supports_counters() const {
        return bool(_counters_feature);
    }

    bool cluster_supports_indexes() const {
        return bool(_indexes_feature);
    }

    bool cluster_supports_digest_multipartition_reads() const {
        return bool(_digest_multipartition_read_feature);
    }

    bool cluster_supports_correct_counter_order() const {
        return bool(_correct_counter_order_feature);
    }

    const gms::feature& cluster_supports_schema_tables_v3() const {
        return _schema_tables_v3;
    }

    bool cluster_supports_reading_correctly_serialized_range_tombstones() const {
        return bool(_correct_non_compound_range_tombstones);
    }

    bool cluster_supports_write_failure_reply() const {
        return bool(_write_failure_reply_feature);
    }

    bool cluster_supports_xxhash_digest_algorithm() const {
        return bool(_xxhash_feature);
    }

    bool cluster_supports_user_defined_functions() const {
        return bool(_udf_feature);
    }

    bool cluster_supports_roles() const {
        return bool(_roles_feature);
    }

    const feature& cluster_supports_la_sstable() const {
        return _la_sstable_feature;
    }

    bool cluster_supports_stream_with_rpc_stream() const {
        return bool(_stream_with_rpc_stream_feature);
    }

    const feature& cluster_supports_mc_sstable() const {
        return _mc_sstable_feature;
    }

    const feature& cluster_supports_cdc() const {
        return _cdc_feature;
    }

    const feature& cluster_supports_per_table_partitioners() const {
        return _per_table_partitioners_feature;
    }

    bool cluster_supports_row_level_repair() const {
        return bool(_row_level_repair_feature);
    }
    feature& cluster_supports_truncation_table() {
        return _truncation_table;
    }
    const feature& cluster_supports_truncation_table() const {
        return _truncation_table;
    }
    const feature& cluster_supports_correct_static_compact_in_mc() const {
        return _correct_static_compact_in_mc;
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

    bool cluster_supports_computed_columns() const {
        return bool(_computed_columns);
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
};

} // namespace gms

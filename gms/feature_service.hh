/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

using namespace std::literals;

/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 *
 * A pointer to `cql3::query_processor` can be optionally supplied
 * if the instance needs to persist enabled features in a system table.
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
    future<> support(const std::string_view& name);
    void enable(const std::set<std::string_view>& list);
    db::schema_features cluster_schema_features() const;
    std::set<std::string_view> known_feature_set();
    std::set<std::string_view> supported_feature_set();

    // Key in the 'system.scylla_local' table, that is used to
    // persist enabled features
    static constexpr const char* ENABLED_FEATURES_KEY = "enabled_features";

private:
    gms::feature _udf_feature { *this, "UDF"sv };
    gms::feature _md_sstable_feature { *this, "MD_SSTABLE_FORMAT"sv };
    gms::feature _me_sstable_feature { *this, "ME_SSTABLE_FORMAT"sv };
    gms::feature _view_virtual_columns { *this, "VIEW_VIRTUAL_COLUMNS"sv };
    gms::feature _digest_insensitive_to_expiry { *this, "DIGEST_INSENSITIVE_TO_EXPIRY"sv };
    gms::feature _computed_columns { *this, "COMPUTED_COLUMNS"sv };
    gms::feature _cdc_feature { *this, "CDC"sv };
    gms::feature _nonfrozen_udts { *this, "NONFROZEN_UDTS"sv };
    gms::feature _hinted_handoff_separate_connection { *this, "HINTED_HANDOFF_SEPARATE_CONNECTION"sv };
    gms::feature _lwt_feature { *this, "LWT"sv };
    gms::feature _per_table_partitioners_feature { *this, "PER_TABLE_PARTITIONERS"sv };
    gms::feature _per_table_caching_feature { *this, "PER_TABLE_CACHING"sv };
    gms::feature _digest_for_null_values_feature { *this, "DIGEST_FOR_NULL_VALUES"sv };
    gms::feature _correct_idx_token_in_secondary_index_feature { *this, "CORRECT_IDX_TOKEN_IN_SECONDARY_INDEX"sv };
    gms::feature _alternator_streams_feature { *this, "ALTERNATOR_STREAMS"sv };
    gms::feature _alternator_ttl_feature { *this, "ALTERNATOR_TTL"sv };
    gms::feature _range_scan_data_variant { *this, "RANGE_SCAN_DATA_VARIANT"sv };
    gms::feature _cdc_generations_v2 { *this, "CDC_GENERATIONS_V2"sv };
    gms::feature _uda { *this, "UDA"sv };
    gms::feature _separate_page_size_and_safety_limit { *this, "SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT"sv };
    gms::feature _supports_raft_cluster_mgmt { *this, "SUPPORTS_RAFT_CLUSTER_MANAGEMENT"sv };
    gms::feature _uses_raft_cluster_mgmt { *this, "USES_RAFT_CLUSTER_MANAGEMENT"sv };
    gms::feature _tombstone_gc_options { *this, "TOMBSTONE_GC_OPTIONS"sv };
    gms::feature _parallelized_aggregation { *this, "PARALLELIZED_AGGREGATION"sv };
    gms::feature _keyspace_storage_options { *this, "KEYSPACE_STORAGE_OPTIONS"sv };

public:

    const std::unordered_map<sstring, std::reference_wrapper<feature>>& registered_features() const;

    bool cluster_supports_user_defined_functions() const {
        return bool(_udf_feature);
    }

    const feature& cluster_supports_md_sstable() const {
        return _md_sstable_feature;
    }

    const feature& cluster_supports_me_sstable() const {
        return _me_sstable_feature;
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

    bool cluster_supports_alternator_ttl() const {
        return bool(_alternator_ttl_feature);
    }

    // Range scans have a data variant, which produces query::result directly,
    // instead of through the intermediate reconcilable_result format.
    bool cluster_supports_range_scan_data_variant() const {
        return bool(_range_scan_data_variant);
    }

    bool cluster_supports_cdc_generations_v2() const {
        return bool(_cdc_generations_v2);
    }

    bool cluster_supports_user_defined_aggregates() const {
        return bool(_uda);
    }

    // Historically max_result_size contained only two fields: soft_limit and
    // hard_limit. It was somehow obscure because for normal paged queries both
    // fields were equal and meant page size. For unpaged queries and reversed
    // queries soft_limit was used to warn when the size of the result exceeded
    // the soft_limit and hard_limit was used to throw when the result was
    // bigger than this hard_limit. To clean things up, we introduced the third
    // field into max_result_size. It's name is page_size. Now page_size always
    // means the size of the page while soft and hard limits are just what their
    // names suggest. They are no longer interepreted as page size. This is not
    // a backwards compatible change so this new cluster feature is used to make
    // sure the whole cluster supports the new page_size field and we can safely
    // send it to replicas.
    bool cluster_supports_separate_page_size_and_safety_limit() const {
        return bool(_separate_page_size_and_safety_limit);
    }

    bool cluster_supports_tombstone_gc_options() const {
        return bool(_tombstone_gc_options);
    }

    bool cluster_supports_parallelized_aggregation() const {
        return bool(_parallelized_aggregation);
    }

    const feature& cluster_supports_raft_cluster_mgmt() const {
        return _supports_raft_cluster_mgmt;
    }

    bool cluster_uses_raft_cluster_mgmt() const {
        return static_cast<bool>(_uses_raft_cluster_mgmt);
    }

    bool cluster_supports_keyspace_storage_options() const {
        return bool(_keyspace_storage_options);
    }

    static std::set<sstring> to_feature_set(sstring features_string);
    // Persist enabled feature in the `system.scylla_local` table under the "enabled_features" key.
    // The key itself is maintained as an `unordered_set<string>` and serialized via `to_string`
    // function to preserve readability.
    void persist_enabled_feature_info(const gms::feature& f) const;
};

} // namespace gms

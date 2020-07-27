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
 *
 * Copyright (C) 2020 ScyllaDB
 */

#include <any>
#include <seastar/core/sstring.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include "log.hh"
#include "db/config.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"

namespace gms {
constexpr std::string_view features::RANGE_TOMBSTONES = "RANGE_TOMBSTONES";
constexpr std::string_view features::LARGE_PARTITIONS = "LARGE_PARTITIONS";
constexpr std::string_view features::MATERIALIZED_VIEWS = "MATERIALIZED_VIEWS";
constexpr std::string_view features::COUNTERS = "COUNTERS";
constexpr std::string_view features::INDEXES = "INDEXES";
constexpr std::string_view features::DIGEST_MULTIPARTITION_READ = "DIGEST_MULTIPARTITION_READ";
constexpr std::string_view features::CORRECT_COUNTER_ORDER = "CORRECT_COUNTER_ORDER";
constexpr std::string_view features::SCHEMA_TABLES_V3 = "SCHEMA_TABLES_V3";
constexpr std::string_view features::CORRECT_NON_COMPOUND_RANGE_TOMBSTONES = "CORRECT_NON_COMPOUND_RANGE_TOMBSTONES";
constexpr std::string_view features::WRITE_FAILURE_REPLY = "WRITE_FAILURE_REPLY";
constexpr std::string_view features::XXHASH = "XXHASH";
constexpr std::string_view features::UDF = "UDF";
constexpr std::string_view features::ROLES = "ROLES";
constexpr std::string_view features::LA_SSTABLE = "LA_SSTABLE_FORMAT";
constexpr std::string_view features::STREAM_WITH_RPC_STREAM = "STREAM_WITH_RPC_STREAM";
constexpr std::string_view features::MC_SSTABLE = "MC_SSTABLE_FORMAT";
constexpr std::string_view features::MD_SSTABLE = "MD_SSTABLE_FORMAT";
constexpr std::string_view features::ROW_LEVEL_REPAIR = "ROW_LEVEL_REPAIR";
constexpr std::string_view features::TRUNCATION_TABLE = "TRUNCATION_TABLE";
constexpr std::string_view features::CORRECT_STATIC_COMPACT_IN_MC = "CORRECT_STATIC_COMPACT_IN_MC";
constexpr std::string_view features::UNBOUNDED_RANGE_TOMBSTONES = "UNBOUNDED_RANGE_TOMBSTONES";
constexpr std::string_view features::VIEW_VIRTUAL_COLUMNS = "VIEW_VIRTUAL_COLUMNS";
constexpr std::string_view features::DIGEST_INSENSITIVE_TO_EXPIRY = "DIGEST_INSENSITIVE_TO_EXPIRY";
constexpr std::string_view features::COMPUTED_COLUMNS = "COMPUTED_COLUMNS";
constexpr std::string_view features::CDC = "CDC";
constexpr std::string_view features::NONFROZEN_UDTS = "NONFROZEN_UDTS";
constexpr std::string_view features::HINTED_HANDOFF_SEPARATE_CONNECTION = "HINTED_HANDOFF_SEPARATE_CONNECTION";
constexpr std::string_view features::LWT = "LWT";
constexpr std::string_view features::PER_TABLE_PARTITIONERS = "PER_TABLE_PARTITIONERS";
constexpr std::string_view features::PER_TABLE_CACHING = "PER_TABLE_CACHING";

static logging::logger logger("features");

feature_config::feature_config() {
}

feature_service::feature_service(feature_config cfg) : _config(cfg)
        , _range_tombstones_feature(*this, features::RANGE_TOMBSTONES)
        , _large_partitions_feature(*this, features::LARGE_PARTITIONS)
        , _materialized_views_feature(*this, features::MATERIALIZED_VIEWS)
        , _counters_feature(*this, features::COUNTERS)
        , _indexes_feature(*this, features::INDEXES)
        , _digest_multipartition_read_feature(*this, features::DIGEST_MULTIPARTITION_READ)
        , _correct_counter_order_feature(*this, features::CORRECT_COUNTER_ORDER)
        , _schema_tables_v3(*this, features::SCHEMA_TABLES_V3)
        , _correct_non_compound_range_tombstones(*this, features::CORRECT_NON_COMPOUND_RANGE_TOMBSTONES)
        , _write_failure_reply_feature(*this, features::WRITE_FAILURE_REPLY)
        , _xxhash_feature(*this, features::XXHASH)
        , _udf_feature(*this, features::UDF)
        , _roles_feature(*this, features::ROLES)
        , _stream_with_rpc_stream_feature(*this, features::STREAM_WITH_RPC_STREAM)
        , _mc_sstable_feature(*this, features::MC_SSTABLE)
        , _md_sstable_feature(*this, features::MD_SSTABLE)
        , _row_level_repair_feature(*this, features::ROW_LEVEL_REPAIR)
        , _truncation_table(*this, features::TRUNCATION_TABLE)
        , _correct_static_compact_in_mc(*this, features::CORRECT_STATIC_COMPACT_IN_MC)
        , _unbounded_range_tombstones_feature(*this, features::UNBOUNDED_RANGE_TOMBSTONES)
        , _view_virtual_columns(*this, features::VIEW_VIRTUAL_COLUMNS)
        , _digest_insensitive_to_expiry(*this, features::DIGEST_INSENSITIVE_TO_EXPIRY)
        , _computed_columns(*this, features::COMPUTED_COLUMNS)
        , _cdc_feature(*this, features::CDC)
        , _nonfrozen_udts(*this, features::NONFROZEN_UDTS)
        , _hinted_handoff_separate_connection(*this, features::HINTED_HANDOFF_SEPARATE_CONNECTION)
        , _lwt_feature(*this, features::LWT)
        , _per_table_partitioners_feature(*this, features::PER_TABLE_PARTITIONERS)
        , _per_table_caching_feature(*this, features::PER_TABLE_CACHING) {
}

feature_config feature_config_from_db_config(db::config& cfg, std::set<sstring> disabled) {
    feature_config fcfg;

    fcfg._masked_features.insert(sstring(gms::features::UNBOUNDED_RANGE_TOMBSTONES));

    fcfg._disabled_features = std::move(disabled);

    if (!cfg.enable_sstables_mc_format()) {
        if (cfg.enable_sstables_md_format()) {
            throw std::runtime_error(
                    "You must use both enable_sstables_mc_format and enable_sstables_md_format "
                    "to enable SSTables md format support");
        }
        fcfg._disabled_features.insert(sstring(gms::features::MC_SSTABLE));
    }
    if (!cfg.enable_sstables_md_format()) {
        fcfg._disabled_features.insert(sstring(gms::features::MD_SSTABLE));
    }
    if (!cfg.enable_user_defined_functions()) {
        fcfg._disabled_features.insert(sstring(gms::features::UDF));
    } else {
        if (!cfg.check_experimental(db::experimental_features_t::UDF)) {
            throw std::runtime_error(
                    "You must use both enable_user_defined_functions and experimental_features:udf "
                    "to enable user-defined functions");
        }
    }

    if (!cfg.check_experimental(db::experimental_features_t::CDC)) {
        fcfg._disabled_features.insert(sstring(gms::features::CDC));
    }

    return fcfg;
}

future<> feature_service::stop() {
    return make_ready_future<>();
}

void feature_service::register_feature(feature& f) {
    auto i = _registered_features.emplace(f.name(), f);
    assert(i.second);
}

void feature_service::unregister_feature(feature& f) {
    _registered_features.erase(f.name());
}


void feature_service::enable(const sstring& name) {
    if (auto it = _registered_features.find(name); it != _registered_features.end()) {
        auto&& f = it->second;
        f.get().enable();
    }
}

void feature_service::support(const std::string_view& name) {
    _config._masked_features.erase(sstring(name));
}

std::set<std::string_view> feature_service::known_feature_set() {
    // Add features known by this local node. When a new feature is
    // introduced in scylla, update it here, e.g.,
    // return sstring("FEATURE1,FEATURE2")
    std::set<std::string_view> features = {
        gms::features::RANGE_TOMBSTONES,
        gms::features::LARGE_PARTITIONS,
        gms::features::COUNTERS,
        gms::features::DIGEST_MULTIPARTITION_READ,
        gms::features::CORRECT_COUNTER_ORDER,
        gms::features::SCHEMA_TABLES_V3,
        gms::features::CORRECT_NON_COMPOUND_RANGE_TOMBSTONES,
        gms::features::WRITE_FAILURE_REPLY,
        gms::features::XXHASH,
        gms::features::ROLES,
        gms::features::LA_SSTABLE,
        gms::features::STREAM_WITH_RPC_STREAM,
        gms::features::MATERIALIZED_VIEWS,
        gms::features::INDEXES,
        gms::features::ROW_LEVEL_REPAIR,
        gms::features::TRUNCATION_TABLE,
        gms::features::CORRECT_STATIC_COMPACT_IN_MC,
        gms::features::VIEW_VIRTUAL_COLUMNS,
        gms::features::DIGEST_INSENSITIVE_TO_EXPIRY,
        gms::features::COMPUTED_COLUMNS,
        gms::features::NONFROZEN_UDTS,
        gms::features::UNBOUNDED_RANGE_TOMBSTONES,
        gms::features::HINTED_HANDOFF_SEPARATE_CONNECTION,
        gms::features::PER_TABLE_PARTITIONERS,
        gms::features::PER_TABLE_CACHING,
        gms::features::LWT,
        gms::features::MC_SSTABLE,
        gms::features::MD_SSTABLE,
        gms::features::UDF,
        gms::features::CDC,
    };

    for (const sstring& s : _config._disabled_features) {
        features.erase(s);
    }
    return features;
}

std::set<std::string_view> feature_service::supported_feature_set() {
    auto features = known_feature_set();

    for (const sstring& s : _config._masked_features) {
        features.erase(s);
    }
    return features;
}

feature::feature(feature_service& service, std::string_view name, bool enabled)
        : _service(&service)
        , _name(name)
        , _enabled(enabled) {
    _service->register_feature(*this);
}

feature::~feature() {
    if (_service) {
        _service->unregister_feature(*this);
    }
}

feature& feature::operator=(feature&& other) {
    _service->unregister_feature(*this);
    _service = std::exchange(other._service, nullptr);
    _name = other._name;
    _enabled = other._enabled;
    _s = std::move(other._s);
    _service->register_feature(*this);
    return *this;
}

void feature::enable() {
    if (!_enabled) {
        if (this_shard_id() == 0) {
            logger.info("Feature {} is enabled", name());
        }
        _enabled = true;
        _s();
    }
}

db::schema_features feature_service::cluster_schema_features() const {
    db::schema_features f;
    f.set_if<db::schema_feature::VIEW_VIRTUAL_COLUMNS>(bool(_view_virtual_columns));
    f.set_if<db::schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY>(bool(_digest_insensitive_to_expiry));
    f.set_if<db::schema_feature::COMPUTED_COLUMNS>(bool(_computed_columns));
    f.set_if<db::schema_feature::CDC_OPTIONS>(bool(_cdc_feature));
    f.set_if<db::schema_feature::PER_TABLE_PARTITIONERS>(bool(_per_table_partitioners_feature));
    return f;
}

void feature_service::enable(const std::set<std::string_view>& list) {
    for (gms::feature& f : {
        std::ref(_range_tombstones_feature),
        std::ref(_large_partitions_feature),
        std::ref(_materialized_views_feature),
        std::ref(_counters_feature),
        std::ref(_indexes_feature),
        std::ref(_digest_multipartition_read_feature),
        std::ref(_correct_counter_order_feature),
        std::ref(_schema_tables_v3),
        std::ref(_correct_non_compound_range_tombstones),
        std::ref(_write_failure_reply_feature),
        std::ref(_xxhash_feature),
        std::ref(_udf_feature),
        std::ref(_roles_feature),
        std::ref(_stream_with_rpc_stream_feature),
        std::ref(_mc_sstable_feature),
        std::ref(_md_sstable_feature),
        std::ref(_row_level_repair_feature),
        std::ref(_truncation_table),
        std::ref(_correct_static_compact_in_mc),
        std::ref(_unbounded_range_tombstones_feature),
        std::ref(_view_virtual_columns),
        std::ref(_digest_insensitive_to_expiry),
        std::ref(_computed_columns),
        std::ref(_cdc_feature),
        std::ref(_nonfrozen_udts),
        std::ref(_hinted_handoff_separate_connection),
        std::ref(_lwt_feature),
        std::ref(_per_table_partitioners_feature),
        std::ref(_per_table_caching_feature),
    })
    {
        if (list.count(f.name())) {
            f.enable();
        }
    }
}

} // namespace gms

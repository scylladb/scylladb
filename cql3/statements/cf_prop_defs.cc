/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/column_identifier.hh"
#include "cql3/statements/property_definitions.hh"
#include "cql3/statements/request_validations.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/extensions.hh"
#include "sstables/parquet/writer_impl.hh"
#include "sstables/parquet/encryption_keys.hh"
#include "db/tags/extension.hh"
#include "cdc/log.hh"
#include "cdc/cdc_extension.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"
#include "tombstone_gc_extension.hh"
#include "tombstone_gc.hh"
#include "db/per_partition_rate_limit_extension.hh"
#include "db/per_partition_rate_limit_options.hh"
#include "db/tablet_options.hh"
#include "utils/bloom_calculations.hh"
#include "utils/overloaded_functor.hh"
#include "db/config.hh"

#include <boost/algorithm/string/predicate.hpp>

namespace cql3 {

namespace statements {

const sstring cf_prop_defs::KW_COMMENT = "comment";
const sstring cf_prop_defs::KW_GCGRACESECONDS = "gc_grace_seconds";
const sstring cf_prop_defs::KW_PAXOSGRACESECONDS = "paxos_grace_seconds";
const sstring cf_prop_defs::KW_MINCOMPACTIONTHRESHOLD = "min_threshold";
const sstring cf_prop_defs::KW_MAXCOMPACTIONTHRESHOLD = "max_threshold";
const sstring cf_prop_defs::KW_CACHING = "caching";
const sstring cf_prop_defs::KW_DEFAULT_TIME_TO_LIVE = "default_time_to_live";
const sstring cf_prop_defs::KW_MIN_INDEX_INTERVAL = "min_index_interval";
const sstring cf_prop_defs::KW_MAX_INDEX_INTERVAL = "max_index_interval";
const sstring cf_prop_defs::KW_SPECULATIVE_RETRY = "speculative_retry";
const sstring cf_prop_defs::KW_BF_FP_CHANCE = "bloom_filter_fp_chance";
const sstring cf_prop_defs::KW_MEMTABLE_FLUSH_PERIOD = "memtable_flush_period_in_ms";
const sstring cf_prop_defs::KW_SYNCHRONOUS_UPDATES = "synchronous_updates";

const sstring cf_prop_defs::KW_COMPACTION = "compaction";
const sstring cf_prop_defs::KW_COMPRESSION = "compression";
const sstring cf_prop_defs::KW_CRC_CHECK_CHANCE = "crc_check_chance";

const sstring cf_prop_defs::KW_ID = "id";

const sstring cf_prop_defs::COMPACTION_STRATEGY_CLASS_KEY = "class";

const sstring cf_prop_defs::COMPACTION_ENABLED_KEY = "enabled";

const sstring cf_prop_defs::KW_TABLETS = "tablets";

const sstring cf_prop_defs::KW_STORAGE_ENGINE = "storage_engine";
const sstring cf_prop_defs::KW_STORAGE_FORMAT = "storage_format";
const sstring cf_prop_defs::KW_PARQUET = "parquet";
const sstring cf_prop_defs::KW_LARGE_DATA_GUARDRAILS_ENABLED = "large_data_guardrails_enabled";

schema::extensions_map cf_prop_defs::make_schema_extensions(const db::extensions& exts) const {
    schema::extensions_map er;
    for (auto& p : exts.schema_extensions()) {
        auto i = _properties.find(p.first);
        if (i != _properties.end()) {
            std::visit(overloaded_functor{
            [&](const sstring& v) {
                auto ep = p.second(v);
                if (ep) {
                    er.emplace(p.first, std::move(ep));
                }
            },
            [&](const property_definitions::extended_map_type& xmap) {
                auto ep = p.second(property_definitions::to_simple_map(std::move(xmap)));
                if (ep) {
                    er.emplace(p.first, std::move(ep));
                }
            }}, i->second);
        }
    }
    return er;
}

data_dictionary::keyspace cf_prop_defs::find_keyspace(const data_dictionary::database db, std::string_view ks_name) {
    try {
        return db.find_keyspace(ks_name);
    } catch (const data_dictionary::no_such_keyspace& e) {
        throw request_validations::invalid_request("{}", e.what());
    }
}

void cf_prop_defs::validate(const data_dictionary::database db, sstring ks_name, const schema::extensions_map& schema_extensions) const {
    // Skip validation if the comapction strategy class is already set as it means we've already
    // prepared (and redoing it would set strategyClass back to null, which we don't want)
    if (_compaction_strategy_class) {
        return;
    }

    const auto& ks = find_keyspace(db, ks_name);

    static std::set<sstring> keywords({
        KW_COMMENT,
        KW_GCGRACESECONDS, KW_CACHING, KW_DEFAULT_TIME_TO_LIVE,
        KW_MIN_INDEX_INTERVAL, KW_MAX_INDEX_INTERVAL, KW_SPECULATIVE_RETRY,
        KW_BF_FP_CHANCE, KW_MEMTABLE_FLUSH_PERIOD, KW_COMPACTION,
        KW_COMPRESSION, KW_CRC_CHECK_CHANCE,  KW_ID, KW_PAXOSGRACESECONDS,
        KW_SYNCHRONOUS_UPDATES, KW_TABLETS,
        KW_STORAGE_ENGINE,
        KW_STORAGE_FORMAT,
        KW_PARQUET,
        KW_LARGE_DATA_GUARDRAILS_ENABLED,
    });
    static std::set<sstring> obsolete_keywords({
        sstring("index_interval"),
        sstring("replicate_on_write"),
        sstring("populate_io_cache_on_flush"),
        sstring("read_repair_chance"),
        sstring("dclocal_read_repair_chance"),
    });

    const auto& exts = db.extensions();
    property_definitions::validate(keywords, exts.schema_extension_keywords(), obsolete_keywords);

    try {
        get_id();
    } catch(...) {
        std::throw_with_nested(exceptions::configuration_exception("Invalid table id"));
    }

    auto compaction_type_options = get_compaction_type_options();
    if (!compaction_type_options.empty()) {
        auto strategy = compaction_type_options.find(COMPACTION_STRATEGY_CLASS_KEY);
        if (strategy == compaction_type_options.end()) {
            throw exceptions::configuration_exception(sstring("Missing sub-option '") + COMPACTION_STRATEGY_CLASS_KEY + "' for the '" + KW_COMPACTION + "' option.");
        }
        _compaction_strategy_class = compaction::compaction_strategy::type(strategy->second);
        remove_from_map_if_exists(KW_COMPACTION, COMPACTION_STRATEGY_CLASS_KEY);

#if 0
       CFMetaData.validateCompactionOptions(compactionStrategyClass, compactionOptions);
#endif
    }

    auto compression_options = get_compression_options();
    if (compression_options && !compression_options->empty()) {
        auto sstable_compression_class = compression_options->find(sstring(compression_parameters::SSTABLE_COMPRESSION));
        if (sstable_compression_class == compression_options->end()) {
            throw exceptions::configuration_exception(sstring("Missing sub-option '") + compression_parameters::SSTABLE_COMPRESSION + "' for the '" + KW_COMPRESSION + "' option.");
        }
        compression_parameters cp(*compression_options);
        cp.validate(compression_parameters::dicts_feature_enabled(bool(db.features().sstable_compression_dicts)));
    }

    auto per_partition_rate_limit_options = get_per_partition_rate_limit_options(schema_extensions);
    if (per_partition_rate_limit_options && !db.features().typed_errors_in_read_rpc) {
        throw exceptions::configuration_exception("Per-partition rate limit is not supported yet by the whole cluster");
    }

    auto tombstone_gc_options = get_tombstone_gc_options(schema_extensions);
    validate_tombstone_gc_options(tombstone_gc_options, db, ks_name);

    // Parquet encryption and whole-component encryption at rest are mutually exclusive, and this
    // is the only place both are visible at once.
    //
    // They are not merely redundant. `scylla_encryption_options` installs an sstable file-io
    // extension that encrypts the entire Data component, so a pq table carrying it would be
    // encrypted twice -- and, far worse, the outer layer makes the file opaque to every reader
    // that is not Scylla. Being openable by an authorised external reader is the entire reason
    // for encrypting *inside* the Parquet format rather than underneath it, so silently accepting
    // both would take away the feature while appearing to add security. Both keys still come from
    // the same providers; it is the property that has to be chosen.
    if (auto opts = get_map(KW_PARQUET)) {
        const sstables::parquet::parquet_parameters pp{*opts};
        if (pp.encryption_enabled() && schema_extensions.contains("scylla_encryption_options")) {
            throw exceptions::configuration_exception(
                    "A table cannot set both scylla_encryption_options and parquet = "
                    "{'encryption': ...}: the first encrypts the whole sstable component, which "
                    "would encrypt the Parquet file a second time and leave a file no external "
                    "reader can open -- which is what encrypting inside the format exists to "
                    "avoid. Pick one; both take their keys from the same key providers");
        }
    }

    validate_minimum_int(KW_DEFAULT_TIME_TO_LIVE, 0, DEFAULT_DEFAULT_TIME_TO_LIVE);
    validate_minimum_int(KW_PAXOSGRACESECONDS, 0, DEFAULT_GC_GRACE_SECONDS);

    auto min_index_interval = get_int(KW_MIN_INDEX_INTERVAL, DEFAULT_MIN_INDEX_INTERVAL);
    auto max_index_interval = get_int(KW_MAX_INDEX_INTERVAL, DEFAULT_MAX_INDEX_INTERVAL);
    if (min_index_interval < 1) {
        throw exceptions::configuration_exception(KW_MIN_INDEX_INTERVAL + " must be greater than 0");
    }
    if (max_index_interval < min_index_interval) {
        throw exceptions::configuration_exception(KW_MAX_INDEX_INTERVAL + " must be greater than " + KW_MIN_INDEX_INTERVAL);
    }

    if (get_simple(KW_BF_FP_CHANCE)) {
        double bloom_filter_fp_chance = get_double(KW_BF_FP_CHANCE, 0/*not used*/);
        double min_bloom_filter_fp_chance = utils::bloom_calculations::min_supported_bloom_filter_fp_chance();
        if (bloom_filter_fp_chance <= min_bloom_filter_fp_chance || bloom_filter_fp_chance > 1.0) {
            throw exceptions::configuration_exception(format(
                "{} must be larger than {} and less than or equal to 1.0 (got {})",
                KW_BF_FP_CHANCE, min_bloom_filter_fp_chance, bloom_filter_fp_chance));
        }
    }

    auto memtable_flush_period = get_int(KW_MEMTABLE_FLUSH_PERIOD, DEFAULT_MEMTABLE_FLUSH_PERIOD);
    if (memtable_flush_period != 0 && memtable_flush_period < DEFAULT_MEMTABLE_FLUSH_PERIOD_MIN_VALUE) {
        throw exceptions::configuration_exception(format(
            "{} must be 0 or greater than {}",
            KW_MEMTABLE_FLUSH_PERIOD, DEFAULT_MEMTABLE_FLUSH_PERIOD_MIN_VALUE));
    }

    speculative_retry::from_sstring(get_string(KW_SPECULATIVE_RETRY, speculative_retry(speculative_retry::type::NONE, 0).to_sstring()));

    if (auto tablet_options_map = get_tablet_options()) {
        if (!ks.uses_tablets()) {
            throw exceptions::configuration_exception("tablet options cannot be used when tablets are disabled for the keyspace");
        }
        if (!db.features().tablet_options) {
            throw exceptions::configuration_exception("tablet options cannot be used until all nodes in the cluster enable this feature");
        }
        db::tablet_options::validate(*tablet_options_map, db.features());
    }

    if (has_property(KW_PARQUET)) {
        // The same gate as KW_STORAGE_FORMAT below, for the same reason: the options land in a
        // schema cell that a node without the feature cannot read, so publishing them mid-upgrade
        // splits the schema. The map can be set without storage_format (options first, format
        // later), so gating only the format property is not enough.
        if (!db.features().parquet_sstable_format) {
            throw exceptions::configuration_exception(format(
                "Cannot set '{}': requires all nodes to support the "
                "PARQUET_SSTABLE_FORMAT cluster feature", KW_PARQUET));
        }
        // Constructing it is the validation: parquet_parameters rejects unknown
        // sub-options, out-of-range values, and anything the writer cannot honour, so a
        // bad value is a configuration error here rather than a surprise at write time.
        if (auto opts = get_map(KW_PARQUET)) {
            // Names, values and ranges. Whether an `encoding.<column>` override *applies to that
            // column's type* is checked in apply_to_builder(), which is the first point that knows
            // the columns; validate() sees only the keyspace.
            (void) sstables::parquet::parquet_parameters{*opts};
        }
    }

    if (has_property(KW_STORAGE_FORMAT)) {
        auto sf = get_string(KW_STORAGE_FORMAT, "");
        if (sf != "sstable" && sf != "parquet" && sf != "hybrid") {
            throw exceptions::configuration_exception(format(
                "Invalid value '{}' for '{}'; expected one of: sstable, parquet, hybrid",
                sf, KW_STORAGE_FORMAT));
        }
        // Setting a non-default format has to wait for every node to understand
        // it. Until then a node that does not know the property would keep
        // writing the native format while others did not, and the schema cell
        // itself changes the digest -- see the PARQUET_SSTABLE_FORMAT gate in
        // db/schema_tables.cc.
        if (sf != "sstable" && !db.features().parquet_sstable_format) {
            throw exceptions::configuration_exception(format(
                "Cannot set '{}' to '{}': requires all nodes to support the "
                "PARQUET_SSTABLE_FORMAT cluster feature", KW_STORAGE_FORMAT, sf));
        }
    }
    if (has_property(KW_STORAGE_ENGINE)) {
        auto storage_engine = get_string(KW_STORAGE_ENGINE, "");
        if (storage_engine == "logstor") {
            if (!db.features().logstor) {
                throw exceptions::configuration_exception(format("The experimental feature 'logstor' must be enabled in order to use the 'logstor' storage engine."));
            }
        } else {
            throw exceptions::configuration_exception(format("Illegal value for '{}'", KW_STORAGE_ENGINE));
        }
    }

    if (has_property(KW_LARGE_DATA_GUARDRAILS_ENABLED) && get_boolean(KW_LARGE_DATA_GUARDRAILS_ENABLED, false)) {
        if (!db.features().large_data_guardrails) {
            throw exceptions::configuration_exception("large_data_guardrails_enabled cannot be used until all nodes in the cluster enable this feature");
        }
    }
}

std::map<sstring, sstring> cf_prop_defs::get_compaction_type_options() const {
    auto compaction_type_options = get_map(KW_COMPACTION);
    if (compaction_type_options ) {
        return compaction_type_options.value();
    }
    return std::map<sstring, sstring>{};
}

std::optional<std::map<sstring, sstring>> cf_prop_defs::get_compression_options() const {
    auto compression_options = get_map(KW_COMPRESSION);
    if (compression_options) {
        return { compression_options.value() };
    }
    return { };
}

int32_t cf_prop_defs::get_default_time_to_live() const
{
    return get_int(KW_DEFAULT_TIME_TO_LIVE, 0);
}

int32_t cf_prop_defs::get_gc_grace_seconds() const
{
    return get_int(KW_GCGRACESECONDS, DEFAULT_GC_GRACE_SECONDS);
}

bool cf_prop_defs::get_synchronous_updates_flag() const {
    return get_boolean(KW_SYNCHRONOUS_UPDATES, false);
}

int32_t cf_prop_defs::get_paxos_grace_seconds() const {
    return get_int(KW_PAXOSGRACESECONDS, DEFAULT_GC_GRACE_SECONDS);
}

std::optional<table_id> cf_prop_defs::get_id() const {
    auto id = get_simple(KW_ID);
    if (id) {
        return std::make_optional<table_id>(utils::UUID(*id));
    }

    return std::nullopt;
}

std::optional<caching_options> cf_prop_defs::get_caching_options() const {
    auto value = get(KW_CACHING);
    if (!value) {
        return {};
    }
    return std::visit(make_visitor(
        [] (const property_definitions::extended_map_type& map) {
            return map.empty() ? std::nullopt : std::optional<caching_options>(caching_options::from_map(to_simple_map(map)));
        },
        [] (const sstring& str) {
            return std::optional<caching_options>(caching_options::from_sstring(str));
        }
    ), *value);
}

const cdc::options* cf_prop_defs::get_cdc_options(const schema::extensions_map& schema_exts) const {
    auto ext = get_schema_extension<cdc::cdc_extension>(schema_exts, cdc::cdc_extension::NAME);
    return ext ? &ext->get_options() : nullptr;
}

const tombstone_gc_options* cf_prop_defs::get_tombstone_gc_options(const schema::extensions_map& schema_exts) const {
    auto ext = get_schema_extension<tombstone_gc_extension>(schema_exts, tombstone_gc_extension::NAME);
    return ext ? &ext->get_options() : nullptr;
}

const db::per_partition_rate_limit_options* cf_prop_defs::get_per_partition_rate_limit_options(const schema::extensions_map& schema_exts) const {
    auto ext = get_schema_extension<db::per_partition_rate_limit_extension>(schema_exts, db::per_partition_rate_limit_extension::NAME);
    return ext ? &ext->get_options() : nullptr;
}

std::optional<db::tablet_options::map_type> cf_prop_defs::get_tablet_options() const {
    if (auto tablet_options = get_map(KW_TABLETS)) {
        return tablet_options.value();
    }
    return std::nullopt;
}

void cf_prop_defs::apply_to_builder(schema_builder& builder, schema::extensions_map schema_extensions, const data_dictionary::database& db, sstring ks_name, bool supports_repair) const {
    if (has_property(KW_COMMENT)) {
        builder.set_comment(get_string(KW_COMMENT, ""));
    }

    if (has_property(KW_GCGRACESECONDS)) {
        builder.set_gc_grace_seconds(get_int(KW_GCGRACESECONDS, builder.get_gc_grace_seconds()));
    }

    if (has_property(KW_PAXOSGRACESECONDS)) {
        builder.set_paxos_grace_seconds(get_paxos_grace_seconds());
    }

    std::optional<sstring> tmp_value = {};
    if (has_property(KW_COMPACTION)) {
        if (get_compaction_type_options().contains(KW_MINCOMPACTIONTHRESHOLD)) {
            tmp_value = get_compaction_type_options().at(KW_MINCOMPACTIONTHRESHOLD);
        }
    }
    int min_compaction_threshold = to_int(KW_MINCOMPACTIONTHRESHOLD, tmp_value, builder.get_min_compaction_threshold());

    tmp_value = {};
    if (has_property(KW_COMPACTION)) {
        if (get_compaction_type_options().contains(KW_MAXCOMPACTIONTHRESHOLD)) {
            tmp_value = get_compaction_type_options().at(KW_MAXCOMPACTIONTHRESHOLD);
        }
    }
    int max_compaction_threshold = to_int(KW_MAXCOMPACTIONTHRESHOLD, tmp_value, builder.get_max_compaction_threshold());

    if (min_compaction_threshold <= 0 || max_compaction_threshold <= 0)
        throw exceptions::configuration_exception("Disabling compaction by setting compaction thresholds to 0 has been deprecated, set the compaction option 'enabled' to false instead.");
    builder.set_min_compaction_threshold(min_compaction_threshold);
    builder.set_max_compaction_threshold(max_compaction_threshold);

    if (has_property(KW_COMPACTION)) {
        if (get_compaction_type_options().contains(COMPACTION_ENABLED_KEY)) {
            auto enabled = boost::algorithm::iequals(get_compaction_type_options().at(COMPACTION_ENABLED_KEY), "true");
            builder.set_compaction_enabled(enabled);
        }
    }

    if (has_property(KW_DEFAULT_TIME_TO_LIVE)) {
        builder.set_default_time_to_live(gc_clock::duration(get_int(KW_DEFAULT_TIME_TO_LIVE, DEFAULT_DEFAULT_TIME_TO_LIVE)));
    }

    if (has_property(KW_SPECULATIVE_RETRY)) {
        builder.set_speculative_retry(get_string(KW_SPECULATIVE_RETRY, builder.get_speculative_retry().to_sstring()));
    }

    if (has_property(KW_MEMTABLE_FLUSH_PERIOD)) {
        builder.set_memtable_flush_period(get_int(KW_MEMTABLE_FLUSH_PERIOD, builder.get_memtable_flush_period()));
    }

    if (has_property(KW_MIN_INDEX_INTERVAL)) {
        builder.set_min_index_interval(get_int(KW_MIN_INDEX_INTERVAL, builder.get_min_index_interval()));
    }

    if (has_property(KW_MAX_INDEX_INTERVAL)) {
        builder.set_max_index_interval(get_int(KW_MAX_INDEX_INTERVAL, builder.get_max_index_interval()));
    }

    if (_compaction_strategy_class) {
        builder.set_compaction_strategy(*_compaction_strategy_class);
        builder.set_compaction_strategy_options(get_compaction_type_options());
    }

    builder.set_bloom_filter_fp_chance(get_double(KW_BF_FP_CHANCE, builder.get_bloom_filter_fp_chance()));
    auto compression_options = get_compression_options();
    if (compression_options) {
        builder.set_compressor_params(compression_parameters(*compression_options));
    }

    auto caching_options = get_caching_options();
    if (caching_options) {
        builder.set_caching_options(std::move(*caching_options));
    }

    // for extensions that are not altered, keep the old ones
    auto& old_exts = builder.get_extensions();
    for (auto& [key, ext] : old_exts) {
        if (!_properties.count(key)) {
            schema_extensions.emplace(key, ext);
        }
    }
    // Set default tombstone_gc mode.
    if (!schema_extensions.contains(tombstone_gc_extension::NAME)) {
        auto ext = seastar::make_shared<tombstone_gc_extension>(get_default_tombstone_gc_mode(db, ks_name, supports_repair));
        schema_extensions.emplace(tombstone_gc_extension::NAME, std::move(ext));
    }
    builder.set_extensions(std::move(schema_extensions));

    if (has_property(KW_SYNCHRONOUS_UPDATES)) {
        bool is_synchronous = get_synchronous_updates_flag();
        std::map<sstring, sstring> tags_map = {
            {db::SYNCHRONOUS_VIEW_UPDATES_TAG_KEY, is_synchronous ? "true" : "false"}
        };

        builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(tags_map));
    }

    if (auto tablet_options_opt = get_map(KW_TABLETS)) {
        builder.set_tablet_options(std::move(*tablet_options_opt));
    }

    if (auto opts = get_map(KW_PARQUET)) {
        // An `encoding.<column>` override names a column and an encoding, and only some encodings
        // apply to a given type: DELTA_BINARY_PACKED on text, or BYTE_STREAM_SPLIT on anything but a
        // double, cannot be honoured. Both failure modes are bad in different ways -- an encoding the
        // writer silently ignores is a setting that lies, and one it fails on takes the table down
        // long after the DDL was accepted -- so it is rejected here, where the columns are known.
        //
        // A misspelled column name is caught for the same reason: left alone it would be inert, and
        // an inert performance setting is one nobody notices is doing nothing.
        const sstables::parquet::parquet_parameters pp{*opts};
        for (const auto& [col, enc] : pp.column_encodings()) {
            const cql3::column_identifier id{col, true};
            if (!builder.has_column(id)) {
                throw exceptions::configuration_exception(seastar::format(
                        "The 'parquet' option sets an encoding for column '{}', which this table does "
                        "not have", col));
            }
            const auto& cdef = builder.find_column(id);
            const auto ct = sstables::parquet::cql_type_of(*cdef.type);
            if (!sstables::parquet::parquet_parameters::applies_to(enc, ct)) {
                throw exceptions::configuration_exception(seastar::format(
                        "Encoding '{}' does not apply to column '{}' of type {}",
                        sstables::parquet::parquet_parameters::to_string(enc), col,
                        cdef.type->name()));
            }
        }
        // A per-column encryption key names a column too, and here the stakes are higher than for
        // an encoding: an `encryption_key.<column>` that named nothing would leave the operator
        // believing a column has its own key when it is in fact encrypted under the table's, which
        // is a security claim that is simply false. So a bad name is an error, never inert.
        for (const auto& [col, kopts] : pp.column_key_opts()) {
            const cql3::column_identifier id{col, true};
            if (!builder.has_column(id)) {
                throw exceptions::configuration_exception(seastar::format(
                        "The 'parquet' option sets an encryption key for column '{}', which this "
                        "table does not have", col));
            }
            const auto& cdef = builder.find_column(id);
            // The name-based restrictions are already checked in parquet_parameters (they hold
            // wherever the property is parsed); this is the one that needs the column's type.
            if (auto why = sstables::parquet::parquet_parameters::keyable_column_error(
                        col, cdef.type->is_multi_cell())) {
                throw exceptions::configuration_exception(seastar::format(
                        "The 'parquet' option asks for a per-column encryption key, but {}", *why));
            }
        }
        // The remaining encryption checks -- an algorithm the format cannot honour, provider
        // options with encryption off -- are in parquet_parameters itself, above, because they
        // must hold everywhere the property is parsed and not only where a DDL statement created
        // it. Whether the provider can actually be *reached* is checked in validate(), which is
        // allowed to block.
        builder.set_parquet_options(*opts);
    }
    if (has_property(KW_STORAGE_FORMAT)) {
        builder.set_storage_format(
                sstring_to_storage_format_type(get_string(KW_STORAGE_FORMAT, "sstable")));
    }
    if (has_property(KW_STORAGE_ENGINE)) {
        auto storage_engine = get_string(KW_STORAGE_ENGINE, "");
        if (storage_engine == "logstor") {
            builder.set_logstor();
        }
    }
    if (has_property(KW_LARGE_DATA_GUARDRAILS_ENABLED)) {
        builder.set_large_data_guardrails_enabled(get_boolean(KW_LARGE_DATA_GUARDRAILS_ENABLED, false));
    }
}

void cf_prop_defs::validate_minimum_int(const sstring& field, int32_t minimum_value, int32_t default_value) const
{
    auto val = get_int(field, default_value);
    if (val < minimum_value) {
        throw exceptions::configuration_exception(format("{} cannot be smaller than {}, (default {})",
                                                         field, minimum_value, default_value));
    }
}

std::optional<compaction::compaction_strategy_type> cf_prop_defs::get_compaction_strategy_class() const {
    // Unfortunately, in our implementation, the compaction strategy begins
    // stored in the compaction strategy options, and then the validate()
    // functions moves it into _compaction_strategy_class... If we want a
    // function that works either before or after validate(), we need to
    // check both places.
    if (_compaction_strategy_class) {
        return _compaction_strategy_class;
    }
    auto compaction_type_options = get_compaction_type_options();
    auto strategy = compaction_type_options.find(COMPACTION_STRATEGY_CLASS_KEY);
    if (strategy != compaction_type_options.end()) {
        return compaction::compaction_strategy::type(strategy->second);
    }
    return std::nullopt;
}

}

}

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/property_definitions.hh"
 
#include "schema/schema_builder.hh"
#include "compaction/compaction_strategy.hh"
#include "utils/UUID.hh"

namespace data_dictionary {
class database;
}

class tombstone_gc_options;

namespace db {
class extensions;
}
namespace cdc {
class options;
}

namespace cql3 {

namespace statements {

class cf_prop_defs : public property_definitions {
public:
    static const sstring KW_COMMENT;
    static const sstring KW_GCGRACESECONDS;
    static const sstring KW_PAXOSGRACESECONDS;
    static const sstring KW_MINCOMPACTIONTHRESHOLD;
    static const sstring KW_MAXCOMPACTIONTHRESHOLD;
    static const sstring KW_CACHING;
    static const sstring KW_DEFAULT_TIME_TO_LIVE;
    static const sstring KW_MIN_INDEX_INTERVAL;
    static const sstring KW_MAX_INDEX_INTERVAL;
    static const sstring KW_SPECULATIVE_RETRY;
    static const sstring KW_BF_FP_CHANCE;
    static const sstring KW_MEMTABLE_FLUSH_PERIOD;
    static const sstring KW_SYNCHRONOUS_UPDATES;

    static const sstring KW_COMPACTION;
    static const sstring KW_COMPRESSION;
    static const sstring KW_CRC_CHECK_CHANCE;

    static const sstring KW_ID;

    static const sstring KW_CDC;

    static const sstring COMPACTION_STRATEGY_CLASS_KEY;
    static const sstring COMPACTION_ENABLED_KEY;

    // FIXME: In origin the following consts are in CFMetaData.
    static constexpr int32_t DEFAULT_DEFAULT_TIME_TO_LIVE = 0;
    static constexpr int32_t DEFAULT_MIN_INDEX_INTERVAL = 128;
    static constexpr int32_t DEFAULT_MAX_INDEX_INTERVAL = 2048;

private:
    mutable std::optional<sstables::compaction_strategy_type> _compaction_strategy_class;
public:
    std::optional<sstables::compaction_strategy_type> get_compaction_strategy_class() const;

    schema::extensions_map make_schema_extensions(const db::extensions& exts) const;
    void validate(const data_dictionary::database db, sstring ks_name, const schema::extensions_map& schema_extensions) const;
    std::map<sstring, sstring> get_compaction_type_options() const;
    std::optional<std::map<sstring, sstring>> get_compression_options() const;
    const cdc::options* get_cdc_options(const schema::extensions_map&) const;
    std::optional<caching_options> get_caching_options() const;
    const tombstone_gc_options* get_tombstone_gc_options(const schema::extensions_map&) const;
    const db::per_partition_rate_limit_options* get_per_partition_rate_limit_options(const schema::extensions_map&) const;
#if 0
    public CachingOptions getCachingOptions() throws SyntaxException, ConfigurationException
    {
        CachingOptions options = null;
        Object val = properties.get(KW_CACHING);
        if (val == null)
            return null;
        else if (val instanceof Map)
            options = CachingOptions.fromMap(getMap(KW_CACHING));
        else if (val instanceof String) // legacy syntax
        {
            options = CachingOptions.fromString(getSimple(KW_CACHING));
            logger.warn("Setting caching options with deprecated syntax.");
        }
        return options;
    }
#endif
    int32_t get_default_time_to_live() const;
    int32_t get_gc_grace_seconds() const;
    int32_t get_paxos_grace_seconds() const;
    std::optional<table_id> get_id() const;
    bool get_synchronous_updates_flag() const;

    void apply_to_builder(schema_builder& builder, schema::extensions_map schema_extensions, const data_dictionary::database& db, sstring ks_name) const;
    void validate_minimum_int(const sstring& field, int32_t minimum_value, int32_t default_value) const;
};

}

}

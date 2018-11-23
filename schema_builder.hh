/*
 * Copyright (C) 2015 ScyllaDB
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

#include "schema.hh"
#include "database_fwd.hh"

struct schema_builder {
public:
    enum class compact_storage { no, yes };
private:
    schema::raw_schema _raw;
    std::experimental::optional<compact_storage> _compact_storage;
    std::experimental::optional<table_schema_version> _version;
    std::experimental::optional<raw_view_info> _view_info;
    schema_builder(const schema::raw_schema&);
public:
    schema_builder(const sstring& ks_name, const sstring& cf_name,
            std::experimental::optional<utils::UUID> = { },
            data_type regular_column_name_type = utf8_type);
    schema_builder(const schema_ptr);

    schema_builder& set_uuid(const utils::UUID& id) {
        _raw._id = id;
        return *this;
    }
    const utils::UUID& uuid() const {
        return _raw._id;
    }
    schema_builder& set_regular_column_name_type(const data_type& t) {
        _raw._regular_column_name_type = t;
        return *this;
    }
    schema_builder& set_default_validation_class(const data_type& t) {
        _raw._default_validation_class = t;
        return *this;
    }
    const data_type& regular_column_name_type() const {
        return _raw._regular_column_name_type;
    }
    const sstring& ks_name() const {
        return _raw._ks_name;
    }
    const sstring& cf_name() const {
        return _raw._cf_name;
    }
    schema_builder& set_comment(const sstring& s) {
        _raw._comment = s;
        return *this;
    }
    const sstring& comment() const {
        return _raw._comment;
    }
    schema_builder& set_default_time_to_live(gc_clock::duration t) {
        _raw._default_time_to_live = t;
        return *this;
    }
    gc_clock::duration default_time_to_live() const {
        return _raw._default_time_to_live;
    }

    schema_builder& set_gc_grace_seconds(int32_t gc_grace_seconds) {
        _raw._gc_grace_seconds = gc_grace_seconds;
        return *this;
    }

    int32_t get_gc_grace_seconds() const {
        return _raw._gc_grace_seconds;
    }

    schema_builder& set_dc_local_read_repair_chance(double chance) {
        _raw._dc_local_read_repair_chance = chance;
        return *this;
    }

    double get_dc_local_read_repair_chance() const {
        return _raw._dc_local_read_repair_chance;
    }

    schema_builder& set_read_repair_chance(double chance) {
        _raw._read_repair_chance = chance;
        return *this;
    }

    double get_read_repair_chance() const {
        return _raw._read_repair_chance;
    }

    schema_builder& set_crc_check_chance(double chance) {
        _raw._crc_check_chance = chance;
        return *this;
    }

    double get_crc_check_chance() const {
        return _raw._crc_check_chance;
    }

    schema_builder& set_min_compaction_threshold(int32_t t) {
        _raw._min_compaction_threshold = t;
        return *this;
    }

    int32_t get_min_compaction_threshold() const {
        return _raw._min_compaction_threshold;
    }

    schema_builder& set_max_compaction_threshold(int32_t t) {
        _raw._max_compaction_threshold = t;
        return *this;
    }

    int32_t get_max_compaction_threshold() const {
        return _raw._max_compaction_threshold;
    }

    schema_builder& set_compaction_enabled(bool enabled) {
        _raw._compaction_enabled = enabled;
        return *this;
    }

    bool compaction_enabled() const {
        return _raw._compaction_enabled;
    }

    schema_builder& set_min_index_interval(int32_t t) {
        _raw._min_index_interval = t;
        return *this;
    }

    int32_t get_min_index_interval() const {
        return _raw._min_index_interval;
    }

    schema_builder& set_max_index_interval(int32_t t) {
        _raw._max_index_interval = t;
        return *this;
    }

    int32_t get_max_index_interval() const {
        return _raw._max_index_interval;
    }

    schema_builder& set_memtable_flush_period(int32_t t) {
        _raw._memtable_flush_period = t;
        return *this;
    }

    int32_t get_memtable_flush_period() const {
        return _raw._memtable_flush_period;
    }

    schema_builder& set_speculative_retry(sstring retry_sstring) {
        _raw._speculative_retry = speculative_retry::from_sstring(retry_sstring);
        return *this;
    }

    const speculative_retry& get_speculative_retry() const {
        return _raw._speculative_retry;
    }

    schema_builder& set_bloom_filter_fp_chance(double fp) {
        _raw._bloom_filter_fp_chance = fp;
        return *this;
    }
    double get_bloom_filter_fp_chance() const {
        return _raw._bloom_filter_fp_chance;
    }
    schema_builder& set_compressor_params(const compression_parameters& cp) {
        _raw._compressor_params = cp;
        return *this;
    }
    schema_builder& set_extensions(schema::extensions_map exts) {
        _raw._extensions = std::move(exts);
        return *this;
    }
    schema_builder& set_compaction_strategy(sstables::compaction_strategy_type type) {
        _raw._compaction_strategy = type;
        return *this;
    }

    schema_builder& set_compaction_strategy_options(std::map<sstring, sstring> options) {
        _raw._compaction_strategy_options = std::move(options);
        return *this;
    }

    schema_builder& set_caching_options(caching_options c) {
        _raw._caching_options = std::move(c);
        return *this;
    }

    schema_builder& set_is_dense(bool is_dense) {
        _raw._is_dense = is_dense;
        return *this;
    }

    schema_builder& set_is_compound(bool is_compound) {
        _raw._is_compound = is_compound;
        return *this;
    }

    schema_builder& set_is_counter(bool is_counter) {
        _raw._is_counter = is_counter;
        return *this;
    }

    class default_names {
    public:
        default_names(const schema_builder&);
        default_names(const schema::raw_schema&);

        sstring partition_key_name();
        sstring clustering_name();
        sstring compact_value_name();
    private:
        sstring unique_name(const sstring&, size_t&, size_t) const;
        const schema::raw_schema& _raw;
        size_t _partition_index, _clustering_index, _compact_index;
    };

    column_definition& find_column(const cql3::column_identifier&);
    schema_builder& with_column(const column_definition& c);
    schema_builder& with_column(bytes name, data_type type, column_kind kind = column_kind::regular_column, column_view_virtual view_virtual = column_view_virtual::no);
    schema_builder& with_column(bytes name, data_type type, column_kind kind, column_id component_index, column_view_virtual view_virtual = column_view_virtual::no);
    schema_builder& remove_column(bytes name);
    schema_builder& without_column(sstring name, api::timestamp_type timestamp);
    schema_builder& without_column(sstring name, data_type, api::timestamp_type timestamp);
    schema_builder& rename_column(bytes from, bytes to);
    schema_builder& alter_column_type(bytes name, data_type new_type);

    // Adds information about collection that existed in the past but the column
    // has since been removed. For adding colllections that are still alive
    // use with_column().
    schema_builder& with_collection(bytes name, data_type type);

    schema_builder& with(compact_storage);
    schema_builder& with_version(table_schema_version);

    schema_builder& with_view_info(utils::UUID base_id, sstring base_name, bool include_all_columns, sstring where_clause);
    schema_builder& with_view_info(const schema& base_schema, bool include_all_columns, sstring where_clause) {
        return with_view_info(base_schema.id(), base_schema.cf_name(), include_all_columns, where_clause);
    }

    schema_builder& with_index(const index_metadata& im);
    schema_builder& without_index(const sstring& name);
    schema_builder& without_indexes();

    default_names get_default_names() const {
        return default_names(_raw);
    }

    // Equivalent to with(cp).build()
    schema_ptr build(compact_storage cp);

    schema_ptr build();
private:
    friend class default_names;
    void prepare_dense_schema(schema::raw_schema& raw);
};

/*
 * Copyright 2015 Cloudius Systems
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

    schema_builder& set_default_validator(const data_type& validator) {
        _raw._default_validator = validator;
        return *this;
    }

    schema_builder& set_gc_grace_seconds(int32_t gc_grace_seconds) {
        _raw._gc_grace_seconds = gc_grace_seconds;
        return *this;
    }

    int32_t get_gc_grace_seconds() {
        return _raw._gc_grace_seconds;
    }

    schema_builder& set_dc_local_read_repair_chance(double chance) {
        _raw._dc_local_read_repair_chance = chance;
        return *this;
    }

    double get_dc_local_read_repair_chance() {
        return _raw._dc_local_read_repair_chance;
    }

    schema_builder& set_read_repair_chance(double chance) {
        _raw._read_repair_chance = chance;
        return *this;
    }

    double get_read_repair_chance() {
        return _raw._read_repair_chance;
    }

    schema_builder& set_min_compaction_threshold(int32_t t) {
        _raw._min_compaction_threshold = t;
        return *this;
    }

    int32_t get_min_compaction_threshold() {
        return _raw._min_compaction_threshold;
    }

    schema_builder& set_max_compaction_threshold(int32_t t) {
        _raw._max_compaction_threshold = t;
        return *this;
    }

    int32_t get_max_compaction_threshold() {
        return _raw._max_compaction_threshold;
    }

    schema_builder& set_min_index_interval(int32_t t) {
        _raw._min_index_interval = t;
        return *this;
    }

    int32_t get_min_index_interval() {
        return _raw._min_index_interval;
    }

    schema_builder& set_max_index_interval(int32_t t) {
        _raw._max_index_interval = t;
        return *this;
    }

    int32_t get_max_index_interval() {
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

    const speculative_retry& get_speculative_retry() {
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

    column_definition& find_column(const cql3::column_identifier&);
    schema_builder& with_column(const column_definition& c);
    schema_builder& with_column(bytes name, data_type type, column_kind kind = column_kind::regular_column);
    schema_builder& with_column(bytes name, data_type type, index_info info, column_kind kind = column_kind::regular_column);
    schema_builder& with_column(bytes name, data_type type, index_info info, column_kind kind, column_id component_index);
    schema_builder& without_column(bytes name);
    schema_builder& without_column(sstring name, api::timestamp_type timestamp);
    schema_builder& with_column_rename(bytes from, bytes to);
    schema_builder& with_altered_column_type(bytes name, data_type new_type);

    // Adds information about collection that existed in the past but the column
    // has since been removed. For adding colllections that are still alive
    // use with_column().
    schema_builder& with_collection(bytes name, data_type type);

    schema_builder& with(compact_storage);
    schema_builder& with_version(table_schema_version);

    void add_default_index_names(database&);

    // Equivalent to with(cp).build()
    schema_ptr build(compact_storage cp);

    schema_ptr build();
};

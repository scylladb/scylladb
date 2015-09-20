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
    schema::raw_schema _raw;

    schema_builder(const schema::raw_schema&);
public:
    schema_builder(const sstring& ks_name, const sstring& cf_name,
            std::experimental::optional<utils::UUID> = { },
            data_type regular_column_name_type = utf8_type);
    schema_builder(const schema_ptr);

    void set_uuid(const utils::UUID& id) {
        _raw._id = id;
    }
    const utils::UUID& uuid() const {
        return _raw._id;
    }
    void set_regular_column_name_type(const data_type& t) {
        _raw._regular_column_name_type = t;
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
    void set_comment(const sstring& s) {
        _raw._comment = s;
    }
    const sstring& comment() const {
        return _raw._comment;
    }
    void set_default_time_to_live(gc_clock::duration t) {
        _raw._default_time_to_live = t;
    }
    gc_clock::duration default_time_to_live() const {
        return _raw._default_time_to_live;
    }

    void set_default_validator(const data_type& validator) {
        _raw._default_validator = validator;
    }

    void set_gc_grace_seconds(int32_t gc_grace_seconds) {
        _raw._gc_grace_seconds = gc_grace_seconds;
    }

    int32_t get_gc_grace_seconds() {
        return _raw._gc_grace_seconds;
    }

    void set_dc_local_read_repair_chance(double chance) {
        _raw._dc_local_read_repair_chance = chance;
    }

    double get_dc_local_read_repair_chance() {
        return _raw._dc_local_read_repair_chance;
    }

    void set_read_repair_chance(double chance) {
        _raw._read_repair_chance = chance;
    }

    double get_read_repair_chance() {
        return _raw._read_repair_chance;
    }

    void set_min_compaction_threshold(int32_t t) {
        _raw._min_compaction_threshold = t;
    }

    int32_t get_min_compaction_threshold() {
        return _raw._min_compaction_threshold;
    }

    void set_max_compaction_threshold(int32_t t) {
        _raw._max_compaction_threshold = t;
    }

    int32_t get_max_compaction_threshold() {
        return _raw._max_compaction_threshold;
    }

    void set_min_index_interval(int32_t t) {
        _raw._min_index_interval = t;
    }

    int32_t get_min_index_interval() {
        return _raw._min_index_interval;
    }

    void set_max_index_interval(int32_t t) {
        _raw._max_index_interval = t;
    }

    int32_t get_max_index_interval() {
        return _raw._max_index_interval;
    }

    void set_memtable_flush_period(int32_t t) {
        _raw._memtable_flush_period = t;
    }

    int32_t get_memtable_flush_period() const {
        return _raw._memtable_flush_period;
    }

    void set_speculative_retry(sstring retry_sstring) {
        _raw._speculative_retry = speculative_retry::from_sstring(retry_sstring);
    }

    const speculative_retry& get_speculative_retry() {
        return _raw._speculative_retry;
    }

    void set_bloom_filter_fp_chance(double fp) {
        _raw._bloom_filter_fp_chance = fp;
    }
    double get_bloom_filter_fp_chance() const {
        return _raw._bloom_filter_fp_chance;
    }
    void set_compressor_params(const compression_parameters& cp) {
        _raw._compressor_params = cp;
    }

    void set_compaction_strategy(sstables::compaction_strategy_type type) {
        _raw._compaction_strategy = type;
    }

    void set_compaction_strategy_options(std::map<sstring, sstring> options) {
        _raw._compaction_strategy_options = std::move(options);
    }

    void set_caching_options(caching_options c) {
        _raw._caching_options = std::move(c);
    }

    void set_is_dense(bool is_dense) {
        _raw._is_dense = is_dense;
    }

    void set_is_compound(bool is_compound) {
        _raw._is_compound = is_compound;
    }

    column_definition& find_column(const cql3::column_identifier&);
    schema_builder& with_column(const column_definition& c);
    schema_builder& with_column(bytes name, data_type type, column_kind kind = column_kind::regular_column);
    schema_builder& with_column(bytes name, data_type type, index_info info, column_kind kind = column_kind::regular_column);
    schema_builder& with_column(bytes name, data_type type, index_info info, column_kind kind, column_id component_index);

    void add_default_index_names(database&);

    enum class compact_storage { no, yes };
    schema_ptr build(compact_storage cp);
    schema_ptr build();
};

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

#include <vector>

#include <seastar/core/shared_ptr.hh>

#include "schema.hh"
#include "sstables/types.hh"
#include "utils/UUID.hh"
#include "db/marshal/type_parser.hh"

namespace sstables {

using column_values_fixed_lengths = std::vector<std::optional<uint32_t>>;

/*
 * A helper method to get fixed lengths of clustering key values
 */
inline column_values_fixed_lengths get_clustering_values_fixed_lengths(const serialization_header& header) {
    column_values_fixed_lengths lengths;
    lengths.reserve(header.clustering_key_types_names.elements.size());
    for (auto&& t : header.clustering_key_types_names.elements) {
        auto type = db::marshal::type_parser::parse(to_sstring_view(t.value));
        lengths.push_back(type->value_length_if_fixed());
    }

    return lengths;
}

/*
 * This class caches a mapping from columns present in sstable to their column_id.
 * This way we don't need to looku them up by column name every time.
 */
class column_translation {

    struct state {

        static std::tuple<std::vector<std::optional<column_id>>,
                          std::vector<std::optional<uint32_t>>,
                          std::vector<bool>,
                          std::vector<bool>> build(
                const schema& s,
                const utils::chunked_vector<serialization_header::column_desc>& src,
                bool is_static) {
            std::vector<std::optional<column_id>> ids;
            std::vector<std::optional<column_id>> lens;
            std::vector<bool> is_collection;
            std::vector<bool> is_counter;
            if (s.is_dense()) {
                if (is_static) {
                    ids.push_back(s.static_begin()->id);
                    lens.push_back(s.static_begin()->type->value_length_if_fixed());
                    is_collection.push_back(s.static_begin()->is_multi_cell());
                    is_counter.push_back(s.static_begin()->is_counter());
                } else {
                    ids.push_back(s.regular_begin()->id);
                    lens.push_back(s.regular_begin()->type->value_length_if_fixed());
                    is_collection.push_back(s.regular_begin()->is_multi_cell());
                    is_counter.push_back(s.regular_begin()->is_counter());
                }
            } else {
                ids.reserve(src.size());
                lens.reserve(src.size());
                for (auto&& desc : src) {
                    const column_definition* def = s.get_column_definition(desc.name.value);
                    const bytes& type_name = desc.type_name.value;
                    data_type type = db::marshal::type_parser::parse(to_sstring_view(type_name));
                    lens.push_back(type->value_length_if_fixed());
                    is_collection.push_back(type->is_multi_cell());
                    is_counter.push_back(type->is_counter());
                    if (def) {
                        if (def->is_multi_cell() != type->is_multi_cell() || def->is_counter() != type->is_counter()) {
                            throw malformed_sstable_exception(sprint(
                                    "{} definition in serialization header does not match schema. "
                                    "Schema collection = {}, counter = {}. Header collection = {}, counter = {}",
                                    def->name(),
                                    def->is_multi_cell(),
                                    def->is_counter(),
                                    type->is_multi_cell(),
                                    type->is_counter()));
                        }
                        ids.push_back(def->id);
                    } else {
                        ids.push_back(std::nullopt);
                    }
                }
            }
            return std::make_tuple(std::move(ids), std::move(lens), std::move(is_collection), std::move(is_counter));
        }

        utils::UUID schema_uuid;
        std::vector<std::optional<column_id>> regular_schema_column_id_from_sstable;
        std::vector<std::optional<column_id>> static_schema_column_id_from_sstable;
        column_values_fixed_lengths regular_column_value_fix_lengths;
        column_values_fixed_lengths static_column_value_fix_lengths;
        column_values_fixed_lengths clustering_column_value_fix_lengths;
        std::vector<bool> static_column_is_collection;
        std::vector<bool> regular_column_is_collection;
        std::vector<bool> static_column_is_counter;
        std::vector<bool> regular_column_is_counter;

        state() = default;
        state(const state&) = delete;
        state& operator=(const state&) = delete;
        state(state&&) = default;
        state& operator=(state&&) = default;

        state(const schema& s, const serialization_header& header)
                : schema_uuid(s.version()) {
            std::tie(regular_schema_column_id_from_sstable,
                     regular_column_value_fix_lengths,
                     regular_column_is_collection,
                     regular_column_is_counter) =
                    build(s, header.regular_columns.elements, false);
            std::tie(static_schema_column_id_from_sstable,
                     static_column_value_fix_lengths,
                     static_column_is_collection,
                     static_column_is_counter) =
                    build(s, header.static_columns.elements, true);
            clustering_column_value_fix_lengths = get_clustering_values_fixed_lengths(header);
        }
    };

    lw_shared_ptr<const state> _state = make_lw_shared<const state>();

public:
    column_translation get_for_schema(const schema& s, const serialization_header& header) {
        if (s.version() != _state->schema_uuid) {
            _state = make_lw_shared(state(s, header));
        }
        return *this;
    }

    const std::vector<std::optional<column_id>>& regular_columns() const {
        return _state->regular_schema_column_id_from_sstable;
    }
    const std::vector<std::optional<column_id>>& static_columns() const {
        return _state->static_schema_column_id_from_sstable;
    }
    const std::vector<std::optional<uint32_t>>& regular_column_value_fix_legths() const {
        return _state->regular_column_value_fix_lengths;
    }
    const std::vector<std::optional<uint32_t>>& static_column_value_fix_legths() const {
        return _state->static_column_value_fix_lengths;
    }
    const std::vector<std::optional<uint32_t>>& clustering_column_value_fix_legths() const {
        return _state->clustering_column_value_fix_lengths;
    }
    const std::vector<bool>& static_column_is_collection() const {
        return _state->static_column_is_collection;
    }
    const std::vector<bool>& regular_column_is_collection() const {
        return _state->regular_column_is_collection;
    }
    const std::vector<bool>& static_column_is_counter() const {
        return _state->static_column_is_counter;
    }
    const std::vector<bool>& regular_column_is_counter() const {
        return _state->regular_column_is_counter;
    }
};

};   // namespace sstables

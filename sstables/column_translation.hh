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

namespace sstables {

/*
 * This class caches a mapping from columns present in sstable to their column_id.
 * This way we don't need to looku them up by column name every time.
 */
class column_translation {

    struct state {
        utils::UUID schema_uuid;
        std::vector<stdx::optional<column_id>> regular_schema_column_id_from_sstable;
        std::vector<stdx::optional<column_id>> static_schema_column_id_from_sstable;
    };

    lw_shared_ptr<const state> _state = make_lw_shared<const state>();

    static std::vector<stdx::optional<column_id>> build(
            const schema& s,
            const utils::chunked_vector<serialization_header::column_desc>& src,
            bool is_static) {
        std::vector<stdx::optional<column_id>> res;
        if (s.is_dense()) {
            res.push_back(is_static ? s.static_begin()->id : s.regular_begin()->id);
        } else {
            res.reserve(src.size());
            for (auto&& desc : src) {
                const column_definition* def = s.get_column_definition(desc.name.value);
                if (def) {
                    res.push_back(def->id);
                } else {
                    res.push_back(stdx::nullopt);
                }
            }
        }
        return res;
    }

    static state build(const schema& s, const serialization_header& header) {
        return {
            s.version(),
            build(s, header.regular_columns.elements, false),
            build(s, header.static_columns.elements, true)
        };
    }
public:
    column_translation get_for_schema(const schema& s, const serialization_header& header) {
        if (s.version() != _state->schema_uuid) {
            _state = build(s, header);
        }
        return *this;
    }

    const std::vector<stdx::optional<column_id>>& regular_columns() const {
        return _state->regular_schema_column_id_from_sstable;
    }
    const std::vector<stdx::optional<column_id>>& static_columns() const {
        return _state->static_schema_column_id_from_sstable;
    }
};

};   // namespace sstables

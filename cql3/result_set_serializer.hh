/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/result_set.hh"
#include "cql3/column_identifier.hh"
#include "db/marshal/type_parser.hh"
#include "serializer_impl.hh"

// Custom serializer for cql3::result_set
// metadata is serialized field-by-field, rows converted to/from bytes for wire format
namespace ser {

namespace {

template<typename Input>
std::vector<lw_shared_ptr<cql3::column_specification>> read_column_specs(Input& in) {
    auto count = deserialize(in, std::type_identity<uint32_t>());
    std::vector<lw_shared_ptr<cql3::column_specification>> specs;
    specs.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        auto ks_name = deserialize(in, std::type_identity<sstring>());
        auto cf_name = deserialize(in, std::type_identity<sstring>());
        auto column_name = deserialize(in, std::type_identity<sstring>());
        auto type_name = deserialize(in, std::type_identity<sstring>());
        auto col_id = ::make_shared<cql3::column_identifier>(column_name, true);
        auto type = db::marshal::type_parser::parse(type_name);
        specs.push_back(make_lw_shared<cql3::column_specification>(ks_name, cf_name, std::move(col_id), std::move(type)));
    }
    return specs;
}

template<typename Output>
void write_column_specs(Output& out, const std::vector<lw_shared_ptr<cql3::column_specification>>& specs) {
    serialize(out, uint32_t(specs.size()));
    for (const auto& spec : specs) {
        serialize(out, spec->ks_name);
        serialize(out, spec->cf_name);
        serialize(out, spec->name->text());
        serialize(out, spec->type->name());
    }
}

}

template<>
struct serializer<cql3::result_set> {
    template<typename Input>
    static cql3::result_set read(Input& in) {
        // Read metadata
        auto flags = deserialize(in, std::type_identity<uint32_t>());
        auto column_specs = read_column_specs(in);
        auto column_count = deserialize(in, std::type_identity<uint32_t>());
        auto paging_state_opt = deserialize(in, std::type_identity<std::optional<service::pager::paging_state>>());

        lw_shared_ptr<const service::pager::paging_state> paging_state;
        if (paging_state_opt) {
            paging_state = make_lw_shared<const service::pager::paging_state>(std::move(*paging_state_opt));
        }

        auto md = ::make_shared<cql3::metadata>(
            cql3::metadata::flag_enum_set::from_mask(flags),
            std::move(column_specs),
            column_count,
            std::move(paging_state)
        );

        // Read rows
        auto row_count = deserialize(in, std::type_identity<uint32_t>());
        cql3::result_set rs(std::move(md));
        for (uint32_t i = 0; i < row_count; ++i) {
            auto cell_count = deserialize(in, std::type_identity<uint32_t>());
            std::vector<managed_bytes_opt> row;
            row.reserve(cell_count);
            for (uint32_t j = 0; j < cell_count; ++j) {
                row.push_back(managed_bytes_opt(std::move(deserialize(in, std::type_identity<bytes_opt>()))));
            }
            rs.add_row(std::move(row));
        }
        return rs;
    }

    template<typename Output>
    static void write(Output& out, const cql3::result_set& rs) {
        const auto& md = rs.get_metadata();

        // Write metadata
        serialize(out, uint32_t(md.flags().mask()));
        write_column_specs(out, md.get_names());
        serialize(out, md.column_count());

        std::optional<service::pager::paging_state> paging_state_copy;
        if (auto ps = md.paging_state()) {
            paging_state_copy = *ps;
        }
        serialize(out, paging_state_copy);

        // Write rows
        // Use bytes instead of managed_bytes to bytes for its IDL serializer
        const auto& rows = rs.rows();
        serialize(out, uint32_t(rows.size()));
        for (const auto& row : rows) {
            serialize(out, uint32_t(row.size()));
            for (const auto& cell : row) {
                serialize(out, to_bytes_opt(cell));
            }
        }
    }

    template<typename Input>
    static void skip(Input& in) {
        read(in);
    }
};

}

/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/index_target.hh"

namespace secondary_index {

struct target_parser {
    struct target_info {
        std::vector<const column_definition*> pk_columns;
        std::vector<const column_definition*> ck_columns;
        cql3::statements::index_target::target_type type;
    };

    static target_info parse(schema_ptr schema, const index_metadata& im);

    static target_info parse(schema_ptr schema, const sstring& target);

    static bool is_local(sstring target_string);

    static sstring get_target_column_name_from_string(const sstring& targets);

    static sstring serialize_targets(const std::vector<::shared_ptr<cql3::statements::index_target>>& targets);
};

}

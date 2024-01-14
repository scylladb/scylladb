/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/keyspace_element_name.hh"

namespace cql3 {

class cf_name;

class index_name : public keyspace_element_name {
    sstring _idx_name = "";
public:
    void set_index(const sstring& idx, bool keep_case);

    const sstring& get_idx() const;

    cf_name get_cf_name() const;

    virtual sstring to_string() const override;
};

}
